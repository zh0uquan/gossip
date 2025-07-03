use gossip::{Init, Message, Node, main_loop, Body};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use rand::prelude::SliceRandom;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

type NodeId = String;
type VectorClock = HashMap<NodeId, usize>;

#[derive(Clone, Debug)]
struct State {
    node_id: NodeId,
    messages: Vec<usize>,
    vector_clock: VectorClock,
    peers: Vec<NodeId>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct Patch {
    updates: Vec<usize>,
    clock: VectorClock,
}

struct BroadcastNode {
    node_id: NodeId,
    state: Arc<Mutex<State>>,
    counter: usize,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Broadcast {
        message: usize,
    },
    BroadcastOk,
    Read,
    ReadOk {
        messages: Vec<usize>,
    },
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk,
    GossipDigest { vector_clock: VectorClock },
    GossipRequest { missing: Vec<NodeId> },
    GossipResponse { patch: Patch }
}

impl Node<Payload> for BroadcastNode {
    async fn from_init(init: Init, tx: UnboundedSender<Message<Payload>>) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        let state = Arc::new(Mutex::new(
            State {
                node_id: init.node_id.clone(),
                messages: vec![],
                vector_clock: Default::default(),
                peers: init.node_ids,
            }
        ));

        let state_clone = state.clone();
        let handler: JoinHandle<anyhow::Result<()>> = tokio::task::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(10000)).await;
                tracing::info!("Sending Gossip Digest");

                let s = state.lock().await;
                for peer in s.peers.iter() {
                    tx.send(Message {
                        id: None,
                        src: s.node_id.clone(),
                        dest: peer.clone(),
                        body: Body {
                            id: None,
                            in_reply_to: None,
                            payload: Payload::GossipDigest { vector_clock: s.vector_clock.clone() },
                        },
                    })?;
                }
            }
        });

        handler.await
            .expect("background task failed")?;

        Ok(Self {
            node_id: init.node_id.clone(),
            state: state_clone,
            counter: 0,
        })
    }

    async fn step<O>(&mut self, input: Message<Payload>, output: &mut O) -> anyhow::Result<()>
    where
        O: AsyncWriteExt + Unpin,
    {
        let counter = self.counter + 1;
        let mut s = self.state.lock().await;
        let mut reply = input.into_reply(Some(counter));

        match reply.body.payload {
            Payload::Broadcast { message } => {
                *s.vector_clock.entry(self.node_id.clone()).or_default() += 1;
                s.messages.push(message);
                tracing::info!("Got a message: {:?} with new clock: {:?}" , message, s.vector_clock);
                reply.body.payload = Payload::BroadcastOk;
                reply.send(output).await?;
            }
            Payload::Read => {
                reply.body.payload = Payload::ReadOk {
                    messages: s.messages.iter().cloned().collect(),
                };
                reply.send(output).await?;
            }
            Payload::Topology { mut topology } => {
                tracing::info!("Got New Topology: {:?}", topology);
                s.peers = topology.entry(self.node_id.clone()).or_default().clone();
                s.vector_clock = s.vector_clock.clone()
                    .into_iter()
                    .filter(|(n, _)| s.peers.contains(n))
                    .collect();              
                reply.body.payload = Payload::TopologyOk;
                reply.send(output).await?;
            }
            Payload::TopologyOk => {}
            Payload::GossipDigest { vector_clock: clock } => {
                let missing = clock_compare(&clock, &s.vector_clock);
                if !missing.is_empty() {
                    reply.body.payload = Payload::GossipRequest { missing };
                    reply.send(output).await?;
                }
            }
            Payload::GossipRequest { .. } => {
                let patch = Patch {
                    updates: s.messages.clone(),
                    clock: s.vector_clock.clone(),
                };
                reply.body.payload = Payload::GossipResponse { patch };
                reply.send(output).await?;
            },
            Payload::GossipResponse { patch } => {
                merge_patch(&mut s, &patch);        
            }
            _ => {}
        }

        Ok(())
    }
}

fn merge_patch(local: &mut State, patch: &Patch) {
    let mut applied = false;

    for (node, patch_version) in &patch.clock {
        let local_version = local.vector_clock.get(node).cloned().unwrap_or(0);
        if *patch_version > local_version {
            local.vector_clock.insert(node.clone(), *patch_version);
            applied = true;
        }
    }
    if applied {
        local.messages.extend(patch.updates.iter().cloned());
        // local.messages.sort();
        // local.messages.dedup();
    }
}


fn clock_compare(incoming: &VectorClock, local: &VectorClock) -> Vec<NodeId> {
    incoming.iter()
        .filter(|(node, clock)| local.get(*node).unwrap_or(&0) < clock)
        .map(|(n, _)| n.clone())
        .collect()
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .init();
    main_loop::<BroadcastNode, _>().await
}
