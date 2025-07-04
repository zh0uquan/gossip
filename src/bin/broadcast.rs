use gossip::{Body, Init, Message, Node, main_loop};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::sync::mpsc::UnboundedSender;

type NodeId = String;
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct VectorClock {
    pub clock: HashMap<NodeId, usize>,
}

impl VectorClock {
    pub fn get(&self, node_id: &NodeId) -> usize {
        *self.clock.get(node_id).unwrap_or(&0)
    }

    pub fn update(&mut self, node_id: &NodeId, clock: usize) {
        self.clock
            .entry(node_id.clone())
            .and_modify(|c| *c = (*c).max(clock))
            .or_insert(clock);
    }

    pub fn missing(&self, other: &VectorClock) -> Vec<(NodeId, usize)> {
        other
            .clock
            .iter()
            .filter_map(|(other_node, other_clock)| {
                let self_clock = self.get(other_node);
                if other_clock > &self_clock {
                    return Some((other_node.clone(), *other_clock));
                }
                None
            })
            .collect()
    }
}

#[derive(Clone, Debug, Default)]
struct State {
    node_id: NodeId,
    messages: HashMap<MessageId, usize>,
    message_ids: HashSet<MessageId>,
    vector_clock: VectorClock,
    peers: Vec<NodeId>,
    counter: usize,
    clock: usize,
}

impl State {
    pub fn update_message(&mut self, value: usize) -> MessageId {
        self.clock += 1;
        let message_id = MessageId {
            node: self.node_id.clone(),
            counter: self.clock,
        };
        self.vector_clock.update(&self.node_id, self.clock);
        self.message_ids.insert(message_id.clone());
        self.messages.insert(message_id.clone(), value);
        message_id
    }
}

struct BroadcastNode {
    node_id: NodeId,
    state: Arc<Mutex<State>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, Hash)]
pub struct MessageId {
    pub node: NodeId,
    pub counter: usize,
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
    GossipDigest {
        vector_clock: VectorClock,
    },
    GossipRequest {
        want: Vec<(NodeId, usize)>,
    },
    GossipResponse {
        entries: Vec<(MessageId, usize)>,
    },
}

impl Node<Payload> for BroadcastNode {
    fn from_init(init: Init) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        let state = Arc::new(Mutex::new(State {
            node_id: init.node_id.clone(),
            peers: init.node_ids,
            ..Default::default()
        }));

        Ok(Self {
            node_id: init.node_id.clone(),
            state,
        })
    }

    async fn heartbeat(&self, tx: UnboundedSender<Message<Payload>>) -> anyhow::Result<()> {
        let state = self.state.clone();
        loop {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let mut s = state.lock().await;
            s.counter += 1;
            for peer in s.peers.iter() {
                let message = Message {
                    id: Some(s.counter),
                    src: s.node_id.clone(),
                    dest: peer.clone(),
                    body: Body {
                        id: None,
                        in_reply_to: None,
                        payload: Payload::GossipDigest {
                            vector_clock: s.vector_clock.clone(),
                        },
                    },
                };
                tracing::info!("Sending Gossip Request to peer {:?}: {:?}", peer, message);
                tx.send(message)?;
            }
        }
    }

    async fn step(
        &mut self,
        input: Message<Payload>,
        tx: UnboundedSender<Message<Payload>>,
    ) -> anyhow::Result<()> {
        let mut s = self.state.lock().await;
        s.counter += 1;
        let mut reply = input.into_reply(Some(s.counter));
        match reply.body.payload {
            Payload::Broadcast { message } => {
                s.update_message(message);
                tracing::info!(
                    "Got a message: {:?} with new clock: {:?}",
                    message,
                    s.vector_clock
                );
                reply.body.payload = Payload::BroadcastOk;
                tx.send(reply)?;
            }
            Payload::Read => {
                reply.body.payload = Payload::ReadOk {
                    messages: s.messages.values().cloned().collect(),
                };
                tx.send(reply)?;
            }
            Payload::Topology { mut topology } => {
                tracing::info!("Got New Topology: {:?}", topology);
                s.peers = topology.entry(self.node_id.clone()).or_default().clone();
                reply.body.payload = Payload::TopologyOk;
                tx.send(reply)?;
            }
            Payload::TopologyOk => {}
            Payload::GossipDigest {
                vector_clock: remote_clock,
            } => {
                tracing::info!("Got new Gossip Digest: {:?}", remote_clock);
                let want = s.vector_clock.missing(&remote_clock);
                if !want.is_empty() {
                    reply.body.payload = Payload::GossipRequest { want };
                    tx.send(reply)?;
                }
            }
            Payload::GossipRequest { want } => {
                let entries = want
                    .into_iter()
                    .flat_map(|(node, from)| {
                        let to = s.vector_clock.get(&node);
                        (from..=to)
                            .map(|i| MessageId {
                                node: node.clone(),
                                counter: i,
                            })
                            .collect::<Vec<_>>()
                    })
                    .filter_map(|id| s.messages.get(&id).map(|value| (id, *value)))
                    .collect();
                reply.body.payload = Payload::GossipResponse { entries };
                tx.send(reply)?;
            }
            Payload::GossipResponse { entries } => {
                tracing::info!("Got new Gossip Response: {:?}", entries);
                for (id, value) in entries {
                    let current = s.vector_clock.get(&id.node);
                    if id.counter > current {
                        s.vector_clock.update(&id.node, id.counter);
                        s.message_ids.insert(id.clone());
                        s.messages.insert(id, value);
                    }
                }
            }
            _ => {}
        }

        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .init();
    main_loop::<BroadcastNode, _>().await
}
