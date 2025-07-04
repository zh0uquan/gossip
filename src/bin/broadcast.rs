use gossip::{Body, Init, Message, Node, main_loop};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::sync::mpsc::UnboundedSender;

type NodeId = String;
type Clock = usize;
type Value = usize;

type MessageId = (NodeId, Clock);
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct VectorClock {
    pub clock: HashMap<NodeId, Clock>,
}

impl VectorClock {
    pub fn get(&self, node_id: &NodeId) -> Clock {
        *self.clock.get(node_id).unwrap_or(&0)
    }

    pub fn update(&mut self, node_id: &NodeId, clock: Clock) {
        self.clock
            .entry(node_id.clone())
            .and_modify(|c| *c = (*c).max(clock))
            .or_insert(clock);
    }

    pub fn missing(&self, other: &VectorClock) -> Vec<MessageId> {
        let mut missing = Vec::new();

        for (other_node, other_clock) in &other.clock {
            let self_clock = self.get(other_node);
            if other_clock > &self_clock {
                missing.push((other_node.clone(), self_clock + 1));
            }
        }

        for (self_node, _) in self.clock.iter() {
            if !other.clock.contains_key(self_node) {
                missing.push((self_node.clone(), 1));
            }
        }

        missing
    }
}

#[derive(Clone, Debug, Default)]
struct State {
    node_id: NodeId,
    messages: HashMap<MessageId, Value>,
    vector_clock: VectorClock,
    peers: Vec<NodeId>,
    counter: usize,
    clock: Clock,
}

impl State {
    pub fn update_message(&mut self, value: usize) -> MessageId {
        self.clock += 1;
        let message_id = (self.node_id.clone(), self.clock);
        self.vector_clock.update(&self.node_id, self.clock);
        self.messages.insert(message_id.clone(), value);
        message_id
    }
}

struct BroadcastNode {
    node_id: NodeId,
    state: Arc<Mutex<State>>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Broadcast {
        message: Value,
    },
    BroadcastOk,
    Read,
    ReadOk {
        messages: Vec<Value>,
    },
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk,
    GossipDigest {
        vector_clock: VectorClock,
    },
    GossipRequest {
        want: Vec<MessageId>,
    },
    GossipResponse {
        entries: Vec<(MessageId, Value)>,
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
            vector_clock: VectorClock {
                clock: HashMap::from([(init.node_id.clone(), 0)]),
            },
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
            tokio::time::sleep(Duration::from_millis(100)).await;
            let mut s = state.lock().await;
            for (i, peer) in s.peers.iter().enumerate() {
                let message = Message {
                    id: Some(s.counter + i + 1),
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
                tracing::info!("Sending Gossip Digest to peer {:?}: {:?}", peer, message);
                tx.send(message)?;
            }
            s.counter += s.peers.len();
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
                    tracing::info!("Try to query entries: {:?}", want);
                    reply.body.payload = Payload::GossipRequest { want };
                    tx.send(reply)?;
                }
            }
            Payload::GossipRequest { want } => {
                let entries = want
                    .into_iter()
                    .flat_map(|(node, from)| {
                        let to = s.vector_clock.get(&node);
                        if from <= to {
                            (from..=to)
                                .map(|i| (node.clone(), i))
                                .collect::<Vec<MessageId>>()
                        } else {
                            Vec::new()
                        }
                    })
                    .filter_map(|id| s.messages.get(&id).map(|value| (id, *value)))
                    .collect();
                reply.body.payload = Payload::GossipResponse { entries };
                tx.send(reply)?;
            }
            Payload::GossipResponse { mut entries } => {
                tracing::info!("Got new Gossip Response: {:?}", entries);
                entries.sort_by_key(|(k, _)| (*k).clone());
                for (id, value) in entries {
                    let (message_node, message_clock) = id.clone();
                    let clock = s.vector_clock.get(&message_node);
                    if message_clock > clock {
                        s.vector_clock.update(&message_node, message_clock);
                        s.messages.insert(id, value);
                    }
                }
                tracing::info!("After patch, my messages is: {:?}", s.messages.values());
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
