use async_trait::async_trait;
use gossip::{Body, Init, Message, Node, RpcService, main_loop};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::sync::mpsc::UnboundedSender;

type Value = usize;
type NodeId = String;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Add { delta: Value },
    AddOk,
    Replicate { values: HashMap<NodeId, Vec<Value>> },
    Read,
    ReadOk { value: Value },
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct State {
    node_id: NodeId,
    node_ids: Vec<NodeId>,
    counter: usize,
    g_vec: HashMap<NodeId, Vec<Value>>,
}

#[derive(Clone)]
pub struct GCounterNode {
    state: Arc<Mutex<State>>,
}

#[async_trait]
impl Node<Payload, ()> for GCounterNode {
    fn from_init(init: Init, _rpc_service: RpcService<()>) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            state: Arc::new(Mutex::new(State {
                node_id: init.node_id,
                node_ids: init.node_ids,
                ..Default::default()
            })),
        })
    }

    async fn heartbeat(&self, tx: UnboundedSender<Message<Payload>>) -> anyhow::Result<()> {
        loop {
            tokio::time::sleep(Duration::from_millis(150)).await;
            let mut s = self.state.lock().await;
            s.counter += 1;

            for peer in s.node_ids.iter() {
                let message = Message {
                    id: Some(s.counter),
                    src: s.node_id.clone(),
                    dest: peer.clone(),
                    body: Body {
                        id: None,
                        in_reply_to: None,
                        payload: Payload::Replicate {
                            values: s.g_vec.clone(),
                        },
                    },
                };
                tracing::info!("Sending replicate to {}, message: {:?}", peer, message);
                tx.send(message)?;
            }
        }
    }

    async fn step(
        &self,
        input: Message<Payload>,
        tx: UnboundedSender<Message<Payload>>,
    ) -> anyhow::Result<()> {
        let mut s = self.state.lock().await;
        let node_id = s.node_id.clone();
        let mut reply = input.into_reply(Some(0));
        match reply.body.payload {
            Payload::Add { delta } => {
                tracing::info!("Got new message: {:?}", delta);
                reply.body.payload = Payload::AddOk;
                s.g_vec.entry(node_id).or_default().push(delta);
                tx.send(reply)?;
            }
            Payload::Replicate { values: incoming } => {
                tracing::info!("Replicated message: {:?}", incoming);
                for (key, new_vec) in incoming.iter() {
                    s.g_vec
                        .entry(key.clone())
                        .and_modify(|existing_vec| {
                            if new_vec.len() > existing_vec.len() {
                                *existing_vec = new_vec.clone();
                            }
                        })
                        .or_insert(new_vec.clone());
                }
            }
            Payload::Read => {
                tracing::info!("Read message: {:?}", s.g_vec);
                reply.body.payload = Payload::ReadOk {
                    value: s.g_vec.values().flatten().sum::<Value>(),
                };
                tx.send(reply)?;
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
    main_loop::<GCounterNode, _, ()>().await
}
