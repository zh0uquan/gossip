use std::collections::{HashMap, HashSet};
use gossip::{Init, Message, Node, main_loop, Body};
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use tokio::sync::Mutex;
use std::time::Duration;
use tokio::sync::mpsc::UnboundedSender;

type Value = usize;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Add { element: Value },
    AddOk,
    Replicate {
        g_set: HashSet<Value>,
    },
    Read,
    ReadOk {
        value: HashSet<Value>,
    },
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct State {
    counter: usize,
    g_set: HashSet<Value>
}

pub struct GSetNode {
    node_id: String,
    node_ids: Vec<String>,
    state: Arc<Mutex<State>>,
}

impl Node<Payload> for GSetNode {
    fn from_init(init: Init) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            node_id: init.node_id,
            node_ids: init.node_ids,
            state: Default::default()
        })
    }

    async fn heartbeat(&self, tx: UnboundedSender<Message<Payload>>) -> anyhow::Result<()> {
        loop {
            tokio::time::sleep(Duration::from_millis(150)).await;
            let mut s = self.state.lock().await;
            s.counter += 1;

            for peer in self.node_ids.iter() {
                let message = Message {
                    id: Some(s.counter),
                    src: self.node_id.clone(),
                    dest: peer.clone(),
                    body: Body {
                        id: None,
                        in_reply_to: None,
                        payload: Payload::Replicate {
                            g_set: s.g_set.clone()
                        },
                    }
                };
                tracing::info!("Sending replicate to {}, message: {:?}", peer, message);
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
        let mut reply = input.into_reply(Some(0));
        match reply.body.payload {
            Payload::Add { element } => {
                tracing::info!("Got new message: {:?}", element);
                reply.body.payload = Payload::AddOk;
                s.g_set.insert(element);
                tx.send(reply)?;
            }
            Payload::Replicate { g_set } => {
                tracing::info!("Replicated message: {:?}", g_set);
                s.g_set = g_set.union(&s.g_set).cloned().collect();
            }
            Payload::Read => {
                reply.body.payload = Payload::ReadOk { value: s.g_set.clone() };
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
    main_loop::<GSetNode, _>().await
}
