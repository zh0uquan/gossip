use gossip::{Init, Message, Node, main_loop};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use tokio::io::AsyncWriteExt;

struct BroadcastNode {
    node_id: String,
    counter: usize,
    messages: HashSet<usize>,
    neighbours: Vec<String>,
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
    SyncRequest {},
}

impl Node<Payload> for BroadcastNode {
    fn from_init(init: Init) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            node_id: init.node_id,
            counter: 0,
            messages: HashSet::new(),
            neighbours: init.node_ids,
        })
    }

    async fn step<O>(&mut self, input: Message<Payload>, output: &mut O) -> anyhow::Result<()>
    where
        O: AsyncWriteExt + Unpin,
    {
        self.counter += 1;
        let mut reply = input.into_reply(Some(self.counter));

        match reply.body.payload {
            Payload::Broadcast { message } => {
                self.messages.insert(message);
                reply.body.payload = Payload::BroadcastOk;
                reply.send(output).await?;
            }
            Payload::Read => {
                reply.body.payload = Payload::ReadOk {
                    messages: self.messages.iter().cloned().collect(),
                };
                reply.send(output).await?;
            }
            Payload::Topology { topology: _ } => {
                reply.body.payload = Payload::TopologyOk;
                reply.send(output).await?;
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
        .init();
    main_loop::<BroadcastNode, _>().await
}
