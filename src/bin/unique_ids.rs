use gossip::{Init, Message, Node, main_loop};
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc::UnboundedSender;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Generate,
    GenerateOk { id: u64 },
}

struct UniqueIdNode {
    node_id: usize,
    counter: usize,
}

fn generate_snowflake_id(timestamp: u64, machine_id: u64, sequence: u64) -> u64 {
    (timestamp << 22) | (machine_id << 12) | sequence
}

impl Node<Payload> for UniqueIdNode {
    fn from_init(init: Init, _tx: UnboundedSender<Message<Payload>>) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        let Init { node_id, .. } = init;
        let node_id = node_id
            .chars()
            .filter(|c| c.is_ascii_digit())
            .collect::<String>();
        Ok(Self {
            node_id: node_id.parse::<usize>()?,
            counter: 0,
        })
    }

    async fn step<O>(&mut self, input: Message<Payload>, output: &mut O) -> anyhow::Result<()>
    where
        O: AsyncWriteExt + Unpin,
    {
        self.counter += 1;
        let mut reply = input.into_reply(Some(self.counter));

        match reply.body.payload {
            Payload::Generate => {
                let id = generate_snowflake_id(
                    SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64,
                    self.node_id as u64,
                    self.counter as u64,
                );
                reply.body.payload = Payload::GenerateOk { id };
                reply.send(output).await?
            }
            Payload::GenerateOk { .. } => {}
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .init();
    main_loop::<UniqueIdNode, _>().await
}
