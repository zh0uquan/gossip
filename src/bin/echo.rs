use gossip::{Init, Message, Node, main_loop};
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use tokio::io::AsyncWriteExt;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Echo { echo: String },
    EchoOk { echo: String },
}

pub struct EchoNode {
    id: usize,
}

impl Node<Payload> for EchoNode {
    fn from_init(_init: Init) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self { id: 1 })
    }

    async fn step<O>(&mut self, input: Message<Payload>, output: &mut O) -> anyhow::Result<()>
    where
        O: AsyncWriteExt + Unpin,
    {
        let mut reply = input.into_reply(Some(0));

        match reply.body.payload {
            Payload::Echo { echo } => {
                reply.body.payload = Payload::EchoOk { echo };
                reply.send(output).await?;
            }
            Payload::EchoOk { .. } => {}
        }
        Ok(())
    }
}

impl Display for EchoNode {
    fn fmt(&self, f: &mut Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "{}", self.id)
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .init();
    main_loop::<EchoNode, _>().await
}
