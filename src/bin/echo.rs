use async_trait::async_trait;
use gossip::{Init, Message, Node, RpcService, main_loop};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::mpsc::UnboundedSender;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Echo { echo: String },
    EchoOk { echo: String },
}

#[allow(dead_code)]
#[derive(Clone)]
pub struct EchoNode {
    id: Arc<Mutex<String>>,
}

#[async_trait]
impl Node<Payload, ()> for EchoNode {
    fn from_init(init: Init, _rpc_service: RpcService<()>) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        let id = Arc::new(Mutex::new(init.node_id));
        Ok(Self { id })
    }

    async fn step(
        &self,
        input: Message<Payload>,
        tx: UnboundedSender<Message<Payload>>,
    ) -> anyhow::Result<()> {
        let mut reply = input.into_reply(Some(0));

        match reply.body.payload {
            Payload::Echo { echo } => {
                reply.body.payload = Payload::EchoOk { echo };
                tracing::info!("sending reply: {:?}", reply);
                tx.send(reply)?;
            }
            Payload::EchoOk { .. } => {}
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
    main_loop::<EchoNode, _, ()>().await
}
