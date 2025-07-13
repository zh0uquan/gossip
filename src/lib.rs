use anyhow::Context;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::option::Option;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::Mutex;
use tokio::sync::mpsc::UnboundedSender;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Message<Payload> {
    pub id: Option<usize>,
    pub src: String,
    pub dest: String,
    pub body: Body<Payload>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum InitPayload {
    Init(Init),
    InitOk,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Init {
    pub node_id: String,
    pub node_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Body<Payload> {
    #[serde(rename = "msg_id")]
    pub id: Option<usize>,
    pub in_reply_to: Option<usize>,
    #[serde(flatten)]
    pub payload: Payload,
}

impl<Payload> Message<Payload> {
    pub fn into_reply(self, id: Option<usize>) -> Message<Payload> {
        Self {
            id,
            src: self.dest,
            dest: self.src,
            body: Body {
                id,
                in_reply_to: self.body.id,
                payload: self.body.payload,
            },
        }
    }

    pub async fn send<O>(&self, output: &mut O) -> anyhow::Result<()>
    where
        Payload: Serialize,
        O: AsyncWriteExt + Unpin,
    {
        let json = serde_json::to_vec(self)?;

        output.write_all(&json).await?;
        output.write_all(b"\n").await?;
        output.flush().await?;

        Ok(())
    }
}

pub trait Node<Payload, RpcPayload> {
    fn from_init(init: Init, rpc_service: RpcService<RpcPayload>) -> anyhow::Result<Self>
    where
        Self: Sized;

    async fn heartbeat(&self, _tx: UnboundedSender<Message<Payload>>) -> anyhow::Result<()> {
        loop {
            tracing::info!("heartbeat loop");
            tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
        }
    }

    async fn step(
        &mut self,
        input: Message<Payload>,
        tx: UnboundedSender<Message<Payload>>,
    ) -> anyhow::Result<()>;
}

#[derive(Debug)]
pub struct RpcService<RpcPayload> {
    inter: Arc<Mutex<Inter<RpcPayload>>>,
}

#[derive(Debug)]
pub struct Inter<RpcPayload> {
    senders: HashMap<usize, tokio::sync::oneshot::Sender<Message<RpcPayload>>>,
    rpc_tx: UnboundedSender<Message<RpcPayload>>,
}

impl<RpcPayload> RpcService<RpcPayload>
where
    RpcPayload: Debug + DeserializeOwned + Send + 'static + Sync,
{
    fn new(inter: Arc<Mutex<Inter<RpcPayload>>>) -> Self {
        Self { inter }
    }

    async fn register_sender(
        &self,
        id: usize,
        tx: tokio::sync::oneshot::Sender<Message<RpcPayload>>,
    ) {
        let mut inter = self.inter.lock().await;
        inter.senders.insert(id, tx);
    }

    pub async fn rpc(&self, message: Message<RpcPayload>) -> anyhow::Result<Message<RpcPayload>> {
        let (tx, rx) = tokio::sync::oneshot::channel::<Message<RpcPayload>>();

        let inter = self.inter.lock().await;
        let message_id = message.id.expect("rpc msg id must be present");

        inter.rpc_tx.send(message)?;

        self.register_sender(message_id, tx).await;

        rx.await
            .map_err(|_| anyhow::anyhow!("response sender dropped"))
    }
}

pub async fn main_loop<N, P, R>() -> anyhow::Result<()>
where
    R: Debug + DeserializeOwned + Send + 'static + Sync + Serialize,
    P: Debug + DeserializeOwned + Send + 'static + Sync + Serialize,
    N: Node<P, R>,
{
    tracing::info!("starting main loop");

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Message<P>>();
    let (rpc_tx, mut rpc_rx) = tokio::sync::mpsc::unbounded_channel::<Message<R>>();

    let stdin = tokio::io::stdin();
    let mut stdout = tokio::io::stdout();
    let mut stdin_lines = BufReader::new(stdin).lines();

    let init_msg: Message<InitPayload> = serde_json::from_str(
        &stdin_lines
            .next_line()
            .await
            .expect("no init message received")
            .context("failed to parse init message")?,
    )?;

    let InitPayload::Init(init) = init_msg.body.payload.clone() else {
        panic!("first message should be init");
    };

    let mut reply_msg = init_msg.into_reply(Some(0));
    reply_msg.body.payload = InitPayload::InitOk {};
    reply_msg.send(&mut stdout).await?;
    tracing::info!("init successful");

    let inter = Arc::new(Mutex::new(Inter {
        senders: Default::default(),
        rpc_tx: rpc_tx.clone(),
    }));
    let rpc_service = RpcService::new(inter.clone());

    let mut node: N = N::from_init(init, rpc_service)?;

    loop {
        let inter = inter.clone();
        tokio::select! {
            maybe_message = rx.recv() => {
                if let Some(message) = maybe_message {
                    message.send(&mut stdout).await?;
                }
            },
            rpc_message = rpc_rx.recv() => {
                if let Some(message) = rpc_message {
                    message.send(&mut stdout).await?;
                }
            },
            maybe_line = stdin_lines.next_line() => {
                if let Some(line) = maybe_line? {
                    if let Ok(input) = serde_json::from_str::<Message<P>>(&line) {
                        node.step(input, tx.clone()).await?;
                    }

                    if let Ok(rpc) = serde_json::from_str::<Message<R>>(&line) {
                        match rpc.body.in_reply_to {
                            None => {
                                tracing::error!("rpc message got no reply id");
                                continue;
                            }
                            Some(id) => {
                                if let Some(sender) = inter.lock().await.senders.remove(&id) {
                                    sender.send(rpc).expect("failed to send message");
                                }
                            }
                        }
                    }
                }
            },
            _ = node.heartbeat(tx.clone()) => {
                anyhow::bail!("heart beat loop exited");
            }
        }
    }
}

pub fn generate_snowflake_id(timestamp: u64, machine_id: u64, sequence: u64) -> u64 {
    (timestamp << 22) | (machine_id << 12) | sequence
}
