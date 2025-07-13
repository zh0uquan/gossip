use anyhow::Context;
use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::option::Option;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter, Stdout};
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

    pub async fn send(&self, output: Arc<Mutex<BufWriter<Stdout>>>) -> anyhow::Result<()>
    where
        Payload: Serialize,
    {
        let json = serde_json::to_vec(self)?;
        let mut writer = output.lock().await;
        writer.write_all(&json).await?;
        writer.write_all(b"\n").await?;
        writer.flush().await?;

        Ok(())
    }
}

#[async_trait]
pub trait Node<Payload, RpcPayload>
where
    Payload: Send + 'static + Sync,
{
    fn from_init(init: Init, rpc_service: RpcService<RpcPayload>) -> anyhow::Result<Self>
    where
        Self: Sized;

    async fn heartbeat(&self, _tx: UnboundedSender<Message<Payload>>) -> anyhow::Result<()> {
        loop {
            tracing::info!("heartbeat loop");
            tokio::time::sleep(tokio::time::Duration::from_secs(1000)).await;
        }
    }

    async fn step(
        &self,
        input: Message<Payload>,
        tx: UnboundedSender<Message<Payload>>,
    ) -> anyhow::Result<()>;
}

pub struct RpcFuture<RpcPayload> {
    inner: tokio::sync::oneshot::Receiver<Message<RpcPayload>>,
}

impl<RpcPayload> Future for RpcFuture<RpcPayload> {
    type Output = anyhow::Result<Message<RpcPayload>>;

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll(cx)
            .map(|res| res.context("response sender dropped"))
    }
}

#[derive(Debug)]
pub struct RpcService<RpcPayload> {
    inter: Arc<Mutex<Inter<RpcPayload>>>,
}

#[derive(Debug)]
pub struct Inter<RpcPayload> {
    senders: HashMap<usize, tokio::sync::oneshot::Sender<Message<RpcPayload>>>,
    stdout: Arc<Mutex<BufWriter<Stdout>>>,
}

impl<RpcPayload> RpcService<RpcPayload>
where
    RpcPayload: Debug + DeserializeOwned + Send + 'static + Sync + Serialize,
{
    fn new(inter: Arc<Mutex<Inter<RpcPayload>>>) -> Self {
        Self { inter }
    }

    pub fn rpc(&self, message: Message<RpcPayload>) -> anyhow::Result<RpcFuture<RpcPayload>> {
        let (tx, rx) = tokio::sync::oneshot::channel::<Message<RpcPayload>>();
        let message_id = message.id.expect("rpc msg id must be present");

        let inter_clone = self.inter.clone();
        tokio::spawn(async move {
            let mut inter = inter_clone.lock().await;
            inter.senders.insert(message_id, tx);
            message
                .send(inter.stdout.clone())
                .await
                .expect("rpc failed to send");
        });
        Ok(RpcFuture { inner: rx })
    }
}

pub async fn main_loop<N, P, R>() -> anyhow::Result<()>
where
    R: Debug + DeserializeOwned + Send + 'static + Sync + Serialize,
    P: Debug + DeserializeOwned + Send + 'static + Sync + Serialize,
    N: Node<P, R> + Sync + Send + 'static + Clone,
{
    tracing::info!("starting main loop");

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Message<P>>();

    let stdin = tokio::io::stdin();
    let stdout = tokio::io::stdout();
    let stdout = Arc::new(Mutex::new(BufWriter::new(stdout)));
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
    reply_msg.send(stdout.clone()).await?;
    tracing::info!("init successful");

    let inter = Arc::new(Mutex::new(Inter {
        senders: Default::default(),
        stdout: stdout.clone(),
    }));
    let rpc_service = RpcService::new(inter.clone());

    let node: N = N::from_init(init, rpc_service)?;
    let node_clone = node.clone();
    let tx_clone = tx.clone();
    tokio::spawn(async move {
        node_clone
            .heartbeat(tx_clone)
            .await
            .expect("heartbeat failed");
    });
    loop {
        let inter = inter.clone();
        tokio::select! {
            maybe_message = rx.recv() => {
                // tracing::info!("sending message: {:?}", maybe_message);
                if let Some(message) = maybe_message {
                    message.send(stdout.clone()).await?;
                }
            },
            maybe_line = stdin_lines.next_line() => {
                // tracing::info!("received stdin line: {:?}", maybe_line);
                if let Some(line) = maybe_line? {
                    if let Ok(input) = serde_json::from_str::<Message<P>>(&line) {
                        tokio::spawn({
                            let tx = tx.clone();
                            let node_clone = node.clone();
                            async move {
                                if let Err(e) = node_clone.step(input, tx).await {
                                    tracing::error!("step failed: {:?}", e);
                                }
                            }
                        });
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
        }
    }
}

pub fn generate_snowflake_id(timestamp: u64, machine_id: u64, sequence: u64) -> u64 {
    (timestamp << 22) | (machine_id << 12) | sequence
}
