use anyhow::Context;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::option::Option;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
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

pub trait Node<Payload> {
    fn from_init(init: Init) -> anyhow::Result<Self>
    where
        Self: Sized;

    async fn heartbeat(&self, _tx: UnboundedSender<Message<Payload>>) -> anyhow::Result<()> {
        loop {
            tracing::info!("heartbeat loop");
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        }
    }

    async fn step(
        &mut self,
        input: Message<Payload>,
        tx: UnboundedSender<Message<Payload>>,
    ) -> anyhow::Result<()>;
}

pub async fn main_loop<N, P>() -> anyhow::Result<()>
where
    P: std::fmt::Debug + DeserializeOwned + Send + 'static + Sync + Serialize,
    N: Node<P>,
{
    tracing::info!("starting main loop");

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Message<P>>();

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

    let mut node: N = N::from_init(init)?;

    loop {
        tokio::select! {
            maybe_message = rx.recv() => {
                if let Some(message) = maybe_message {
                    message.send(&mut stdout).await?;
                }
            },
            maybe_line = stdin_lines.next_line() => {
                if let Some(line) = maybe_line? {
                    let input: Message<P> = serde_json::from_str(&line).context("failed to parse init message")?;
                    node.step(input, tx.clone()).await?;
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
