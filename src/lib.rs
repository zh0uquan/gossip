use anyhow::Context;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::option::Option;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::task::JoinHandle;

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
            id: self.id,
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

    async fn step<O>(&mut self, input: Message<Payload>, output: &mut O) -> anyhow::Result<()>
    where
        O: AsyncWriteExt + Unpin;
}

pub async fn main_loop<N, P>() -> anyhow::Result<()>
where
    P: std::fmt::Debug + DeserializeOwned + Send + 'static + Sync,
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
    let jh: JoinHandle<anyhow::Result<()>> = tokio::task::spawn(async move {
        while let Some(input_string) = stdin_lines.next_line().await? {
            tracing::info!("Received: {:?}", input_string);
            let input =
                serde_json::from_str(&input_string).context("failed to parse init message")?;

            tx.send(input)?;
        }
        Ok(())
    });

    while let Some(message) = rx.recv().await {
        node.step(message, &mut stdout).await?;
    }

    jh.await
        .expect("stdin task panicked")
        .context("node crashed")?;

    Ok(())
}
