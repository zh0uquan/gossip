use anyhow::Context;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::option::Option;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

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
enum InitPayload {
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk {},
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
    fn from_init() -> anyhow::Result<Self>
    where
        Self: Sized;

    async fn step<O>(&mut self, input: Message<Payload>, output: &mut O) -> anyhow::Result<()>
    where
        O: AsyncWriteExt + Unpin;
}

pub async fn main_loop<N, P>() -> anyhow::Result<()>
where
    P: std::fmt::Debug + DeserializeOwned,
    N: Node<P>,
{
    tracing::info!("starting main loop");

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

    let InitPayload::Init { .. } = init_msg.body.payload else {
        panic!("first message should be init");
    };

    let mut reply_msg = init_msg.into_reply(Some(0));
    reply_msg.body.payload = InitPayload::InitOk {};
    reply_msg.send(&mut stdout).await?;
    tracing::info!("init successful");

    let mut node: N = N::from_init()?;

    while let Some(input_string) = stdin_lines.next_line().await? {
        tracing::info!("Received: {:?}", input_string);
        let input = serde_json::from_str(&input_string)?;
        node.step(input, &mut stdout).await?;
    }

    Ok(())
}
