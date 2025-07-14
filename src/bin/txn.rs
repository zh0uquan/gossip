use async_trait::async_trait;
use gossip::{Body, Init, Message, Node, RpcService, main_loop};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::mpsc::UnboundedSender;

type Key = usize;
type Value = Option<usize>;
const KV_STORE: &str = "kv_store";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Txn { txn: Vec<(String, Key, Value)> },
    TxnOk { txn: Vec<(String, Key, Value)> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum RpcPayload {
    Read {
        key: String,
    },
    ReadOk {
        value: String,
    },
    Write {
        key: String,
        value: String,
    },
    WriteOk,
    Cas {
        key: String,
        from: String,
        to: String,
        create_if_not_exists: bool,
    },
    CasOk,
    Error {
        text: String,
        code: usize,
    },
}

struct State {
    rpc_service: RpcService<RpcPayload>,
    node_id: String,
    r#type: String,
    msg_counter: Mutex<usize>,
    rpc_counter: Mutex<usize>,
}

#[derive(Clone)]
struct TxnNode {
    state: Arc<State>,
}

impl State {
    async fn create_new_message(&self) -> Message<RpcPayload> {
        let mut rpc_counter = self.rpc_counter.lock().await;
        *rpc_counter += 1;
        Message {
            id: Some(*rpc_counter),
            src: self.node_id.clone(),
            dest: self.r#type.clone(),
            body: Body {
                id: Some(*rpc_counter),
                in_reply_to: None,
                payload: RpcPayload::CasOk,
            },
        }
    }

    async fn get_value(&self, key: String) -> anyhow::Result<Option<String>> {
        let mut message = self.create_new_message().await;
        message.body.payload = RpcPayload::Read { key: key.clone() };
        let message = self.rpc_service.rpc(message)?.await?;
        match message.body.payload {
            RpcPayload::ReadOk { value } => Ok(Some(value)),
            RpcPayload::Error { code, text } => {
                tracing::error!("error code: {code:?}, text: {text:?}");
                Ok(None)
            }
            _ => {
                tracing::error!("get_value {:?} failed", key);
                Err(anyhow::anyhow!("Failed to read value for key: {}", key))
            }
        }
    }

    async fn cas_value(&self, from: String, to: String) -> anyhow::Result<()> {
        let mut message = self.create_new_message().await;
        message.body.payload = RpcPayload::Cas {
            key: KV_STORE.into(),
            from,
            to,
            create_if_not_exists: true,
        };
        let message = self.rpc_service.rpc(message)?.await?;
        match message.body.payload {
            RpcPayload::CasOk => Ok(()),
            RpcPayload::Error { code, text } => {
                tracing::error!("failed to cas value: {code:?}, text: {text:?}");
                Err(anyhow::anyhow!("failed to cas value: {code:?}"))
            }
            _ => {
                tracing::error!("cas_value failed");
                Err(anyhow::anyhow!("Failed to cas value"))
            }
        }
    }

    async fn txn(
        &self,
        txn: Vec<(String, Key, Value)>,
    ) -> anyhow::Result<Vec<(String, Key, Value)>> {
        let json_value = self.get_value(KV_STORE.into()).await?;
        let data: HashMap<Key, Value> = match json_value.clone() {
            Some(v) => serde_json::from_str(&v)?,
            None => HashMap::new(),
        };
        let mut new_data = data.clone();
        for (op, k, v) in txn.iter() {
            match op.as_str() {
                "w" => {
                    *new_data.entry(*k).or_default() = *v;
                }
                "r" => {}
                _ => {
                    anyhow::bail!("invalid txn operation: {op}");
                }
            }
        }
        tracing::info!("new data : {:?}", new_data);
        let new_json_value = serde_json::to_string(&new_data)?;
        self.cas_value(json_value.unwrap_or_default(), new_json_value)
            .await?;
        Ok(txn
            .into_iter()
            .map(|(op, k, v)| {
                if op == "r" {
                    let new_v = data.get(&k).cloned().unwrap_or(v);
                    (op, k, new_v)
                } else {
                    (op, k, v)
                }
            })
            .collect::<Vec<(String, Key, Value)>>())
    }
}

#[async_trait]
impl Node<Payload, RpcPayload> for TxnNode {
    fn from_init(init: Init, rpc_service: RpcService<RpcPayload>) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            state: Arc::new(State {
                rpc_service,
                node_id: init.node_id.clone(),
                r#type: "lin-kv".into(),
                msg_counter: Mutex::new(0),
                rpc_counter: Mutex::new(0),
            }),
        })
    }

    async fn step(
        &self,
        input: Message<Payload>,
        tx: UnboundedSender<Message<Payload>>,
    ) -> anyhow::Result<()> {
        let mut msg_counter = self.state.msg_counter.lock().await;
        *msg_counter += 1;
        let mut reply = input.into_reply(Some(*msg_counter));
        match reply.body.payload {
            Payload::Txn { txn } => {
                tracing::info!("txn request {:?}", txn);
                let txn = self.state.txn(txn).await?;
                tracing::info!("txn reply {:?}", txn);
                reply.body.payload = Payload::TxnOk { txn };
                tx.send(reply)?;
            }
            Payload::TxnOk { .. } => {}
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
    main_loop::<TxnNode, _, RpcPayload>().await
}
