use anyhow::Context;
use async_trait::async_trait;
use gossip::{Body, Init, Message, Node, RpcService, main_loop};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Mutex;
use tokio::sync::mpsc::UnboundedSender;

type Value = usize;
type NodeId = String;
type LogId = String;

type Offset = usize;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Send {
        key: LogId,
        msg: Value,
    },
    SendOk {
        offset: Offset,
    },
    Poll {
        offsets: HashMap<LogId, Offset>,
    },
    PollOk {
        msgs: HashMap<LogId, Vec<[usize; 2]>>,
    },
    CommitOffsets {
        offsets: HashMap<LogId, Offset>,
    },
    CommitOffsetsOk,
    ListCommittedOffsets {
        keys: Vec<LogId>,
    },
    ListCommittedOffsetsOk {
        offsets: HashMap<LogId, Offset>,
    },
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum RpcPayload {
    Read {
        key: String,
    },
    ReadOk {
        value: Value,
    },
    Write {
        key: String,
        value: Value,
    },
    WriteOk,
    Cas {
        key: String,
        from: Offset,
        to: Offset,
    },
    CasOk,
    Default,
    Error {
        text: String,
        code: usize,
    },
}

#[derive(Debug)]
struct LinKvStore {
    rpc_service: RpcService<RpcPayload>,
    counter: Arc<AtomicUsize>,
    r#type: String,
    node_id: NodeId,
}
impl LinKvStore {
    fn latest_log(log_id: &LogId) -> String {
        format!("log/{log_id}")
    }

    fn entry_log(log_id: &LogId, offset: Offset) -> String {
        format!("{log_id}_{offset}")
    }

    fn commited_log(log_id: &LogId) -> String {
        format!("committed/{log_id}")
    }
    async fn get_value(&self, key: String) -> anyhow::Result<Option<Value>> {
        let mut message = self.create_new_message();
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

    async fn write_value(&self, key: String, value: Value) -> anyhow::Result<()> {
        let mut message = self.create_new_message();
        message.body.payload = RpcPayload::Write {
            key: key.clone(),
            value,
        };
        tracing::info!("write {:?}", message);
        let message = self
            .rpc_service
            .rpc(message)?
            .await
            .context(format!("commit {:?} failed", key))?;
        match message.body.payload {
            RpcPayload::WriteOk => Ok(()),
            _ => Err(anyhow::anyhow!("Failed to write value for key: {}", key)),
        }
    }

    fn create_new_message(&self) -> Message<RpcPayload> {
        let msg_id = self.counter.fetch_add(1, Ordering::SeqCst) + 1;
        let msg = Message {
            id: Some(msg_id),
            src: self.node_id.clone(),
            dest: self.r#type.clone(),
            body: Body {
                id: Some(msg_id),
                in_reply_to: None,
                payload: RpcPayload::Default,
            },
        };
        msg
    }

    async fn append(&self, key: &LogId, value: Value) -> anyhow::Result<Offset> {
        let latest = LinKvStore::latest_log(key);
        let maybe_offset = self.get_value(latest.clone()).await?;
        // key not exist
        let mut offset = match maybe_offset {
            Some(offset) => offset + 1,
            None => 0,
        };
        tracing::info!("append value {:?} -> {:?}", value, latest);
        loop {
            let mut msg = self.create_new_message();
            msg.body.payload = RpcPayload::Cas {
                key: latest.clone(),
                from: offset.saturating_sub(1),
                to: offset,
            };
            tracing::info!("sending cas: {:?}", msg);
            let reply = self.rpc_service.rpc(msg)?.await?;
            tracing::info!("got cas reply: {:?}", reply);
            match reply.body.payload {
                RpcPayload::CasOk => {
                    break;
                }
                RpcPayload::Error { code, text } => {
                    tracing::error!("error code: {code:?}, text: {text:?}");
                    if code == 20 {
                        self.write_value(latest.clone(), offset).await?;
                        break;
                    }
                }
                _ => {
                    offset += 1;
                    continue;
                }
            }
        }
        self.write_value(Self::entry_log(key, offset), value)
            .await?;
        Ok(offset)
    }

    async fn commit(&self, key: &LogId, offset: Offset) -> anyhow::Result<()> {
        self.write_value(Self::commited_log(key), offset).await?;
        Ok(())
    }

    async fn get_committed_offset(&self, key: &LogId) -> Option<Offset> {
        match self.get_value(Self::commited_log(key)).await {
            Ok(Some(offset)) => Some(offset),
            _ => None,
        }
    }

    async fn poll(&self, key: &LogId, offset: Offset) -> Vec<[usize; 2]> {
        let mut vec = vec![];
        for index in offset..Offset::MAX {
            let maybe_value = self.get_value(Self::entry_log(key, index)).await;
            match maybe_value {
                Ok(value) => {
                    let Some(value) = value else {
                        break;
                    };
                    vec.push([index, value])
                }
                Err(_) => break,
            }
        }
        vec
    }
}

#[derive(Debug)]
pub struct LogStore {
    link_kv_store: LinKvStore,
}

impl LogStore {
    pub async fn poll(&self, offsets: HashMap<LogId, Offset>) -> HashMap<LogId, Vec<[usize; 2]>> {
        let mut results = HashMap::new();
        for (key, offset) in offsets {
            results.insert(key.clone(), self.link_kv_store.poll(&key, offset).await);
        }
        results
    }

    pub async fn commit(&mut self, commits: HashMap<LogId, Offset>) -> anyhow::Result<()> {
        for (key, offset) in commits {
            self.link_kv_store.commit(&key, offset).await?;
        }
        Ok(())
    }

    pub async fn append(&mut self, key: LogId, value: Value) -> Offset {
        let value = self.link_kv_store.append(&key, value).await;
        value.unwrap_or(0)
    }

    pub async fn list_committed_offset(&self, keys: Vec<LogId>) -> HashMap<LogId, Offset> {
        let mut results = HashMap::new();
        for key in keys {
            let offset = self.link_kv_store.get_committed_offset(&key).await;
            if let Some(offset) = offset {
                results.insert(key, offset);
            }
        }
        results
    }
}

#[derive(Debug)]
pub struct State {
    pub node_id: String,
    pub node_ids: Vec<String>,
    pub counter: Arc<AtomicUsize>,
    pub log_store: LogStore,
}

#[derive(Clone)]
pub struct KafkaLogNode {
    state: Arc<Mutex<State>>,
}

#[async_trait]
impl Node<Payload, RpcPayload> for KafkaLogNode {
    fn from_init(init: Init, rpc_service: RpcService<RpcPayload>) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        let counter = Arc::new(AtomicUsize::new(0));
        Ok(Self {
            state: Arc::new(Mutex::new(State {
                node_ids: init.node_ids,
                node_id: init.node_id.clone(),
                counter: counter.clone(),
                log_store: LogStore {
                    link_kv_store: LinKvStore {
                        rpc_service,
                        counter,
                        r#type: "lin-kv".to_string(),
                        node_id: init.node_id,
                    },
                },
            })),
        })
    }

    async fn step(
        &self,
        input: Message<Payload>,
        tx: UnboundedSender<Message<Payload>>,
    ) -> anyhow::Result<()> {
        let mut s = self.state.lock().await;
        let mut reply = input.into_reply(Some(0));
        match reply.body.payload {
            Payload::Send { key, msg } => {
                tracing::info!("Got new message: {:?} {:?}", key, msg);
                let offset = s.log_store.append(key, msg).await;
                tracing::debug!("Appended offsets: {:?}", offset);
                reply.body.payload = Payload::SendOk { offset };
                tx.send(reply)?;
            }
            Payload::Poll { offsets } => {
                tracing::info!("Poll offsets: {:?}", offsets);
                let offsets = s.log_store.poll(offsets).await;
                reply.body.payload = Payload::PollOk { msgs: offsets };
                tx.send(reply)?;
            }
            Payload::CommitOffsets { offsets } => {
                tracing::info!("Commit offsets: {:?}", offsets);
                if let Ok(_) = s.log_store.commit(offsets).await {
                    reply.body.payload = Payload::CommitOffsetsOk;
                    tx.send(reply)?;
                }
            }
            Payload::ListCommittedOffsets { keys } => {
                tracing::info!("List committed offsets: {:?}", keys);
                let offsets = s.log_store.list_committed_offset(keys).await;
                reply.body.payload = Payload::ListCommittedOffsetsOk { offsets };
                tx.send(reply)?;
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
        .with_ansi(false)
        .init();
    main_loop::<KafkaLogNode, _, RpcPayload>().await
}
