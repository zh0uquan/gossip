use anyhow::Context;
use gossip::{Init, Inter, Message, Node, main_loop, RpcService, Body};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
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
    Default
}

#[derive(Debug, Clone, Default)]
pub struct MemLog {
    committed: Vec<Value>,
    pending: VecDeque<Value>,
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
    
    fn entry_
    
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
        let mut msg = self.create_new_message();
        let key = LinKvStore::log_entry(key);
        msg.body.payload = RpcPayload::Write {
            key, value
        };
        let message: Message<RpcPayload> = self.rpc_service.rpc(msg).await?;
        
        match message.body.payload { 
            RpcPayload::WriteOk
        }
        
    }
        
    async fn commit(&self, offset: Offset) -> anyhow::Result<()> {
        Ok(())
    }
    
    async fn get_committed_offset(&self) -> Offset {
        0
    }
    
    async fn poll(&self, offset: Offset) -> Vec<[usize; 2]> {
        vec![[0, 0]];
    }
}



#[derive(Debug)]
pub struct LogStore<R> {
    link_kv_store: LinKvStore<R>,
}

impl<R> LogStore<R> {
    pub async fn poll(&self, offsets: HashMap<LogId, Offset>) -> HashMap<LogId, Vec<[usize; 2]>> {
        let results = HashMap::new();
        for (key, offset) in offsets.iter() {
            if let Some(log) = self.link_kv_store.poll(key, offset).await {
                
                
            }
            
            
            
        }
            .filter_map(|(key, offset)| {
                if let Some(log) = self.logs.get(key) {
                    return Some((key.clone(), log.poll(*offset)));
                }
                None
            })
            .collect()
    }

    pub async fn commit(&mut self, commits: HashMap<LogId, Offset>) -> anyhow::Result<()> {
        for (key, offset) in commits {
            if let Some(log) = self.logs.get_mut(&key) {
                log.commit(offset)?;
            }
        }

        Ok(())
    }

    pub async fn append(&mut self, key: LogId, value: Value) -> Offset {
        self.logs.entry(key).or_default().append(value)
    }

    pub async fn list_committed_offset(&self, keys: Vec<LogId>) -> HashMap<LogId, Offset> {
        keys.into_iter()
            .filter_map(|key| self.logs.get(&key).map(|log| (key, log)))
            .map(|(key, log)| (key, log.get_committed_offset()))
            .collect()
    }
}

#[derive(Debug)]
pub struct State {
    pub counter: Arc<AtomicUsize>,
    pub log_store: LogStore<RpcPayload>,
}

pub struct KafkaLogNode {
    node_id: String,
    node_ids: Vec<String>,
    state: Arc<Mutex<State>>,
}

impl Node<Payload, RpcPayload> for KafkaLogNode {
    fn from_init(init: Init, rpc_service: RpcService<RpcPayload>) -> anyhow::Result<Self>
    where
        Self: Sized,
    {   
        
        let counter = Arc::new(AtomicUsize::new(0));
        Ok(Self {
            node_id: init.node_id.clone(),
            node_ids: init.node_ids,
            state: Arc::new(Mutex::new(State {
                counter: counter.clone(),
                log_store: LogStore {
                    link_kv_store: LinKvStore {
                        rpc_service,
                        counter,
                        r#type: "lin-kv".to_string(),
                        node_id: init.node_id,
                    },
                }
            })),
        })
    }

    async fn step(
        &mut self,
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
