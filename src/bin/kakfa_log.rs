use anyhow::Context;
use gossip::{Init, Message, Node, main_loop};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
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

#[derive(Debug, Clone, Default)]
pub struct MemLog {
    committed: Vec<Value>,
    pending: VecDeque<Value>,
}

impl MemLog {
    pub fn append(&mut self, value: Value) -> Offset {
        self.pending.push_back(value);
        self.committed.len() + self.pending.len()
    }

    pub fn commit(&mut self, offset: Offset) -> anyhow::Result<()> {
        let to_commit = offset.saturating_sub(self.committed.len());

        for _ in 0..=to_commit {
            let new_commit = self
                .pending
                .pop_front()
                .context("commit length should be checked")?;
            self.committed.push(new_commit);
        }

        Ok(())
    }

    pub fn get_committed_offset(&self) -> Offset {
        self.committed.len()
    }

    pub fn poll(&self, offset: Offset) -> Vec<[usize; 2]> {
        if offset >= self.committed.len() {
            return vec![];
        }
        let result: Vec<[usize; 2]> = self.committed[offset..]
            .iter()
            .copied()
            .enumerate()
            .map(|(i, v)| [i, v])
            .collect();
        result
    }
}

#[derive(Debug, Default, Clone)]
pub struct LogStore {
    logs: HashMap<LogId, MemLog>,
}

impl LogStore {
    pub fn poll(&self, offsets: HashMap<LogId, Offset>) -> HashMap<LogId, Vec<[usize; 2]>> {
        offsets
            .iter()
            .filter_map(|(key, offset)| {
                if let Some(log) = self.logs.get(key) {
                    return Some((key.clone(), log.poll(*offset)));
                }
                None
            })
            .collect()
    }

    pub fn commit(&mut self, commits: HashMap<LogId, Offset>) -> anyhow::Result<()> {
        for (key, offset) in commits {
            if let Some(log) = self.logs.get_mut(&key) {
                log.commit(offset)?;
            }
        }

        Ok(())
    }

    pub fn append(&mut self, key: LogId, value: Value) -> Offset {
        self.logs.entry(key).or_default().append(value)
    }

    pub fn list_committed_offset(&self, keys: Vec<LogId>) -> HashMap<LogId, Offset> {
        keys.into_iter()
            .filter_map(|key| self.logs.get(&key).map(|log| (key, log)))
            .map(|(key, log)| (key, log.get_committed_offset()))
            .collect()
    }
}

#[derive(Debug, Clone, Default)]
pub struct State {
    pub counter: usize,
    pub log_store: LogStore,
}

pub struct KafkaLogNode {
    node_id: String,
    node_ids: Vec<String>,
    state: Arc<Mutex<State>>,
}

impl Node<Payload> for KafkaLogNode {
    fn from_init(init: Init) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            node_id: init.node_id,
            node_ids: init.node_ids,
            state: Default::default(),
        })
    }

    async fn heartbeat(&self, _tx: UnboundedSender<Message<Payload>>) -> anyhow::Result<()> {
        loop {
            tokio::time::sleep(Duration::from_secs(1000)).await;
            tracing::info!("heartbeat loop");
        }
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
                let offset = s.log_store.append(key, msg);
                reply.body.payload = Payload::SendOk { offset };
                tx.send(reply)?;
            }
            Payload::Poll { offsets } => {
                tracing::info!("Poll offsets: {:?}", offsets);
                let offsets = s.log_store.poll(offsets);
                reply.body.payload = Payload::PollOk { msgs: offsets };
                tx.send(reply)?;
            }
            Payload::CommitOffsets { offsets } => {
                tracing::info!("Commit offsets: {:?}", offsets);
                s.log_store.commit(offsets)?;
                reply.body.payload = Payload::CommitOffsetsOk;
                tx.send(reply)?;
            }
            Payload::ListCommittedOffsets { keys } => {
                tracing::info!("List committed offsets: {:?}", keys);
                let offsets = s.log_store.list_committed_offset(keys);
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
    main_loop::<KafkaLogNode, _>().await
}
