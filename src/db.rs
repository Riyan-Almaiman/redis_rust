use crate::commands::{RedisCommand};
use crate::lists::{BlockingClient, List};
use crate::resp::Resp;
use indexmap::IndexMap;
use std::collections::HashMap;
use std::time::{Duration, SystemTime};
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

pub struct DB {
    pub database: HashMap<Vec<u8>, KeyValue>,
    pub receiver: mpsc::Receiver<Client>,
    pub sender: mpsc::Sender<Client>,
}
#[derive(Debug)]
pub struct Client {
    pub client_id: Uuid,
    pub timeout: Option<Duration>,
    pub response_tx: oneshot::Sender<Resp>,
    pub command: RedisCommand,
}
#[derive(Debug, Clone)]
pub struct EntryId {
    pub time: u64,
    pub sequence: u64,
    pub id: String,
}

#[derive(Debug, Clone)]
pub enum CommandOutcome {
    Done(Resp),
    Blocked {
        keys: Vec<Vec<u8>>,
        timeout: f64,
        id: Uuid,
    },
}
#[derive(Debug, Clone)]
pub struct StreamEntry {
    pub entry_id: EntryId,
    pub fields: Vec<(Vec<u8>, Vec<u8>)>,
}

#[derive(Debug, Clone)]
pub struct Stream {
    pub entries: Vec<StreamEntry>,
    pub last_id: Option<EntryId>,
}
impl Stream{

    pub fn new() -> Self {
        Stream{entries: Vec::new(), last_id: None}
    }
    fn add_entry(&mut self, entry: StreamEntry) -> Result<(), String> {
        let res = match &self.last_id {
            None => {
                if entry.entry_id.time == 0 && entry.entry_id.sequence == 0 {
                    return  Err("RR The ID specified in XADD must be greater than 0-0".to_string())
                }
                self.entries.push(entry)

            }
            Some(id) => {
                if !Self::validate_new_entry(&id, &entry.entry_id) {
                     return Err("ERR The ID specified in XADD is equal or smaller than the target stream top item".to_string())
                }

                self.entries.push(entry)

            }
        };
        Ok(res)

    }
    fn validate_new_entry(last_entry_id: &EntryId, new_entry_id: &EntryId) -> bool {
        if last_entry_id.time > new_entry_id.time {
            return false;
        }
        if last_entry_id.time == new_entry_id.time && last_entry_id.sequence > new_entry_id.sequence {
                return false;
            }

        true
    }


}
enum ValueType {
    String(Vec<u8>),
    List(List),
    Set,
    ZSet,
    Hash,
    Stream(Stream),
    VectorSet
}
impl ValueType {
    fn stream() -> Self {
        ValueType::Stream(Stream::new())
    }

}
struct KeyValue {
    expiry: Option<SystemTime>,
    value: ValueType,
}
 impl KeyValue {
    pub fn new(expiry: Option<SystemTime>, value: ValueType) -> Self {
        KeyValue{expiry, value}
    }
}
impl DB {
    pub fn new() -> Self {
        let (pipeline_tx, pipeline_rx) = tokio::sync::mpsc::channel::<Client>(100);

        Self {
            database: HashMap::new(),
            sender: pipeline_tx,
            receiver: pipeline_rx,
        }
    }

    pub async fn start(&mut self) {
        while let Some(request) = self.receiver.recv().await {
            let Client {
                command,
                response_tx,
                client_id,
                ..
            } = request;

            let outcome = match command {
                RedisCommand::Type(key) => {
                    let type_str = match self.database.get(&key) {
                        None => b"none".as_slice(),
                        Some(kv) => match kv.value {
                            ValueType::String(_) => b"string".as_slice(),
                            ValueType::List(_) => b"list".as_slice(),
                            ValueType::Stream(_) => b"stream".as_slice(),
                            _ => b"none".as_slice(),
                        },
                    };
                    CommandOutcome::Done(Resp::SimpleString(type_str.to_vec()))
                }
                RedisCommand::XAdd { key, entry } => {
                    let stream = self.database.entry(key.clone()).or_insert_with(|| {
                        KeyValue::new(None, ValueType::stream())
                    });
                    if let ValueType::Stream( stream) = &mut stream.value {
                        let new_entry = StreamEntry {
                            entry_id: entry.entry_id.clone(),
                            fields: entry.fields,
                        };
                        let result =  stream.add_entry(new_entry);
                        match result{
                            Ok(_) => {
                                stream.last_id = Some(entry.entry_id.clone());

                                CommandOutcome::Done(Resp::BulkString(entry.entry_id.id.into_bytes()))
                            }
                            Err(err) => {
                                CommandOutcome::Done(Resp::Error(err.into_bytes()))
                            }
                        }

                    } else {
                        CommandOutcome::Done(Resp::Error(b"WRONGTYPE Operation against a key holding the wrong kind of value".to_vec()))
                    }
                }
                RedisCommand::Get(key) => {
                    let is_expired = self.database.get(&key).map_or(false, |kv| {
                        kv.expiry.map_or(false, |e| e < SystemTime::now())
                    });

                    if is_expired {
                        self.database.remove(&key);
                        CommandOutcome::Done(Resp::NullBulkString)
                    } else {
                        match self.database.get(&key) {
                            Some(kv) => {
                                if let ValueType::String(s) = &kv.value {
                                    CommandOutcome::Done(Resp::BulkString(s.clone()))
                                } else {
                                    CommandOutcome::Done(Resp::Error(b"WRONGTYPE Operation against a key holding the wrong kind of value".to_vec()))
                                }
                            }
                            None => CommandOutcome::Done(Resp::NullBulkString),
                        }
                    }
                }
                RedisCommand::LPush { key, elements } => {
                    let entry = self.database.entry(key.clone()).or_insert_with(|| KeyValue {
                        expiry: None,
                        value: ValueType::List(List::new(key.clone())),
                    });

                    if let ValueType::List(ref mut list) = entry.value {
                        list.lpush(elements)
                    } else {
                        CommandOutcome::Done(Resp::Error(b"WRONGTYPE".to_vec()))
                    }
                }
                RedisCommand::LLen(key) => {
                    if let Some(kv) = self.database.get(&key) {
                        if let ValueType::List(ref list) = kv.value {
                            list.llen()
                        } else {
                            CommandOutcome::Done(Resp::Error(b"WRONGTYPE".to_vec()))
                        }
                    } else {
                        CommandOutcome::Done(Resp::Integer(0))
                    }
                }
                RedisCommand::Ping => CommandOutcome::Done(Resp::SimpleString(b"PONG".to_vec())),
                RedisCommand::Echo(data) => CommandOutcome::Done(Resp::BulkString(data)),
                RedisCommand::BLPop { keys, timeout } => {
                    let mut satisfied = false;
                    // Wrap the sender so we can "take" it once
                    let mut tx_wrapper = Some(response_tx);

                    for key in &keys {
                        if let Some(kv) = self.database.get_mut(key) {
                            if let ValueType::List(ref mut list) = kv.value {
                                if let Some(tx) = tx_wrapper.take() {
                                    list.try_blpop(client_id, tx, timeout, self.sender.clone());
                                    satisfied = true;
                                }
                                break;
                            }
                        }
                    }

                    if !satisfied {
                        if let Some(tx) = tx_wrapper.take() {
                            let first_key = keys[0].clone();
                            let mut new_list = List::new(first_key.clone());
                            new_list.create_blocking_client(&keys, timeout, client_id, tx, self.sender.clone());

                            self.database.insert(first_key, KeyValue {
                                expiry: None,
                                value: ValueType::List(new_list),
                            });
                        }
                    }
                    continue;
                }
                RedisCommand::InternalTimeoutCleanup { key, client_id } => {
                    if let Some(kv) = self.database.get_mut(key.as_bytes()) {
                        if let ValueType::List(ref mut list) = kv.value {
                            if let Some(blocked_client) = list.blocking_clients.shift_remove(&client_id) {
                                let _ = blocked_client.response_tx.send(Resp::NullArray);
                            }
                        }
                    }
                    continue;
                }
                RedisCommand::LRange { key, start, stop } => {
                    if let Some(kv) = self.database.get(&key) {
                        if let ValueType::List(ref list) = kv.value {
                            list.get_list_range(start, stop)
                        } else {
                            CommandOutcome::Done(Resp::Error(b"WRONGTYPE".to_vec()))
                        }
                    } else {
                        CommandOutcome::Done(Resp::Array(vec![]))
                    }
                }

                RedisCommand::LPop { key, count } => {
                    if let Some(kv) = self.database.get_mut(&key) {
                        if let ValueType::List(ref mut list) = kv.value {
                            list.lpop(&key, count)
                        } else {
                            CommandOutcome::Done(Resp::Error(b"WRONGTYPE".to_vec()))
                        }
                    } else {
                        CommandOutcome::Done(Resp::NullBulkString)
                    }
                }

                RedisCommand::Set { key, value, expiry } => {
                    let expiry_time =
                        expiry.map(|ms| SystemTime::now() + Duration::from_millis(ms));
                    self.database.insert(
                        key,
                        KeyValue {
                            value: ValueType::String(value),
                            expiry: expiry_time,
                        },
                    );
                    CommandOutcome::Done(Resp::SimpleString(b"OK".to_vec()))
                }

                RedisCommand::RPush { key, elements } => {
                    let entry = self
                        .database
                        .entry(key.clone())
                        .or_insert_with(|| KeyValue {
                            expiry: None,
                            value: ValueType::List(List::new(key.clone())),
                        });

                    if let ValueType::List(ref mut list) = entry.value {
                        list.rpush(elements)
                    } else {
                        CommandOutcome::Done(Resp::Error(b"WRONGTYPE".to_vec()))
                    }
                }

                _ => CommandOutcome::Done(Resp::NullArray),
            };

            match outcome {
                CommandOutcome::Done(resp) => {
                    let _ = response_tx.send(resp);
                }
                CommandOutcome::Blocked { keys, timeout, id } => {
                    let entry = self
                        .database
                        .entry(keys[0].clone())
                        .or_insert_with(|| KeyValue {
                            expiry: None,
                            value: ValueType::List(List::new(keys[0].clone())),
                        });

                    if let ValueType::List(ref mut list) = entry.value {
                        list.create_blocking_client(
                            &keys,
                            timeout,
                            id,
                            response_tx,
                            self.sender.clone(),
                        );
                    }
                }
            }
        }
    }
}
