use crate::blocking_list::{BlockingClient, BlockingList};
use crate::blocking_stream::{BlockingStreamClient, BlockingStreams, StreamWait};
use crate::commands::RedisCommand;
use crate::lists::List;
use crate::resp::Resp;
use crate::valuetype::ValueType;
use std::collections::HashMap;
use std::time::{Duration, SystemTime};
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

pub struct DB {
    pub database: HashMap<Vec<u8>, KeyValue>,
    pub receiver: mpsc::Receiver<Client>,
    pub blocked_list: BlockingList,
    pub blocking_streams: BlockingStreams,
    pub sender: mpsc::Sender<Client>,
}
#[derive(Debug)]
pub struct Client {
    pub client_id: Uuid,
    pub timeout: Option<Duration>,
    pub response_tx: oneshot::Sender<Resp>,
    pub command: RedisCommand,
}

pub struct KeyValue {
    expiry: Option<SystemTime>,
    pub(crate) value: ValueType,
}
impl KeyValue {
    pub fn new(expiry: Option<SystemTime>, value: ValueType) -> Self {
        KeyValue { expiry, value }
    }
}
impl DB {
    pub fn new() -> Self {
        let (pipeline_tx, pipeline_rx) = tokio::sync::mpsc::channel::<Client>(100);

        Self {
            database: HashMap::new(),
            sender: pipeline_tx,
            receiver: pipeline_rx,
            blocked_list: BlockingList {
                blocked_list: Default::default(),
                waiters: HashMap::new(),
            },
            blocking_streams: BlockingStreams {
                waiters: HashMap::new(),
                clients: HashMap::new(),
            },
        }
    }
    fn wake_stream_client(&mut self, client_id: Uuid) {

        if let Some(client) = self.blocking_streams.clients.remove(&client_id) {

            // Remove from all wait queues
            for q in self.blocking_streams.waiters.values_mut() {
                q.retain(|id| *id != client_id);
            }

            let mut result = Vec::new();

            for wait in &client.waits {
                if let Some(kv) = self.database.get(&wait.key) {
                    if let ValueType::Stream(stream) = &kv.value {
                        let entries = stream.get_read_streams(

                            wait.time,
                            wait.sequence,
                        );

                        if let Resp::Array(ref arr) = entries {
                            if !arr.is_empty() {
                                result.push(Resp::Array(vec![
                                    Resp::BulkString(wait.key.clone()),
                                    entries,
                                ]));
                            }
                        }
                    }
                }
            }

            let _ = client.response_tx.send(Resp::Array(result));
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
                RedisCommand::XRead { streams, timeout } => {
                    let mut result = Vec::new();

                    for streamread in &streams {
                        if let Some(kv) = self.database.get(&streamread.key) {
                            if let ValueType::Stream(stream) = &kv.value {
                                let entries = stream.get_read_streams(
                                    streamread.time,
                                    streamread.sequence,
                                );

                                if let Resp::Array(ref arr) = entries {
                                    if !arr.is_empty() {
                                        result.push(Resp::Array(vec![
                                            Resp::BulkString(streamread.key.clone()),
                                            entries,
                                        ]));
                                    }
                                }
                            }
                        }
                    }

                    if !result.is_empty() {
                        let _ = response_tx.send(Resp::Array(result));
                        continue;
                    }

                    if let Some(timeout) = timeout {

                        let waits = streams.iter().map(|s| StreamWait {
                            key: s.key.clone(),
                            time: s.time,
                            sequence: s.sequence,
                        }).collect();

                        let client = BlockingStreamClient {
                            id: client_id,
                            response_tx,
                            waits,
                        };

                        for wait in &client.waits {
                            self.blocking_streams
                                .waiters
                                .entry(wait.key.clone())
                                .or_default()
                                .push_back(client_id);
                        }

                        self.blocking_streams.clients.insert(client_id, client);

                        if timeout > 0.0 {
                            let sender_clone = self.sender.clone();
                            tokio::spawn(async move {
                                tokio::time::sleep(Duration::from_millis(timeout as u64)).await;
                                let _ = sender_clone.send(Client {
                                    client_id,
                                    timeout: None,
                                    response_tx: oneshot::channel().0,
                                    command: RedisCommand::InternalTimeoutCleanup { client_id },
                                }).await;

                            });
                        }

                        continue;
                    }

                    Resp::NullBulkString
                }
                RedisCommand::Type(key) => {
                    let type_str = match self.database.get(&key) {
                        None => b"none".as_slice(),
                        Some(kv) => kv.value.get_value_type(),
                    };
                    Resp::SimpleString(type_str.to_vec())
                }
                RedisCommand::XRange {
                    key,
                    start_time,
                    end_time,
                    start_sequence,
                    end_sequence,
                } => match self.database.get_mut(&key) {
                    None => Resp::Array(Vec::new()),
                    Some(kv) => {
                        if let ValueType::Stream(stream) = &mut kv.value {
                            let entries = stream.get_range(
                                start_time,
                                end_time,
                                start_sequence,
                                end_sequence,
                            );
                            entries
                        } else {
                            Resp::Error(
                                    b"WRONGTYPE Operation against a key holding the wrong kind of value".to_vec()
                                )
                        }
                    }
                },
                RedisCommand::XAdd { key, fields, id } => {
                    let stream = self
                        .database
                        .entry(key.clone())
                        .or_insert_with(|| KeyValue::new(None, ValueType::stream()));

                    if let ValueType::Stream(stream) = &mut stream.value {
                        match stream.add_entry(fields, id) {
                            Ok(new_id) => {

                                let id_str = new_id.get_id_string();

                                // WAKE BLOCKED STREAM CLIENTS
                                if let Some(queue) = self.blocking_streams.waiters.get(&key) {

                                    let mut to_wake = Vec::new();

                                    for client_id in queue {
                                        if let Some(client) = self.blocking_streams.clients.get(client_id) {

                                            if let Some(wait) = client.waits.iter()
                                                .find(|w| w.key == key) {

                                                if new_id.time > wait.time ||
                                                    (new_id.time == wait.time && new_id.sequence > wait.sequence)
                                                {
                                                    to_wake.push(*client_id);
                                                }
                                            }
                                        }
                                    }

                                    for client_id in to_wake {
                                        self.wake_stream_client(client_id);
                                    }
                                }

                                Resp::BulkString(id_str.into_bytes())
                            }
                            Err(err) => Resp::Error(err.into_bytes()),
                        }
                    } else {
                        Resp::Error(
                            b"WRONGTYPE Operation against a key holding the wrong kind of value"
                                .to_vec(),
                        )
                    }
                }
                RedisCommand::Get(key) => {
                    let is_expired = self.database.get(&key).map_or(false, |kv| {
                        kv.expiry.map_or(false, |e| e < SystemTime::now())
                    });

                    if is_expired {
                        self.database.remove(&key);
                        Resp::NullBulkString
                    } else {
                        match self.database.get(&key) {
                            Some(kv) => {
                                if let ValueType::String(s) = &kv.value {
                                    Resp::BulkString(s.clone())
                                } else {
                                    Resp::Error(b"WRONGTYPE Operation against a key holding the wrong kind of value".to_vec())
                                }
                            }
                            None => Resp::NullBulkString,
                        }
                    }
                }
                RedisCommand::LPush { key, elements } => {
                    let entry = self
                        .database
                        .entry(key.clone())
                        .or_insert_with(|| KeyValue {
                            expiry: None,
                            value: ValueType::List(List::new(key.clone())),
                        });

                    if let ValueType::List(ref mut list) = entry.value {
                        list.lpush(elements)
                    } else {
                        Resp::Error(b"WRONGTYPE".to_vec())
                    }
                }
                RedisCommand::LLen(key) => {
                    if let Some(kv) = self.database.get(&key) {
                        if let ValueType::List(ref list) = kv.value {
                            list.llen()
                        } else {
                            Resp::Error(b"WRONGTYPE".to_vec())
                        }
                    } else {
                        Resp::Integer(0)
                    }
                }
                RedisCommand::Ping => Resp::SimpleString(b"PONG".to_vec()),
                RedisCommand::Echo(data) => Resp::BulkString(data),
                RedisCommand::BLPop { keys, timeout } => {
                    let mut found = None;
                    for key in &keys {
                        if let Some(kv) = self.database.get_mut(key) {
                            if let ValueType::List(ref mut list) = kv.value {
                                let resp = list.lpop(key, 1);

                                if !matches!(resp, Resp::NullBulkString) {
                                    found = Some(Resp::Array(vec![
                                        Resp::BulkString(key.clone()),
                                        resp,
                                    ]));
                                    break;
                                }
                            }
                        }
                    }
                    match found {
                        Some(found) => {
                            response_tx.send(found).unwrap();
                            continue;
                        }
                        None => {
                            let client = BlockingClient {
                                id: client_id,
                                response_tx,
                            };

                            self.blocked_list.register(keys, client);
                            if timeout > 0.0 {
                                let sender_clone = self.sender.clone();
                                let id = client_id;

                                tokio::spawn(async move {
                                    tokio::time::sleep(Duration::from_secs_f64(timeout)).await;

                                    let _ = sender_clone
                                        .send(Client {
                                            client_id: id,
                                            timeout: None,
                                            response_tx: oneshot::channel().0,
                                            command: RedisCommand::InternalTimeoutCleanup {
                                                client_id: id,
                                            },
                                        })
                                        .await;
                                });
                            }
                        }
                    }

                    continue;
                }
                RedisCommand::InternalTimeoutCleanup { client_id: target_id, .. } => {

                    // Check Lists
                    if let Some(blocked_client) = self.blocked_list.blocked_list.remove(&target_id) {
                        for queue in self.blocked_list.waiters.values_mut() {
                            queue.retain(|id| *id != target_id);
                        }
                        let _ = blocked_client.response_tx.send(Resp::NullArray);
                    }

                    // Check Streams
                    if let Some(stream_client) = self.blocking_streams.clients.remove(&target_id) {
                        for queue in self.blocking_streams.waiters.values_mut() {
                            queue.retain(|id| *id != target_id);
                        }
                        let _ = stream_client.response_tx.send(Resp::NullBulkString);
                    } else {
                    }

                    continue; 
                }
                RedisCommand::LRange { key, start, stop } => {
                    if let Some(kv) = self.database.get(&key) {
                        if let ValueType::List(ref list) = kv.value {
                            list.get_list_range(start, stop)
                        } else {
                            Resp::Error(b"WRONGTYPE".to_vec())
                        }
                    } else {
                        Resp::Array(vec![])
                    }
                }

                RedisCommand::LPop { key, count } => {
                    if let Some(kv) = self.database.get_mut(&key) {
                        if let ValueType::List(ref mut list) = kv.value {
                            list.lpop(&key, count)
                        } else {
                            Resp::Error(b"WRONGTYPE".to_vec())
                        }
                    } else {
                        Resp::NullBulkString
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
                    Resp::SimpleString(b"OK".to_vec())
                }

                RedisCommand::RPush { key, elements } => {
                    let entry = self
                        .database
                        .entry(key.clone())
                        .or_insert_with(|| KeyValue {
                            expiry: None,
                            value: ValueType::List(List::new(key.clone())),
                        });

                    if let ValueType::List(list) = &mut entry.value {
                        let mut length = list.list.len();
                        for element in elements {
                            length += 1;
                            if !self.blocked_list.wake_one(&key, element.clone()) {
                                list.rpush(vec![element]);
                            }
                        }

                        Resp::Integer(length)
                    } else {
                        Resp::Error(b"WRONGTYPE".to_vec())
                    }
                }
            };

            let _ = response_tx.send(outcome);
        }
    }
}
