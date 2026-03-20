use std::{
    collections::VecDeque,
    time::{Duration, SystemTime},
};

use tokio::sync::oneshot;
use uuid::Uuid;

use crate::{
    blocking_list::BlockingClient,
    blocking_stream::{BlockingStreamClient, StreamWait},
    commands::RedisCommand,
    db::{Client, DB, KeyValue},
    execute_commands,
    lists::List,
    resp::Resp,
    valuetype::ValueType,
};

impl DB {
    pub fn execute_commands(&mut self, command: RedisCommand, client_id: Uuid) -> Resp {
        let outcome = match command {
            RedisCommand::Multi => {
                self.multi_list.insert(client_id, vec![]);
                Resp::SimpleString("OK".as_bytes().to_vec())
            }
            RedisCommand::Discard => {
                let exists = self.multi_list.remove(&client_id);
                if exists.is_some() {
                    Resp::SimpleString("OK".as_bytes().to_vec())
                } else {
                    Resp::Error("ERR DISCARD without MULTI".as_bytes().to_vec())
                }
            }
            RedisCommand::Incr { key } => {
                let item = self.database.entry(key).or_insert(KeyValue {
                    expiry: None,
                    value: ValueType::String("0".as_bytes().to_vec()),
                });

                match &item.value {
                    ValueType::String(value) => {
                        if let Ok(num_string) = String::from_utf8(value.clone()) {
                            if let Ok(mut number) = num_string.parse::<i64>() {
                                number += 1;
                                let v = number.to_string().as_bytes().to_vec();
                                item.value = ValueType::String(v);
                                Resp::Integer(number as usize)
                            } else {
                                Resp::Error(
                                    "ERR value is not an integer or out of range"
                                        .as_bytes()
                                        .to_vec(),
                                )
                            }
                        } else {
                            Resp::Error(
                                "ERR value is not an integer or out of range"
                                    .as_bytes()
                                    .to_vec(),
                            )
                        }
                    }
                    _ => panic!("idk if this can happen"),
                }
            }
            RedisCommand::XRead { streams, timeout } => {
                let mut resolved_streams = Vec::new();

                for s_read in &streams {
                    let (t, s) = if s_read.id == "$" {
                        if let Some(kv) = self.database.get(&s_read.key) {
                            if let ValueType::Stream(stream) = &kv.value {
                                stream.get_last_id()
                            } else {
                                (0, 0)
                            }
                        } else {
                            (0, 0)
                        }
                    } else {
                        s_read
                            .id
                            .split_once('-')
                            .map(|(t_str, s_str)| {
                                (
                                    t_str.parse::<u64>().unwrap_or(0),
                                    s_str.parse::<u64>().unwrap_or(0),
                                )
                            })
                            .unwrap_or((0, 0))
                    };

                    resolved_streams.push(StreamWait {
                        key: s_read.key.clone(),
                        time: t,
                        sequence: s,
                    });
                }

                let mut result = VecDeque::new();
                for wait in &resolved_streams {
                    if let Some(kv) = self.database.get(&wait.key) {
                        if let ValueType::Stream(stream) = &kv.value {
                            let entries = stream.get_read_streams(wait.time, wait.sequence);
                            if let Resp::Array(ref arr) = entries {
                                if !arr.is_empty() {
                                    result.push_back(Resp::Array(VecDeque::from([
                                        Resp::BulkString(wait.key.clone()),
                                        entries,
                                    ])));
                                }
                            }
                        }
                    }
                }

                if !result.is_empty() {
                    return Resp::Array(result);
                }

                if let Some(timeout_ms) = timeout {
                    return Resp::BlockingStreamClient {
                        client_id,
                        resolved_streams,
                        timeout_ms,
                    };
                }

                return Resp::NullBulkString;
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
                None => Resp::Array(VecDeque::new()),
                Some(kv) => {
                    if let ValueType::Stream(stream) = &mut kv.value {
                        let entries =
                            stream.get_range(start_time, end_time, start_sequence, end_sequence);
                        entries
                    } else {
                        Resp::Error(
                            b"WRONGTYPE Operation against a key holding the wrong kind of value"
                                .to_vec(),
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
                                    if let Some(client) =
                                        self.blocking_streams.clients.get(client_id)
                                    {
                                        if let Some(wait) =
                                            client.waits.iter().find(|w| w.key == key)
                                        {
                                            if new_id.time > wait.time
                                                || (new_id.time == wait.time
                                                    && new_id.sequence > wait.sequence)
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
                        value: ValueType::List(List::new()),
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
                                found = Some(Resp::Array(VecDeque::from([
                                    Resp::BulkString(key.clone()),
                                    resp,
                                ])));
                                break;
                            }
                        }
                    }
                }
                match found {
                    Some(found) => return found,
                    None => return Resp::BlockingClient { keys, timeout },
                }
            }
            RedisCommand::InternalTimeoutCleanup {
                client_id: target_id,
                ..
            } => {
                if let Some(blocked_client) = self.blocked_list.blocked_list.remove(&target_id) {
                    for queue in self.blocked_list.waiters.values_mut() {
                        queue.retain(|id| *id != target_id);
                    }
                    let _ = blocked_client.response_tx.send(Resp::NullArray);
                }

                if let Some(stream_client) = self.blocking_streams.clients.remove(&target_id) {
                    for queue in self.blocking_streams.waiters.values_mut() {
                        queue.retain(|id| *id != target_id);
                    }
                    let _ = stream_client.response_tx.send(Resp::NullArray);
                } else {
                }

                return Resp::None;
            }
            RedisCommand::LRange { key, start, stop } => {
                if let Some(kv) = self.database.get(&key) {
                    if let ValueType::List(ref list) = kv.value {
                        list.get_list_range(start, stop)
                    } else {
                        Resp::Error(b"WRONGTYPE".to_vec())
                    }
                } else {
                    Resp::Array(VecDeque::new())
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
                let expiry_time = expiry.map(|ms| SystemTime::now() + Duration::from_millis(ms));
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
                        value: ValueType::List(List::new()),
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
            RedisCommand::Error(err) => Resp::Error(err.as_bytes().to_vec()),
            RedisCommand::Exec => {
                if let Some(client) = self.multi_list.remove(&client_id) {
                    return Resp::Exec(client);
                } else {
                    Resp::Error("ERR EXEC without MULTI".as_bytes().to_vec())
                }
            }
        };

        outcome
    }
}
