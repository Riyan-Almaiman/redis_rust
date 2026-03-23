use std::collections::VecDeque;

use crate::commands::CommandResult;
use crate::db::{KeyValue, DB};
use crate::lists::List;
use crate::resp::Resp;
use crate::sorted_list::SortedList;
use crate::valuetype::ValueType;

pub struct ListCommands;

impl ListCommands {
    pub fn lpush(db: &mut DB, key: Vec<u8>, elements: Vec<Vec<u8>>) -> CommandResult {
        let entry = db.database.entry(key).or_insert_with(|| KeyValue {
            expiry: None,
            value: ValueType::List(List::new()),
        });

        match &mut entry.value {
            ValueType::List(list) => CommandResult::Response(list.lpush(elements)),
            _ => CommandResult::Response(Resp::Error(b"WRONGTYPE".to_vec())),
        }
    }

    pub fn zrem(db: &mut DB, key: String, value: String) -> CommandResult {
        let response = match db.database.get_mut(key.as_bytes()) {
            Some(kv) => match &mut kv.value {
                ValueType::SortedList(sorted_list) => sorted_list.remove(&value),
                _ => false,
            },
            None => false,
        };
        if response {
            CommandResult::Response(Resp::Integer(1))
        } else {
            CommandResult::Response(Resp::Integer(0))
        }
    }
    pub fn zscore(db: &mut DB, key: String, value: String) -> CommandResult {
        let response = match db.database.get(key.as_bytes()) {
            Some(kv) => match &kv.value {
                ValueType::SortedList(sorted_list) => sorted_list.values.get(&value),
                _ => None,
            },
            None => None,
        };
        if let Some(score) = response {
            CommandResult::Response(Resp::BulkString(score.to_string().into_bytes()))
        } else {
            CommandResult::Response(Resp::NullBulkString)
        }
    }
    pub fn zcard(db: &mut DB, key: String) -> CommandResult {
        let response = match db.database.get(key.as_bytes()) {
            Some(kv) => match &kv.value {
                ValueType::SortedList(sorted_list) => sorted_list.values.len(),
                _ => 0,
            },
            None => 0,
        };
        CommandResult::Response(Resp::Integer(response ))
    }
    pub fn zrange(db: &mut DB, key: String, start: isize, end: isize) -> CommandResult {
        let response = match db.database.get(key.as_bytes()) {
            Some(kv) => match &kv.value {
                ValueType::SortedList(sorted_list) => Resp::Array(sorted_list.zrange(start, end)),

                _ => Resp::NullArray,
            },
            None => Resp::Array(VecDeque::new()),
        };
        CommandResult::Response(response)
    }
    pub fn zrank(db: &mut DB, key: String, value: String) -> CommandResult {
        let response = match db.database.get(key.as_bytes()) {
            Some(kv) => match &kv.value {
                ValueType::SortedList(sorted_list) => sorted_list.rank_of(&value),
                _ => None,
            },
            None => None,
        };
        if let Some(rank) = response {
            CommandResult::Response(Resp::Integer(rank ))
        } else {
            CommandResult::Response(Resp::NullBulkString)
        }
    }
    pub fn zadd(db: &mut DB, key: String, values: Vec<(f64, String)>) -> CommandResult {
        let  key_map = db.database.entry(key.into()).or_insert(KeyValue{expiry: None, value: ValueType::SortedList(SortedList::new())} );
        let mut created = 0;
        for val in &values {
                if let ValueType::SortedList(ref mut value) = key_map.value {
                   
                    if value.zadd(val.1.clone(), val.0) {
                        created+=1;
                    }
                }
        }


        CommandResult::Response(Resp::Integer(created))

    }
    pub fn rpush(db: &mut DB, key: Vec<u8>, elements: Vec<Vec<u8>>) -> CommandResult {
        let entry = db
            .database
            .entry(key.clone())
            .or_insert_with(|| KeyValue::new(None, ValueType::List(List::new())));

        match &mut entry.value {
            ValueType::List(list) => {
                let mut length = list.list.len();
                for element in elements {
                    length += 1;
                    if !db.blocking.lists.wake_one(&key, element.clone()) {
                        list.rpush(vec![element]);
                    }
                }

                CommandResult::Response(Resp::Integer(length))
            }
            _ => CommandResult::Response(Resp::Error(b"WRONGTYPE".to_vec())),
        }
    }

    pub fn llen(db: &mut DB, key: Vec<u8>) -> CommandResult {
        let response = match db.database.get(&key) {
            Some(kv) => match &kv.value {
                ValueType::List(list) => list.llen(),
                _ => Resp::Error(b"WRONGTYPE".to_vec()),
            },
            None => Resp::Integer(0),
        };

        CommandResult::Response(response)
    }

    pub fn lpop(db: &mut DB, key: Vec<u8>, count: usize) -> CommandResult {
        let response = match db.database.get_mut(&key) {
            Some(kv) => match &mut kv.value {
                ValueType::List(list) => list.lpop(&key, count),
                _ => Resp::Error(b"WRONGTYPE".to_vec()),
            },
            None => Resp::NullBulkString,
        };

        CommandResult::Response(response)
    }

    pub fn lrange(db: &mut DB, key: Vec<u8>, start: i64, stop: i64) -> CommandResult {
        let response = match db.database.get(&key) {
            Some(kv) => match &kv.value {
                ValueType::List(list) => list.get_list_range(start, stop),
                _ => Resp::Error(b"WRONGTYPE".to_vec()),
            },
            None => Resp::Array(VecDeque::new()),
        };

        CommandResult::Response(response)
    }

    pub fn blpop(db: &mut DB, keys: Vec<Vec<u8>>, timeout: f64) -> CommandResult {
        for key in &keys {
            if let Some(kv) = db.database.get_mut(key) {
                if let ValueType::List(list) = &mut kv.value {
                    let resp = list.lpop(key, 1);

                    if !matches!(resp, Resp::NullBulkString) {
                        return CommandResult::Response(Resp::Array(VecDeque::from([
                            Resp::BulkString(key.clone()),
                            resp,
                        ])));
                    }
                }
            }
        }

        CommandResult::BlockList { keys, timeout }
    }
}
