use crate::command_router::CommandResult;
use crate::db::KeyValue;
use crate::{db::DB, resp::Resp, valuetype::ValueType};
use std::time::{Duration, SystemTime};

pub struct StringCommands;

impl StringCommands {
    pub fn get(db: &mut DB, key: Vec<u8>) -> CommandResult {
        let is_expired = db.database.get(&key).map_or(false, |kv| {
            kv.expiry.map_or(false, |e| e < SystemTime::now())
        });

        if is_expired {
            db.database.remove(&key);
            return CommandResult::Response(Resp::NullBulkString);
        }

        match db.database.get(&key) {
            Some(kv) => {
                if let ValueType::String(s) = &kv.value {
                    CommandResult::Response(Resp::BulkString(s.clone()))
                } else {
                    CommandResult::Response(Resp::Error(b"WRONGTYPE".to_vec()))
                }
            }
            None => CommandResult::Response(Resp::NullBulkString),
        }
    }

    pub fn set(db: &mut DB, key: Vec<u8>, value: Vec<u8>, expiry: Option<u64>) -> CommandResult {
        let expiry_time = expiry.map(|ms| SystemTime::now() + Duration::from_millis(ms));

        db.database.insert(
            key,
            crate::db::KeyValue {
                value: ValueType::String(value),
                expiry: expiry_time,
            },
        );

        CommandResult::Response(Resp::SimpleString(b"OK".to_vec()))
    }

    pub fn incr(db: &mut DB, key: Vec<u8>) -> CommandResult {
        let item = db.database.entry(key).or_insert(KeyValue {
            expiry: None,
            value: ValueType::String(b"0".to_vec()),
        });

        let response = match &item.value {
            ValueType::String(value) => {
                if let Ok(num_string) = String::from_utf8(value.clone()) {
                    if let Ok(mut number) = num_string.parse::<i64>() {
                        number += 1;
                        item.value = ValueType::String(number.to_string().into_bytes());
                        Resp::Integer(number as usize)
                    } else {
                        Resp::Error(b"ERR value is not an integer or out of range".to_vec())
                    }
                } else {
                    Resp::Error(b"ERR value is not an integer or out of range".to_vec())
                }
            }
            _ => Resp::Error(b"ERR value is not an integer or out of range".to_vec()),
        };

        CommandResult::Response(response)
    }
}
