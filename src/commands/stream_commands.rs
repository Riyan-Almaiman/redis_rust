use std::collections::VecDeque;

use uuid::Uuid;

use crate::blocking_stream::StreamWait;
use crate::command_router::CommandResult;
use crate::commands_parser::StreamRead;
use crate::db::{KeyValue, DB};
use crate::resp::Resp;
use crate::valuetype::ValueType;

pub struct StreamCommands;

impl StreamCommands {
    pub fn xread(
        db: &mut DB,
        streams: Vec<StreamRead>,
        timeout: Option<f64>,
        client_id: Uuid,
    ) -> CommandResult {
        let mut resolved_streams = Vec::new();

        for stream_read in &streams {
            let (time, sequence) = if stream_read.id == "$" {
                match db.database.get(&stream_read.key) {
                    Some(kv) => match &kv.value {
                        ValueType::Stream(stream) => stream.get_last_id(),
                        _ => (0, 0),
                    },
                    None => (0, 0),
                }
            } else {
                stream_read
                    .id
                    .split_once('-')
                    .map(|(time_str, sequence_str)| {
                        (
                            time_str.parse::<u64>().unwrap_or(0),
                            sequence_str.parse::<u64>().unwrap_or(0),
                        )
                    })
                    .unwrap_or((0, 0))
            };

            resolved_streams.push(StreamWait {
                key: stream_read.key.clone(),
                time,
                sequence,
            });
        }

        let mut result = VecDeque::new();
        for wait in &resolved_streams {
            if let Some(kv) = db.database.get(&wait.key) {
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
            return CommandResult::Response(Resp::Array(result));
        }

        if let Some(timeout_ms) = timeout {
            return CommandResult::BlockStream {
                client_id,
                streams: resolved_streams,
                timeout_ms,
            };
        }

        CommandResult::Response(Resp::NullBulkString)
    }

    pub fn xrange(
        db: &mut DB,
        key: Vec<u8>,
        start_time: u64,
        end_time: u64,
        start_sequence: u64,
        end_sequence: u64,
    ) -> CommandResult {
        let response = match db.database.get_mut(&key) {
            None => Resp::Array(VecDeque::new()),
            Some(kv) => match &mut kv.value {
                ValueType::Stream(stream) => {
                    stream.get_range(start_time, end_time, start_sequence, end_sequence)
                }
                _ => Resp::Error(
                    b"WRONGTYPE Operation against a key holding the wrong kind of value".to_vec(),
                ),
            },
        };

        CommandResult::Response(response)
    }

    pub fn xadd(
        db: &mut DB,
        key: Vec<u8>,
        fields: Vec<(Vec<u8>, Vec<u8>)>,
        id: crate::commands_parser::StreamEntryIdCommandType,
    ) -> CommandResult {
        let stream_value = db
            .database
            .entry(key.clone())
            .or_insert_with(|| KeyValue::new(None, ValueType::stream()));

        let response = match &mut stream_value.value {
            ValueType::Stream(stream) => match stream.add_entry(fields, id) {
                Ok(new_id) => {
                    let id_str = new_id.get_id_string();

                    if let Some(queue) = db.blocking.streams.waiters.get(&key) {
                        let mut to_wake = Vec::new();

                        for client_id in queue {
                            if let Some(client) = db.blocking.streams.clients.get(client_id) {
                                if let Some(wait) = client.waits.iter().find(|wait| wait.key == key)
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
                            db.wake_stream_client(client_id);
                        }
                    }

                    Resp::BulkString(id_str.into_bytes())
                }
                Err(err) => Resp::Error(err.into_bytes()),
            },
            _ => Resp::Error(
                b"WRONGTYPE Operation against a key holding the wrong kind of value".to_vec(),
            ),
        };

        CommandResult::Response(response)
    }
}
