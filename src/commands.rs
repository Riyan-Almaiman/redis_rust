use uuid::Uuid;
use crate::commands::RedisCommand::XRange;

#[derive(Clone, Debug)]
pub enum StreamEntryIdCommandType {
    Explicit { time: u64, sequence: u64 },
    GenerateTimeAndSequence,

    GenerateOnlySequence { time: u64 },
}

#[derive(Debug, Clone)]
pub enum RedisCommand {
    Ping,
    Echo(Vec<u8>),
    Set {
        key: Vec<u8>,
        value: Vec<u8>,
        expiry: Option<u64>,
    },
    Get(Vec<u8>),
    RPush {
        key: Vec<u8>,
        elements: Vec<Vec<u8>>,
    },
    LPush {
        key: Vec<u8>,
        elements: Vec<Vec<u8>>,
    },
    InternalTimeoutCleanup {
        key: String,
        client_id: Uuid,
    },
    LRange {
        key: Vec<u8>,
        start: i64,
        stop: i64,
    },
    LLen(Vec<u8>),
    LPop {
        key: Vec<u8>,
        count: usize,
    },
    BLPop {
        keys: Vec<Vec<u8>>,
        timeout: f64,
    },
    Type(Vec<u8>),
    XAdd {
        key: Vec<u8>,
        fields: Vec<(Vec<u8>, Vec<u8>)>,
        id: StreamEntryIdCommandType,
    },
    XRange {
        key: Vec<u8>,
        start_time: u64,
        end_time: u64,
        start_sequence: u64,
        end_sequence: u64,
    }
}
impl RedisCommand {
    pub fn from_resp(cmds: &[Vec<u8>]) -> Result<Self, String> {
        if cmds.is_empty() {
            return Err("Empty command".to_string());
        }

        let command_name = std::str::from_utf8(&cmds[0])
            .map(|s| s.to_lowercase())
            .map_err(|_| "Invalid UTF-8 in command")?;

        match command_name.as_str() {
            "xrange" => {
                let key = cmds[1].clone();
                Self::parse_xrange(key, cmds)
            }
            "xadd" => {
                if cmds.len() < 5 || (cmds.len() - 3) % 2 != 0 {
                    return Err(
                        "XADD requires key, ID, and at least one field-value pair".to_string()
                    );
                }

                let key = cmds[1].clone();
                let mut entries = Vec::new();
                let mut i = 3;
                while i + 1 < cmds.len() {
                    entries.push((cmds[i].clone(), cmds[i + 1].clone()));
                    i += 2;
                }
                let id_str = std::str::from_utf8(&cmds[2]).map_err(|_| "Invalid ID encoding")?;
                if id_str.is_empty() {
                    return Err("Invalid ID".to_string());
                }
                if id_str == "*" {
                    return Ok(RedisCommand::XAdd {
                        key,
                        fields: entries,
                        id: StreamEntryIdCommandType::GenerateTimeAndSequence,
                    });
                }

                let (time_str, seq_str) = id_str.split_once('-').ok_or("Invalid ID format")?;
                let time = time_str
                    .parse::<u64>()
                    .map_err(|_| "Invalid ID timestamp")?;

                match seq_str {
                    "*" => {
                        return Ok(RedisCommand::XAdd {
                            key,
                            fields: entries,
                            id: StreamEntryIdCommandType::GenerateOnlySequence { time },
                        });
                    }
                    _ => {
                        let sequence = seq_str.parse::<u64>().map_err(|_| "Invalid ID sequence")?;
                        return Ok(RedisCommand::XAdd {
                            key,
                            fields: entries,
                            id: StreamEntryIdCommandType::Explicit { time, sequence },
                        });
                    }
                }
            }
            "set" => {
                if cmds.len() < 3 {
                    return Err("SET requires a key and a value".to_string());
                }

                let mut expiry = None;

                let mut i = 3;
                while i < cmds.len() {
                    let arg = std::str::from_utf8(&cmds[i])
                        .map(|s| s.to_lowercase())
                        .unwrap_or_default();

                    match arg.as_str() {
                        "px" => {
                            if i + 1 < cmds.len() {
                                expiry = std::str::from_utf8(&cmds[i + 1])
                                    .ok()
                                    .and_then(|s| s.parse::<u64>().ok());
                                i += 2;
                            } else {
                                return Err("SET PX requires a timeout value".to_string());
                            }
                        }
                        "ex" => {
                            if i + 1 < cmds.len() {
                                expiry = std::str::from_utf8(&cmds[i + 1])
                                    .ok()
                                    .and_then(|s| s.parse::<u64>().ok())
                                    .map(|secs| secs * 1000);
                                i += 2;
                            } else {
                                return Err("SET EX requires a timeout value".to_string());
                            }
                        }
                        _ => i += 1,
                    }
                }

                Ok(RedisCommand::Set {
                    key: cmds[1].clone(),
                    value: cmds[2].clone(),
                    expiry,
                })
            }

            "get" => {
                if cmds.len() < 2 {
                    return Err("GET requires a key".to_string());
                }
                Ok(RedisCommand::Get(cmds[1].clone()))
            }

            "rpush" => {
                if cmds.len() < 3 {
                    return Err("RPUSH requires key and at least one element".to_string());
                }
                Ok(RedisCommand::RPush {
                    key: cmds[1].clone(),
                    elements: cmds[2..].to_vec(),
                })
            }
            "ping" => Ok(RedisCommand::Ping),
            "echo" => {
                if cmds.len() < 2 {
                    return Err("ECHO requires an argument".to_string());
                }
                Ok(RedisCommand::Echo(cmds[1].clone()))
            }
            "type" => {
                if cmds.len() < 2 {
                    return Err("TYPE requires a key".to_string());
                }
                Ok(RedisCommand::Type(cmds[1].clone()))
            }
            "lpush" => {
                if cmds.len() < 3 {
                    return Err("LPUSH requires key and at least one element".to_string());
                }
                Ok(RedisCommand::LPush {
                    key: cmds[1].clone(),
                    elements: cmds[2..].to_vec(),
                })
            }

            "lrange" => {
                if cmds.len() < 4 {
                    return Err("LRANGE requires key, start, and stop".to_string());
                }
                let start = std::str::from_utf8(&cmds[2])
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);
                let stop = std::str::from_utf8(&cmds[3])
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);
                Ok(RedisCommand::LRange {
                    key: cmds[1].clone(),
                    start,
                    stop,
                })
            }

            "llen" => cmds
                .get(1)
                .map(|arg| RedisCommand::LLen(arg.clone()))
                .ok_or_else(|| "LLEN requires a key".to_string()),

            "lpop" => {
                if cmds.len() < 2 {
                    return Err("LPOP requires a key".to_string());
                }
                let count = cmds
                    .get(2)
                    .and_then(|b| std::str::from_utf8(b).ok())
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(1);
                Ok(RedisCommand::LPop {
                    key: cmds[1].clone(),
                    count,
                })
            }

            "blpop" => {
                if cmds.len() < 3 {
                    return Err("BLPOP requires at least one key and a timeout".to_string());
                }
                let timeout = std::str::from_utf8(cmds.last().unwrap())
                    .map_err(|_| "Invalid timeout encoding")?
                    .parse()
                    .map_err(|_| "Invalid timeout value")?;
                let keys = cmds[1..cmds.len() - 1].to_vec();
                Ok(RedisCommand::BLPop { keys, timeout })
            }

            _ => Err(format!("Unknown command: {}", command_name)),
        }
    }

    fn parse_xrange(key: Vec<u8>, cmds: &[Vec<u8>])->Result<RedisCommand, String> {
        let start_str = std::str::from_utf8(&cmds[2]).map_err(|_| "Invalid ID encoding")?;
        let end_str = std::str::from_utf8(&cmds[3]).map_err(|_| "Invalid ID encoding")?;

        let (start_time, start_seq) =
            if start_str == "-" || start_str == "+"{
                match start_str {
                    "-" => (0, 0),
                    "+" => (u64::MAX, u64::MAX),
                    _ => panic!("idk how this would happen")

                }
            } else if  let Some((t, s)) = start_str.split_once('-') {
                (t.parse::<u64>().map_err(|_| "Err")?, s.parse::<u64>().map_err(|_| "Err")?)
            } else {
                (start_str.parse::<u64>().map_err(|_| "Err")?, 0)
            };

        let (end_time, end_seq) = if let Some((t, s)) = end_str.split_once('-') {
            (t.parse::<u64>().map_err(|_| "Err")?, s.parse::<u64>().map_err(|_| "Err")?)
        } else {
            (end_str.parse::<u64>().map_err(|_| "Err")?, u64::MAX)
        };

        Ok(XRange { key, start_time, end_time, start_sequence: start_seq, end_sequence: end_seq })
    }
}
