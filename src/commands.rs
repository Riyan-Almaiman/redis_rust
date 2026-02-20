use uuid::Uuid;

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
        id: Vec<u8>,
        entries: Vec<(Vec<u8>, Vec<u8>)>,
    },
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
            "xadd" => {
                let ascii_cmds: Vec<String> = cmds
                    .iter()
                    .map(|bytes| String::from_utf8_lossy(bytes).into_owned())
                    .collect();

                if cmds.len() < 5 || (cmds.len() - 3) % 2 != 0 {
                    return Err("XADD requires key, ID, and at least one field-value pair".to_string());
                }


                let key = cmds[1].clone();
                let id = cmds[2].clone();

                let mut entries = Vec::new();
                let mut i = 3;
                while i + 1 < cmds.len() {
                    entries.push((cmds[i].clone(), cmds[i+1].clone()));
                    i += 2;
                }

                Ok(RedisCommand::XAdd {
                    key,
                    id,
                    entries,
                })
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
}
