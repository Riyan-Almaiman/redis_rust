use uuid::Uuid;

#[derive(Debug)]
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
    InternalTimeoutCleanup { key: String, client_id: Uuid },
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




            "rpush" => {
                if cmds.len() < 3 {
                    return Err("RPUSH requires key and at least one element".to_string());
                }
                Ok(RedisCommand::RPush {
                    key: cmds[1].clone(),
                    elements: cmds[2..].to_vec(),
                })
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
