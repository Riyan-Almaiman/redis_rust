use std::f64;

use uuid::Uuid;

#[derive(Clone, Debug)]
pub enum StreamEntryIdCommandType {
    Explicit { time: u64, sequence: u64 },
    GenerateTimeAndSequence,

    GenerateOnlySequence { time: u64 },
}
#[derive(Debug, Clone)]

pub struct StreamRead {
    pub key: Vec<u8>,
    pub id: String,
}
#[derive(Debug, Clone)]
pub enum RedisCommand {
    Ping,
    Incr {
        key: Vec<u8>,
    },
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
    },
    XRead {
        streams: Vec<StreamRead>,
        timeout: Option<f64>,
    },
    Multi,
    Error(String),
    Exec,
    Discard,
    Info {
        section: Option<String>,
    },
    REPLCONF(Vec<String>),
    PSYNC {
        replication_id: String,
        replication_offset: String,
    },
    Wait {
        replicas_num: u64,
        timeout: u64,
    },
    Config(Vec<String>),

    Keys(String),
    Subscribe(String),
    Publish {
        channel: String,
        message: String,
    },
    Unsubscribe(String),
    Zadd {
        key: String,
        values: Vec<(f64, String)>,
    },   Zrank {
        key: String,
        values:  String,
    },
    Zrange {
        key: String,
        start: usize,
        end: usize,
    },
    
}
impl RedisCommand {
    pub fn from_parts(command: &str, args: &[&str]) -> Result<Self, String> {
        match command.to_lowercase().as_str() {
                "zrange" => {
                if args.len() < 3 {
                    return Err("zrange requires key, start, end".into());
                }
            
                let start = args[1].parse::<usize>().map_err(|_| "Invalid start")?;
                let end = args[2].parse::<usize>().map_err(|_| "Invalid end")?;
                Ok(RedisCommand::Zrange {
                    key: args[0].to_string(),
                    start,
                    end,
                })
            }
                "zrank" => {
                // let mut values = Vec::new();

                if args.len() < 2 {
                    return Err("invalid params".into());
                }


                // for arg in & args[1..] {
                //     values.push(arg.to_string());
                // }
                Ok(RedisCommand::Zrank {
                    key: args[0].to_string(),
                        values: args[1].to_string(),
                })
            }
            "zadd" => {
                let mut values = Vec::new();

                if args.len() < 3 {
                    return Err("invalid params".into());
                }

                if args[1..].len() % 2 != 0 {
                    return Err("invalid params".into());
                }

                for chunk in args[1..].chunks(2) {
                    let f: f64 = chunk[0].parse::<f64>().map_err(|_| "invalid value")?;
                    values.push((f, chunk[1].to_string()));
                }
                Ok(RedisCommand::Zadd {
                    key: args[0].to_string(),
                    values,
                })
            }
            "unsubscribe" => {
                if args.len() < 1 {
                    return Err("no params".into());
                }
                Ok(RedisCommand::Unsubscribe(args[0].to_owned()))
            }
            "keys" => {
                if args.len() < 1 {
                    return Err("no params".into());
                }
                Ok(RedisCommand::Keys(args[0].to_owned()))
            }
            "publish" => {
                if args.len() < 2 {
                    return Err("no params".into());
                }
                Ok(RedisCommand::Publish {
                    channel: args[0].to_owned(),
                    message: args[1].to_owned(),
                })
            }
            "subscribe" => {
                if args.len() < 1 {
                    return Err("no params".into());
                }
                Ok(RedisCommand::Subscribe(args[0].to_owned()))
            }
            "wait" => {
                if args.len() < 2 {
                    return Err("WAIT requires numreplicas and timeout".into());
                }
                Ok(RedisCommand::Wait {
                    replicas_num: args[0].parse::<u64>().map_err(|_| "Invalid numreplicas")?,
                    timeout: args[1].parse::<u64>().map_err(|_| "Invalid timeout")?,
                })
            }
            "config" => {
                if args.len() < 1 {
                    return Err("no params".into());
                }
                Ok(RedisCommand::Config(
                    args.iter().map(|s| s.to_string()).collect(),
                ))
            }
            "psync" => Ok(RedisCommand::PSYNC {
                replication_id: "?".to_string(),
                replication_offset: "-1".to_string(),
            }),
            "replconf" => Ok(RedisCommand::REPLCONF(
                args.to_vec().iter().map(|s| s.to_string()).collect(),
            )),
            "info" => {
                if args.len() >= 1 {
                    Ok(RedisCommand::Info {
                        section: Some(args[0].to_string()),
                    })
                } else {
                    Ok(RedisCommand::Info { section: None })
                }
            }
            "exec" => Ok(RedisCommand::Exec),
            "discard" => Ok(RedisCommand::Discard),

            "ping" => Ok(RedisCommand::Ping),
            "multi" => Ok(RedisCommand::Multi),

            "incr" => {
                if args.len() < 1 {
                    return Err("INCR requires an argument".into());
                }
                Ok(RedisCommand::Incr {
                    key: args[0].as_bytes().to_vec(),
                })
            }
            // ---------------- ECHO ----------------
            "echo" => {
                if args.len() < 1 {
                    return Err("ECHO requires an argument".into());
                }
                Ok(RedisCommand::Echo(args[0].as_bytes().to_vec()))
            }

            // ---------------- SET ----------------
            "set" => {
                if args.len() < 2 {
                    return Err("SET requires a key and a value".into());
                }

                let mut expiry = None;
                let mut i = 2;

                while i < args.len() {
                    match args[i].to_lowercase().as_str() {
                        "px" => {
                            if i + 1 >= args.len() {
                                return Err("SET PX requires timeout".into());
                            }
                            expiry = Some(args[i + 1].parse::<u64>().map_err(|_| "Invalid PX")?);
                            i += 2;
                        }
                        "ex" => {
                            if i + 1 >= args.len() {
                                return Err("SET EX requires timeout".into());
                            }
                            let secs = args[i + 1].parse::<u64>().map_err(|_| "Invalid EX")?;
                            expiry = Some(secs * 1000);
                            i += 2;
                        }
                        _ => i += 1,
                    }
                }

                Ok(RedisCommand::Set {
                    key: args[0].as_bytes().to_vec(),
                    value: args[1].as_bytes().to_vec(),
                    expiry,
                })
            }

            // ---------------- GET ----------------
            "get" => {
                if args.len() < 1 {
                    return Err("GET requires a key".into());
                }
                Ok(RedisCommand::Get(args[0].as_bytes().to_vec()))
            }

            // ---------------- TYPE ----------------
            "type" => {
                if args.len() < 1 {
                    return Err("TYPE requires a key".into());
                }
                Ok(RedisCommand::Type(args[0].as_bytes().to_vec()))
            }

            // ---------------- RPUSH ----------------
            "rpush" => {
                if args.len() < 2 {
                    return Err("RPUSH requires key and at least one element".into());
                }

                Ok(RedisCommand::RPush {
                    key: args[0].as_bytes().to_vec(),
                    elements: args[1..].iter().map(|s| s.as_bytes().to_vec()).collect(),
                })
            }

            // ---------------- LPUSH ----------------
            "lpush" => {
                if args.len() < 2 {
                    return Err("LPUSH requires key and at least one element".into());
                }

                Ok(RedisCommand::LPush {
                    key: args[0].as_bytes().to_vec(),
                    elements: args[1..].iter().map(|s| s.as_bytes().to_vec()).collect(),
                })
            }

            // ---------------- LRANGE ----------------
            "lrange" => {
                if args.len() < 3 {
                    return Err("LRANGE requires key, start, stop".into());
                }

                let start = args[1].parse::<i64>().map_err(|_| "Invalid start")?;
                let stop = args[2].parse::<i64>().map_err(|_| "Invalid stop")?;

                Ok(RedisCommand::LRange {
                    key: args[0].as_bytes().to_vec(),
                    start,
                    stop,
                })
            }

            // ---------------- LLEN ----------------
            "llen" => {
                if args.len() < 1 {
                    return Err("LLEN requires a key".into());
                }

                Ok(RedisCommand::LLen(args[0].as_bytes().to_vec()))
            }

            // ---------------- LPOP ----------------
            "lpop" => {
                if args.len() < 1 {
                    return Err("LPOP requires a key".into());
                }

                let count = if args.len() >= 2 {
                    args[1].parse::<usize>().map_err(|_| "Invalid count")?
                } else {
                    1
                };

                Ok(RedisCommand::LPop {
                    key: args[0].as_bytes().to_vec(),
                    count,
                })
            }

            // ---------------- BLPOP ----------------
            "blpop" => {
                if args.len() < 2 {
                    return Err("BLPOP requires keys and timeout".into());
                }

                let timeout = args
                    .last()
                    .unwrap()
                    .parse::<f64>()
                    .map_err(|_| "Invalid timeout")?;

                let keys = args[..args.len() - 1]
                    .iter()
                    .map(|s| s.as_bytes().to_vec())
                    .collect();

                Ok(RedisCommand::BLPop { keys, timeout })
            }

            // ---------------- XADD ----------------
            "xadd" => {
                if args.len() < 3 || (args.len() - 2) % 2 != 0 {
                    return Err("XADD requires key, ID, and field-value pairs".into());
                }

                let key = args[0].as_bytes().to_vec();
                let id_str = args[1];

                let mut fields = Vec::new();
                let mut i = 2;

                while i + 1 < args.len() {
                    fields.push((args[i].as_bytes().to_vec(), args[i + 1].as_bytes().to_vec()));
                    i += 2;
                }

                if id_str == "*" {
                    return Ok(RedisCommand::XAdd {
                        key,
                        fields,
                        id: StreamEntryIdCommandType::GenerateTimeAndSequence,
                    });
                }

                let (time_str, seq_str) = id_str.split_once('-').ok_or("Invalid ID format")?;

                let time = time_str
                    .parse::<u64>()
                    .map_err(|_| "Invalid ID timestamp")?;

                match seq_str {
                    "*" => Ok(RedisCommand::XAdd {
                        key,
                        fields,
                        id: StreamEntryIdCommandType::GenerateOnlySequence { time },
                    }),
                    _ => {
                        let sequence = seq_str.parse::<u64>().map_err(|_| "Invalid ID sequence")?;

                        Ok(RedisCommand::XAdd {
                            key,
                            fields,
                            id: StreamEntryIdCommandType::Explicit { time, sequence },
                        })
                    }
                }
            }

            // ---------------- XRANGE ----------------
            "xrange" => {
                if args.len() < 3 {
                    return Err("XRANGE requires key start end".into());
                }

                Self::parse_xrange(args[0].as_bytes().to_vec(), args[1], args[2])
            }

            // ---------------- XREAD ----------------
            "xread" => {
                let streams_pos = args
                    .iter()
                    .position(|c| c.eq_ignore_ascii_case("streams"))
                    .ok_or("Missing STREAMS keyword")?;

                let block_pos = args.iter().position(|c| c.eq_ignore_ascii_case("block"));

                let timeout = if let Some(pos) = block_pos {
                    Some(
                        args[pos + 1]
                            .parse::<f64>()
                            .map_err(|_| "Invalid timeout")?,
                    )
                } else {
                    None
                };

                let stream_args = &args[streams_pos + 1..];

                if stream_args.len() % 2 != 0 {
                    return Err("Invalid stream/id pairs".into());
                }

                let half = stream_args.len() / 2;

                let mut streams = Vec::new();

                for i in 0..half {
                    streams.push(StreamRead {
                        key: stream_args[i].as_bytes().to_vec(),
                        id: stream_args[i + half].to_string(),
                    });
                }

                Ok(RedisCommand::XRead { streams, timeout })
            }

            // ---------------- UNKNOWN ----------------
            _ => Err(format!("Unknown command: {}", command)),
        }
    }

    fn parse_xrange(key: Vec<u8>, start_str: &str, end_str: &str) -> Result<RedisCommand, String> {
        fn parse_id(id: &str, is_start: bool) -> Result<(u64, u64), String> {
            if id == "-" {
                return Ok((0, 0));
            }
            if id == "+" {
                return Ok((u64::MAX, u64::MAX));
            }

            if let Some((t, s)) = id.split_once('-') {
                Ok((
                    t.parse::<u64>().map_err(|_| "Invalid time")?,
                    s.parse::<u64>().map_err(|_| "Invalid sequence")?,
                ))
            } else {
                let time = id.parse::<u64>().map_err(|_| "Invalid ID")?;
                if is_start {
                    Ok((time, 0))
                } else {
                    Ok((time, u64::MAX))
                }
            }
        }

        let (start_time, start_seq) = parse_id(start_str, true)?;
        let (end_time, end_seq) = parse_id(end_str, false)?;

        Ok(RedisCommand::XRange {
            key,
            start_time,
            end_time,
            start_sequence: start_seq,
            end_sequence: end_seq,
        })
    }
    pub fn name(&self) -> &str {
        match self {
            RedisCommand::Zrange { key, start, end } => "zrange",
            RedisCommand::Zrank { key: _, values: _ } => "zrank",
            RedisCommand::Zadd { key: _, values: _ }=>"zadd",
            RedisCommand::Zrange { key: _, start: _, end: _ } => "zrange",
            RedisCommand::Unsubscribe(_) => "unsubscribe",
            RedisCommand::Publish { .. } => "publish",
            RedisCommand::Ping => "ping",
            RedisCommand::Incr { .. } => "incr",
            RedisCommand::Echo(_) => "echo",
            RedisCommand::Set { .. } => "set",
            RedisCommand::Get(_) => "get",
            RedisCommand::RPush { .. } => "rpush",
            RedisCommand::LPush { .. } => "lpush",
            RedisCommand::InternalTimeoutCleanup { .. } => "internal_timeout_cleanup",
            RedisCommand::LRange { .. } => "lrange",
            RedisCommand::LLen(_) => "llen",
            RedisCommand::LPop { .. } => "lpop",
            RedisCommand::BLPop { .. } => "blpop",
            RedisCommand::Type(_) => "type",
            RedisCommand::XAdd { .. } => "xadd",
            RedisCommand::XRange { .. } => "xrange",
            RedisCommand::XRead { .. } => "xread",
            RedisCommand::Multi => "multi",
            RedisCommand::Error(_) => "error",
            RedisCommand::Exec => "exec",
            RedisCommand::Discard => "discard",
            RedisCommand::Info { .. } => "info",
            RedisCommand::REPLCONF(_) => "replconf",
            RedisCommand::PSYNC { .. } => "psync",
            RedisCommand::Wait { .. } => "wait",
            RedisCommand::Config(_) => "config",
            RedisCommand::Keys(_) => "keys",
            RedisCommand::Subscribe(_) => "subscribe",
        }
    }
}
