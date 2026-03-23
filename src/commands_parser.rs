use std::f64;

use uuid::Uuid;

mod auth_parser;
mod list_parser;
mod server_parser;
mod sorted_parser;
mod stream_parser;
mod string_parser;

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
        start: isize,
        end: isize,
    },
    Zcard(String),
    Zscore {
        key: String,
        value: String,
    },
    Zrem {
        key: String,
        value: String,
    },
    GeoAdd {
        key: String,
        longitude: f64,
        latitude: f64,
        member: String,
    },
    GeoPos {
        key: String,
        members: Vec<String>,
    },
    GeoDist {
        key: String,
        member1: String,
        member2: String,
    },
    GeoSearch {
        key: String,
        longitude: f64,
        latitude: f64,
        radius: f64,
    },
    Acl {
        subcommand: String,
        arguments: Vec<String>,}
    
}
impl RedisCommand {
    pub fn from_parts(command: &str, args: &[&str]) -> Result<Self, String> {
        let command = command.to_ascii_lowercase();

        match command.as_str() {
            "acl" => auth_parser::parse(command.as_str(), args),
            "unsubscribe" | "publish" | "subscribe" | "wait" | "config" | "psync"
            | "replconf" | "info" | "exec" | "discard" | "ping" | "multi" => {
                server_parser::parse(command.as_str(), args)
            }
            "keys" | "incr" | "echo" | "set" | "get" | "type" => {
                string_parser::parse(command.as_str(), args)
            }
            "rpush" | "lpush" | "lrange" | "llen" | "lpop" | "blpop" => {
                list_parser::parse(command.as_str(), args)
            }
            "xadd" | "xrange" | "xread" => stream_parser::parse(command.as_str(), args),
            "geosearch" | "geodist" | "geopos" | "geoadd" | "zrem" | "zscore"
            | "zcard" | "zrange" | "zrank" | "zadd" => {
                sorted_parser::parse(command.as_str(), args)
            }
            _ => Err(format!("Unknown command: {}", command)),
        }
    }

    pub fn name(&self) -> &str {
        match self {
            RedisCommand::Acl { .. } => "acl",
            RedisCommand::GeoSearch {..} => "geosearch",
            RedisCommand::GeoDist {.. } => "geodist",
            RedisCommand::GeoPos { .. } => "geopos",
            RedisCommand::GeoAdd { .. } => "geoadd",
            RedisCommand::Zrem { .. } => "zrem",
            RedisCommand::Zscore { .. } => "zscore",
            RedisCommand::Zcard(_) => "zcard",
            RedisCommand::Zrange { key: _, start: _, end: _ } => "zrange",
            RedisCommand::Zrank { key: _, values: _ } => "zrank",
            RedisCommand::Zadd { key: _, values: _ }=>"zadd",
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
