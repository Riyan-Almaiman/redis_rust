use crate::blocking_stream::StreamWait;
use crate::commands::list_commands::ListCommands;
use crate::commands::server_commands::ServerCommands;
use crate::commands::stream_commands::StreamCommands;
use crate::commands::string_commands::StringCommands;
use crate::commands_parser::RedisCommand;
use crate::resp::Resp;
use uuid::Uuid;

pub enum CommandResult {
    Response(Resp),
    Wait {
        timeout: u64,
        replicas: u64,
        offset: u64
    },
    BlockList {
        keys: Vec<Vec<u8>>,
        timeout: f64,
    },
    RegisterSlave (Resp),
    BlockStream {
        client_id: Uuid,
        streams: Vec<StreamWait>,
        timeout_ms: f64,
    },

    Exec(Vec<RedisCommand>),

    None,
}

use crate::db::DB;

pub fn route(db: &mut DB, cmd: RedisCommand, client_id: Uuid) -> CommandResult {
    match cmd {
        RedisCommand::Wait {timeout, replicas_num} => ServerCommands::wait(db, timeout, replicas_num),
        RedisCommand::Ping => ServerCommands::ping(),
        RedisCommand::Echo(data) => ServerCommands::echo(data),
        RedisCommand::Get(key) => StringCommands::get(db, key),
        RedisCommand::Set { key, value, expiry } => StringCommands::set(db, key, value, expiry),
        RedisCommand::Incr { key } => StringCommands::incr(db, key),
        RedisCommand::LPush { key, elements } => ListCommands::lpush(db, key, elements),
        RedisCommand::RPush { key, elements } => ListCommands::rpush(db, key, elements),
        RedisCommand::LLen(key) => ListCommands::llen(db, key),
        RedisCommand::LPop { key, count } => ListCommands::lpop(db, key, count),
        RedisCommand::LRange { key, start, stop } => ListCommands::lrange(db, key, start, stop),
        RedisCommand::BLPop { keys, timeout } => ListCommands::blpop(db, keys, timeout),
        RedisCommand::Type(key) => ServerCommands::data_type(db, key),
        RedisCommand::Info { section } => ServerCommands::info(db, section),
        RedisCommand::REPLCONF(args) => ServerCommands::replconf(args, db),
        RedisCommand::PSYNC { .. } => ServerCommands::psync(db),
        RedisCommand::Multi => ServerCommands::multi(db, client_id),
        RedisCommand::Discard => ServerCommands::discard(db, client_id),
        RedisCommand::Exec => ServerCommands::exec(db, client_id),
        RedisCommand::InternalTimeoutCleanup { client_id } => {
            ServerCommands::cleanup_timeout(db, client_id)
        }
        RedisCommand::XRead { streams, timeout } => {
            StreamCommands::xread(db, streams, timeout, client_id)
        }
        RedisCommand::XRange {
            key,
            start_time,
            end_time,
            start_sequence,
            end_sequence,
        } => StreamCommands::xrange(db, key, start_time, end_time, start_sequence, end_sequence),
        RedisCommand::XAdd { key, fields, id } => StreamCommands::xadd(db, key, fields, id),
        RedisCommand::Error(err) => CommandResult::Response(Resp::Error(err.into_bytes())),
    }
}
