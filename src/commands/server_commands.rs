use std::collections::VecDeque;
use crate::command_router::CommandResult;
use crate::db::DB;
use crate::resp::Resp;
use crate::role::Role;
use crate::send::send_cmd;
use crate::valuetype::ValueType;
use uuid::Uuid;
use crate::resp::Resp::BulkString;

pub struct ServerCommands;

impl ServerCommands {
    pub fn ping() -> CommandResult {
        CommandResult::Response(Resp::SimpleString(b"PONG".to_vec()))
    }

    pub fn echo(data: Vec<u8>) -> CommandResult {
        CommandResult::Response(Resp::BulkString(data))
    }
    pub fn subscribe(db: &mut DB, channel: String, client_id: Uuid) -> CommandResult {
        let subscriber = db.subscribers.entry(client_id).or_insert_with(|| Vec::new());
        subscriber.push(channel.clone());
        let mut resp = VecDeque::new();
        resp.push_back(BulkString("subscribe".as_bytes().to_vec()));
        resp.push_back(BulkString(channel.as_bytes().to_vec()));
        resp.push_back(BulkString(subscriber.len().to_string().as_bytes().to_vec()));

        CommandResult::Response(Resp::Array(resp))
    }
    pub fn info(db: &DB, section: Option<String>) -> CommandResult {
        let mut sections = Vec::new();

        if let Some(section) = section {
            match section.as_str() {
                "replication" => sections.push(db.role.get_replication()),
                "role" => sections.push(db.role.get_role()),
                _ => {
                    sections.push(db.role.get_replication());
                    sections.push(db.role.get_role());
                }
            }
        }

        CommandResult::Response(Resp::BulkString(sections.join("").into_bytes()))
    }
    pub fn config(db: &DB, args: Vec<String>) -> CommandResult {
        let mut res = Vec::new();

        match args[0].to_lowercase().as_str() {
            "get" => {
                    for arg in args[1..].iter() {
                        match arg.to_lowercase().as_str() {
                            "dir" => {
                                res.push(Resp::BulkString(arg.as_bytes().to_vec()));
                                res.push(Resp::BulkString(db.dir.as_bytes().to_vec()));


                            },
                            "dbfilename" => {
                                res.push(Resp::BulkString(arg.as_bytes().to_vec()));
                                res.push(Resp::BulkString(db.file_name.as_bytes().to_vec()));
                            }
                            _ => ()
                        }
                    }
            }
            _ => (),
        }
        CommandResult::Response(Resp::Array(VecDeque::from(res)))
    }
    pub fn replconf(args: Vec<String>, db: &mut DB, client_id: Uuid) -> CommandResult {
        match &db.role {
            Role::Master {
                replication_id,
                replication_offset,
            } => {
                if args.len() >= 2
                    && args[0].to_lowercase() == "ack"
                    && args[1].parse::<u64>().is_ok()
                {
                    let slave_offset: u64 = args[1].parse().unwrap_or(0);
                    db.ack_waiters
                        .retain(|tx| tx.send((client_id, slave_offset)).is_ok());
                    return CommandResult::None;
                } else {
                    return CommandResult::Response(Resp::SimpleString(b"OK".to_vec()));
                }
            }
            Role::Slave { .. } => {
                panic!("")
            }
        }
    }

    pub fn psync(db: &DB) -> CommandResult {
        CommandResult::RegisterSlave(Resp::SimpleString(
            format!(
                "FULLRESYNC {} {}",
                db.role.get_repl_id(),
                db.role.get_repl_offset()
            )
            .into_bytes(),
        ))
    }

    pub fn multi(db: &mut DB, client_id: uuid::Uuid) -> CommandResult {
        db.multi_list.insert(client_id, vec![]);
        CommandResult::Response(Resp::SimpleString(b"OK".to_vec()))
    }

    pub fn discard(db: &mut DB, client_id: uuid::Uuid) -> CommandResult {
        if db.multi_list.remove(&client_id).is_some() {
            CommandResult::Response(Resp::SimpleString(b"OK".to_vec()))
        } else {
            CommandResult::Response(Resp::Error(b"ERR DISCARD without MULTI".to_vec()))
        }
    }

    pub fn exec(db: &mut DB, client_id: uuid::Uuid) -> CommandResult {
        if let Some(client) = db.multi_list.remove(&client_id) {
            CommandResult::Exec(client)
        } else {
            CommandResult::Response(Resp::Error(b"ERR EXEC without MULTI".to_vec()))
        }
    }

    pub fn data_type(db: &DB, key: Vec<u8>) -> CommandResult {
        let type_str = match db.database.get(&key) {
            None => b"none".as_slice(),
            Some(kv) => match kv.value {
                ValueType::String(_) | ValueType::List(_) | ValueType::Stream(_) => {
                    kv.value.get_value_type()
                }
            },
        };

        CommandResult::Response(Resp::SimpleString(type_str.to_vec()))
    }
    pub fn wait(db: &mut DB, timeout: u64, replicas: u64) -> CommandResult {
        CommandResult::Wait {
            timeout,
            replicas,
            offset: db.role.get_repl_offset().parse().unwrap(),
        }
    }
    pub fn cleanup_timeout(db: &mut DB, target_id: uuid::Uuid) -> CommandResult {
        if let Some(blocked_client) = db.blocking.lists.blocked_list.remove(&target_id) {
            for queue in db.blocking.lists.waiters.values_mut() {
                queue.retain(|id| *id != target_id);
            }
            send_cmd(blocked_client.response_tx, Resp::NullArray);
        }

        if let Some(stream_client) = db.blocking.streams.clients.remove(&target_id) {
            for queue in db.blocking.streams.waiters.values_mut() {
                queue.retain(|id| *id != target_id);
            }
            send_cmd(stream_client.response_tx, Resp::NullArray);
        }

        CommandResult::None
    }
}
