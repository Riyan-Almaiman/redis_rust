use crate::command_router::CommandResult;
use crate::db::DB;
use crate::resp::Resp;
use crate::valuetype::ValueType;

pub struct ServerCommands;

impl ServerCommands {
    pub fn ping() -> CommandResult {
        CommandResult::Response(Resp::SimpleString(b"PONG".to_vec()))
    }

    pub fn echo(data: Vec<u8>) -> CommandResult {
        CommandResult::Response(Resp::BulkString(data))
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

    pub fn replconf() -> CommandResult {
        CommandResult::Response(Resp::SimpleString(b"OK".to_vec()))
    }

    pub fn psync(db: &DB) -> CommandResult {
        CommandResult::Response(Resp::SimpleString(
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

    pub fn cleanup_timeout(db: &mut DB, target_id: uuid::Uuid) -> CommandResult {
        if let Some(blocked_client) = db.blocked_list.blocked_list.remove(&target_id) {
            for queue in db.blocked_list.waiters.values_mut() {
                queue.retain(|id| *id != target_id);
            }
            let _ = blocked_client.response_tx.send(Resp::NullArray);
        }

        if let Some(stream_client) = db.blocking_streams.clients.remove(&target_id) {
            for queue in db.blocking_streams.waiters.values_mut() {
                queue.retain(|id| *id != target_id);
            }
            let _ = stream_client.response_tx.send(Resp::NullArray);
        }

        CommandResult::None
    }
}
