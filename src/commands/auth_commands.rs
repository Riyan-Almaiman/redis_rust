use crate::command_router::CommandResult;
use crate::db::DB;
use crate::resp::Resp;
use crate::user;

pub struct AuthCommands;

impl AuthCommands {
    pub fn acl(db: &mut DB, subcommand: String, _arguments: Vec<String>) -> CommandResult {
        match subcommand.to_ascii_lowercase().as_str() {
            "whoami" => CommandResult::Response(Resp::BulkString(b"default".to_vec())),
            "getuser"  => {
                let user = db.users.get("default");
                if let Some(user) = user {
            
                    CommandResult::Response(Resp::from_strings(user.flags.iter().cloned().collect()))
                } else {
                    CommandResult::Response(Resp::Error(b"ERR no such user".to_vec()))
                }
            },
            _ => CommandResult::Response(Resp::Error(b"ERR unknown ACL subcommand".to_vec())),
        }

    }
}