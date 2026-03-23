use crate::command_router::CommandResult;
use crate::db::DB;
use crate::resp::Resp;
use crate::user;

pub struct AuthCommands;

impl AuthCommands {
    pub fn acl(db: &mut DB, subcommand: String, arguments: Vec<String>) -> CommandResult {
        match subcommand.to_ascii_lowercase().as_str() {
            "whoami" => CommandResult::Response(Resp::BulkString(b"default".to_vec())),
            "getuser"  => {
                let username = arguments.get(0);
                if username.is_none() {
                    return CommandResult::Response(Resp::Error(b"ERR wrong number of arguments for 'getuser' command".to_vec()));
                }
                let username = username.unwrap();
                let user = db.users.get(username);
          
                if let Some(user) = user {

                    let flags = Resp::from_strings(user.flags.iter().map(|(_, flag)| flag.to_str().to_string()).collect());
                    let flag = Resp::BulkString("flags".as_bytes().to_vec());
                    let response = Resp::Array(vec![flag, flags].into());
                    CommandResult::Response(response)
                } else {
                    CommandResult::Response(Resp::Error(b"ERR no such user".to_vec()))
                }
            },
            _ => CommandResult::Response(Resp::Error(b"ERR unknown ACL subcommand".to_vec())),
        }

    }
}