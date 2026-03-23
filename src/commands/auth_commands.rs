use std::collections::VecDeque;
use std::vec;

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
                    let mut response = VecDeque::new();
                    let mut flags = Self::get_flags_for_user(user);
                    let mut passwords = Self::get_passwords_for_user(user);
                    response.append(&mut flags);
                    response.append(&mut passwords);
                    CommandResult::Response(Resp::Array(response))
                } else {
                    CommandResult::Response(Resp::Error(b"ERR no such user".to_vec()))
                }
            },
            _ => CommandResult::Response(Resp::Error(b"ERR unknown ACL subcommand".to_vec())),
        }

    }

    fn get_flags_for_user(user: &user::User) -> VecDeque<Resp> {
         let flag = Resp::BulkString("flags".as_bytes().to_vec());

       let flags = user.flags.iter().map(|flag| Resp::BulkString(flag.to_str().to_string().into_bytes())).collect();

        vec![flag, Resp::Array(flags)].into()
        
    }
    fn get_passwords_for_user(user: &user::User) -> VecDeque<Resp> {
        let pass = Resp::BulkString("passwords".as_bytes().to_vec());
        let mut passwords: VecDeque<Resp> = user.passwords.iter().map(|pw| Resp::BulkString(pw.as_bytes().to_vec())).collect();
    
        vec![pass, Resp::Array(passwords)].into()
    }
}