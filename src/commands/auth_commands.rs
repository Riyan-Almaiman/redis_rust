use core::hash;
use std::collections::VecDeque;
use std::vec;

use sha2::Digest;

use crate::commands::CommandResult;
use crate::db::DB;
use crate::resp::Resp;
use crate::user;

pub struct AuthCommands;

impl AuthCommands {
    pub fn acl(db: &mut DB, client_id: uuid::Uuid, subcommand: String, arguments: Vec<String>) -> CommandResult {
        match subcommand.to_ascii_lowercase().as_str() {
            "whoami" => {
                let username = db.authenticated_user(client_id);
                if let Some(username) = username {
                    CommandResult::Response(Resp::BulkString(username.as_bytes().to_vec()))
                } else {
                    CommandResult::Response(Resp::Error(b"NOAUTH Authentication required.".to_vec()))
                }
            }
            "getuser" => {
                let username = arguments.get(0);
                if username.is_none() {
                    return CommandResult::Response(Resp::Error(
                        b"ERR wrong number of arguments for 'getuser' command".to_vec(),
                    ));
                }
                let username = username.unwrap();
                let user = db.users.get(username);

                if let Some(user) = user {
                    let mut response = VecDeque::new();
                    let  flags = Resp::from_strings(user.get_flags_for_user());
                    let  flags_resp = Resp::BulkString("flags".as_bytes().to_vec());
                    let  passwords = Resp::from_strings((*user.passwords).to_vec());
                    let  password  = Resp::BulkString("passwords".as_bytes().to_vec());

                    response.append(&mut vec![flags_resp, flags].into());
                    response.append(&mut vec![password, passwords].into());
                    CommandResult::Response(Resp::Array(response))
                } else {
                    CommandResult::Response(Resp::Error(b"ERR no such user".to_vec()))
                }
            }
            "setuser" => {
                                let username = db.authenticated_user(client_id);
                if username.is_none() {
                    return CommandResult::Response(Resp::Error(b"NOAUTH Authentication required.".to_vec()));
                }

                let username = arguments.get(0);
                if username.is_none() {
                    return CommandResult::Response(Resp::Error(
                        b"ERR wrong number of arguments for 'setuser' command".to_vec(),
                    ));
                }
                let username = username.unwrap();
                let  user = db
                    .users
                    .entry(username.clone())
                    .or_insert_with(|| user::User {
                        name: username.clone(),
                        passwords: Vec::new(),
                        allowed_commands: std::collections::HashSet::new(),
                        flags: std::collections::HashSet::new(),
                    });

                let pass_arg = arguments.get(1);
                if let Some(pass_arg) = pass_arg {
                    if let Some((idx, first_char)) = pass_arg.char_indices().next() {
                        if first_char != '>' && first_char != '|' {
                            return CommandResult::Response(Resp::Error(
                                b"ERR invalid password format".to_vec(),
                            ));
                        }
                        let pass = &pass_arg[idx + first_char.len_utf8()..];
                        let hashed_pass = DB::hash_password(pass);
                        user.passwords.push(hashed_pass);
                    }
                    user.flags.remove(&user::Flag::NoPass);
                } else {
                    return CommandResult::Response(Resp::Error(
                        b"ERR wrong number of arguments for 'setuser' command".to_vec(),
                    ));
                }

                CommandResult::Response(Resp::SimpleString(b"OK".to_vec()))
            }
            _ => CommandResult::Response(Resp::Error(b"ERR unknown ACL subcommand".to_vec())),
        }
    }
    pub fn auth(db: &mut DB, client_id: uuid::Uuid, username: String, password: String) -> CommandResult {
        if db.authenticate_user(&username, &password, client_id) {
            CommandResult::Response(Resp::SimpleString(b"OK".to_vec()))
        } else {
            CommandResult::Response(Resp::Error(b"WRONGPASS invalid username-password pair or user is disabled.".to_vec()))
        }
    }


}
