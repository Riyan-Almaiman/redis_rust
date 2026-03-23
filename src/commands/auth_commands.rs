use core::hash;
use std::collections::VecDeque;
use std::vec;

use sha2::Digest;

use crate::command_router::CommandResult;
use crate::db::DB;
use crate::resp::Resp;
use crate::user;

pub struct AuthCommands;

impl AuthCommands {
    pub fn acl(db: &mut DB, subcommand: String, arguments: Vec<String>) -> CommandResult {
        match subcommand.to_ascii_lowercase().as_str() {
            "whoami" => CommandResult::Response(Resp::BulkString(b"default".to_vec())),
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
                    let mut flags = Self::get_flags_for_user(user);
                    let mut passwords = Self::get_passwords_for_user(user);
                    response.append(&mut flags);
                    response.append(&mut passwords);
                    CommandResult::Response(Resp::Array(response))
                } else {
                    CommandResult::Response(Resp::Error(b"ERR no such user".to_vec()))
                }
            }
            "setuser" => {
                let username = arguments.get(0);
                if username.is_none() {
                    return CommandResult::Response(Resp::Error(
                        b"ERR wrong number of arguments for 'setuser' command".to_vec(),
                    ));
                }
                let username = username.unwrap();
                let mut user = db.users.entry(username.clone()).or_insert_with(|| user::User {
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
                        let hashed_pass = Self::hash_password(pass);
                        user.passwords.push(hashed_pass);
                    }
                                   user.flags.remove(&user::Flag::NoPass);

                }
                else {
                    return CommandResult::Response(Resp::Error(
                        b"ERR wrong number of arguments for 'setuser' command".to_vec(),
                    ))
                }

                CommandResult::Response(Resp::SimpleString(b"OK".to_vec()))
            }
            _ => CommandResult::Response(Resp::Error(b"ERR unknown ACL subcommand".to_vec())),
        }
    }

    fn get_flags_for_user(user: &user::User) -> VecDeque<Resp> {
        let flag = Resp::BulkString("flags".as_bytes().to_vec());

        let flags = user
            .flags
            .iter()
            .map(|flag| Resp::BulkString(flag.to_str().to_string().into_bytes()))
            .collect();

        vec![flag, Resp::Array(flags)].into()
    }
    fn get_passwords_for_user(user: &user::User) -> VecDeque<Resp> {
        let pass = Resp::BulkString("passwords".as_bytes().to_vec());
        let  passwords: VecDeque<Resp> = user
            .passwords
            .iter()
            .map(|pw| Resp::BulkString(pw.as_bytes().to_vec()))
            .collect();

        vec![pass, Resp::Array(passwords)].into()
    }
        pub fn hash_password(password: &str) -> String {
            let mut hasher = sha2::Sha256::new();
            hasher.update(password.as_bytes());
            let result = hasher.finalize();
            hex::encode(result)
        }

    pub fn validate_password(password: &str, stored_hash: &str) -> bool {
        Self::hash_password(password) == stored_hash
    }
}
