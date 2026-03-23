use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use uuid::Uuid;


use crate::commands;
use crate::commands_parser::RedisCommand;
use crate::db::DB;
use crate::parser::Parser;
use crate::resp::Resp;

impl DB {
    pub fn load_rdb(&mut self, path: &str) {
        let tmp_path = Path::new("/tmp/rdb_output.resp");
        let Ok(file) = File::open(path) else { return };
        let reader = BufReader::new(file);
        let Ok(_) = rdb::parse(
            reader,
            rdb::formatter::Protocol::new(Some(PathBuf::from(tmp_path))),
            rdb::filter::Simple::new(),
        ) else {
            return;
        };
        let Ok(resp_bytes) = std::fs::read(tmp_path) else {
            return;
        };

        let mut parser = Parser::new();
        parser.read_buffer.extend_from_slice(&resp_bytes);
        println!("{:?}", resp_bytes);
        while let Some(mut resp) = parser.parse() {
            if let Resp::Array(ref mut arr) = resp {
                let cmd_name = arr.pop_front();
                if let Some(cmd) = cmd_name {
                    let cmd_name_bytes = Resp::get_bytes(&cmd).unwrap();
                    let args: Vec<&str> = arr
                        .iter()
                        .map(|item| {
                            let bytes: &[u8] = Resp::get_bytes(item).unwrap();
                            std::str::from_utf8(bytes).expect("Invalid UTF-8")
                        })
                        .collect();
                    let cmd_name = std::str::from_utf8(cmd_name_bytes).unwrap().to_lowercase();

                    if cmd_name == "select" {
                        continue;
                    }

                    if cmd_name == "pexpireat" {
                        let key = args.first().map(|value| value.as_bytes().to_vec());
                        let ts = args.get(1).and_then(|value| value.parse::<u64>().ok());
                        if let (Some(key), Some(ts)) = (key, ts) {
                            let expiry = SystemTime::UNIX_EPOCH + Duration::from_millis(ts);
                            if expiry > SystemTime::now() {
                                if let Some(kv) = self.database.get_mut(&key) {
                                    kv.expiry = Some(expiry);
                                }
                            } else {
                                self.database.remove(&key);
                            }
                        }
                        continue;
                    }

                    if let Ok(command) = RedisCommand::from_parts(&cmd_name, &args) {
                        commands::route(self, command, Uuid::new_v4());
                    }
                }
            }
        }
    }
}