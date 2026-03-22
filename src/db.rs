use crate::blocking_manger::BlockingManager;
use crate::command_router;
use crate::command_router::CommandResult;
use crate::commands_parser::RedisCommand;
use crate::resp::Resp;
use crate::role::Role;
use crate::send::send_cmd;
use crate::valuetype::ValueType;
use base64::Engine;
use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};
use tokio::sync::mpsc;
use uuid::Uuid;
use crate::parser::Parser;

pub struct Master {
    pub replication_id: String,
    pub replication_offset: u64,
}

pub struct DB {
    pub database: HashMap<Vec<u8>, KeyValue>,
    pub multi_list: HashMap<Uuid, Vec<RedisCommand>>,
    pub receiver: mpsc::Receiver<Client>,
    pub blocking: BlockingManager,
    pub sender: mpsc::Sender<Client>,
    pub role: Role,
    pub slaves: HashMap<Uuid, mpsc::UnboundedSender<Vec<u8>>>,
    pub ack_waiters: Vec<mpsc::UnboundedSender<(Uuid, u64)>>,
    pub dir: String,
    pub file_name: String,
    pub subscribers: HashMap<Uuid, Vec<String>>,


}
#[derive(Debug)]
pub struct Client {
    pub client_id: Uuid,
    pub timeout: Option<Duration>,
    pub response_tx: mpsc::UnboundedSender<Vec<u8>>,
    pub resp_command: Resp,
    pub command: RedisCommand,
}

pub struct KeyValue {
    pub expiry: Option<SystemTime>,
    pub(crate) value: ValueType,
}
impl KeyValue {
    pub fn new(expiry: Option<SystemTime>, value: ValueType) -> Self {
        KeyValue { expiry, value }
    }
}
impl DB {
    pub fn execute_commands(&mut self, command: RedisCommand, client_id: Uuid) -> CommandResult {
        command_router::route(self, command, client_id)
    }
    pub async fn new(role: Role, dir: String, file_name: String) -> Self {
        let (pipeline_tx, pipeline_rx) = tokio::sync::mpsc::channel::<Client>(1000);

        let mut db = Self {
            database: HashMap::new(),
            sender: pipeline_tx,
            receiver: pipeline_rx,
            file_name,
            dir,
            role,
            subscribers: HashMap::new(),
            ack_waiters: Vec::new(),
            slaves: HashMap::new(),
            multi_list: HashMap::new(),
            blocking: BlockingManager {
                lists: Default::default(),
                streams: Default::default(),
            },
        };

        db.load_rdb(&format!("{}/{}", db.dir, db.file_name));
        db
    }
    fn load_rdb(&mut self, path: &str) {
        let tmp_path = Path::new("/tmp/rdb_output.resp");
        let Ok(file) = File::open(path) else { return };
        let reader = BufReader::new(file);
        let Ok(_) = rdb::parse(reader, rdb::formatter::Protocol::new(Some(PathBuf::from(tmp_path))), rdb::filter::Simple::new()) else { return };
        let Ok(resp_bytes) = std::fs::read(tmp_path) else { return };

        let mut parser = Parser::new();
        parser.read_buffer.extend_from_slice(&resp_bytes);
        println!("{:?}", resp_bytes);
        while let Some(mut resp) = parser.parse() {
            if let Resp::Array(ref mut arr) = resp {
                let cmd_name = arr.pop_front();
                if let Some(cmd) = cmd_name {
                    let cmd_name_bytes = Resp::get_bytes(&cmd).unwrap();
                    let cmd_name = std::str::from_utf8(cmd_name_bytes).unwrap().to_lowercase();
                    let args: Vec<&str> = arr
                        .iter()
                        .map(|a| {
                            let bytes: &[u8] = Resp::get_bytes(a).unwrap();
                            std::str::from_utf8(bytes).expect("Invalid UTF-8")
                        })
                        .collect();
                    let cmd_name = std::str::from_utf8(cmd_name_bytes).unwrap().to_lowercase();

                    if cmd_name == "select" {
                        continue;
                    }

                    if cmd_name == "pexpireat" {
                        let key = args.get(0).map(|s| s.as_bytes().to_vec());
                        let ts = args.get(1).and_then(|s| s.parse::<u64>().ok());
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

                    if let Ok(command) = RedisCommand::from_parts(&*cmd_name, &args) {
                        command_router::route(self, command, Uuid::new_v4());
                    }
                }
            }
        }
    }
    fn is_write_command(cmd: &RedisCommand) -> bool {
        matches!(
            cmd,
            RedisCommand::Set { .. }
                | RedisCommand::Incr { .. }
                | RedisCommand::RPush { .. }
                | RedisCommand::LPush { .. }
                | RedisCommand::LPop { .. }
                | RedisCommand::XAdd { .. }
        )
    }

    fn send_slaves(&mut self, resp: &Vec<u8>) -> u64 {
        self.role.increment_offset(resp.len());
        self.slaves
            .retain(|_id, slave| slave.send(resp.clone()).is_ok());
        self.slaves.len() as u64
    }
    pub fn cleanup_dead_slaves(&mut self) -> u64 {
        self.slaves.retain(|_id, slave| !slave.is_closed());
        self.slaves.len() as u64
    }
    pub async fn start(&mut self) {
        while let Some(request) = self.receiver.recv().await {
            let Client {
                command,
                response_tx,
                client_id,
                resp_command,
                ..
            } = request;
            let mut cmd_buffer = Vec::new();
            resp_command.write_format(&mut cmd_buffer);

            if let Some(client) = self.multi_list.get_mut(&client_id) {
                match command {
                    RedisCommand::Exec => {
                        let cmds = self.multi_list.remove(&client_id).unwrap_or_default();

                        let mut responses = VecDeque::new();

                        for cmd in cmds {
                            if let CommandResult::Response(r) =
                                self.execute_commands(cmd, client_id)
                            {
                                responses.push_back(r);
                            }
                        }
                        send_cmd(response_tx, Resp::Array(responses));
                    }

                    RedisCommand::Discard => {
                        self.multi_list.remove(&client_id);
                        send_cmd(response_tx, Resp::SimpleString(b"OK".to_vec()));
                    }

                    _ => {
                        client.push(command);
                        send_cmd(response_tx, Resp::SimpleString(b"QUEUED".to_vec()));
                    }
                }

                continue;
            }

            let outcome = self.execute_commands(command.clone(), client_id);

            match outcome {
                CommandResult::Response(resp) => {
                    send_cmd(response_tx, resp);
                }
                CommandResult::Wait {
                    timeout,
                    replicas,
                    offset,
                } => {
                    self.cleanup_dead_slaves();
                    let slave_count = self.slaves.len() as u64;

                    if slave_count == 0 || offset == 0 {
                        send_cmd(response_tx, Resp::Integer(slave_count as usize));
                        continue;
                    }

                    let (ack_tx, mut ack_rx) = mpsc::unbounded_channel::<(Uuid, u64)>();
                    self.ack_waiters.push(ack_tx);

                    let mut ack_buf = Vec::new();
                    Resp::Array(
                        vec![
                            Resp::BulkString(b"REPLCONF".to_vec()),
                            Resp::BulkString(b"GETACK".to_vec()),
                            Resp::BulkString(b"*".to_vec()),
                        ]
                        .into(),
                    )
                    .write_format(&mut ack_buf);

                    let mut slaves = self.slaves.clone();

                    tokio::spawn(async move {
                        let mut acked = 0;
                        let deadline = tokio::time::sleep(Duration::from_millis(timeout));
                        tokio::pin!(deadline);
                        let mut interval = tokio::time::interval(Duration::from_millis(50));

                        for slave in slaves.values() {
                            let _ = slave.send(ack_buf.clone());
                        }
                        loop {
                            tokio::select! {
                                _ = &mut deadline => break,
                                // _ = interval.tick() => {
                                //     for slave in slaves.values() { let _ = slave.send(ack_buf.clone()); }
                                // }
                                Some((slave_id, ack_offset)) = ack_rx.recv() => {
                                    if ack_offset >= offset {
                                        slaves.remove_entry(&slave_id);
                                        acked+=1
                                    }
                                    if acked as u64 >= replicas {
                                        break;
                                    }
                                }
                            }
                        }
                        send_cmd(response_tx, Resp::Integer(acked));
                    });
                }
                CommandResult::RegisterSlave(resp) => match self.role {
                    Role::Master { .. } => {
                        send_cmd(response_tx.clone(), resp);

                        let rdb_bytes = base64::engine::general_purpose::STANDARD
                                .decode("UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==")
                                .unwrap();
                        let header = format!("${}\r\n", rdb_bytes.len());
                        let _ = response_tx.send(header.into_bytes());
                        let _ = response_tx.send(rdb_bytes);

                        self.slaves.insert(client_id, response_tx);
                    }
                    Role::Slave { .. } => {
                        send_cmd(response_tx.clone(), resp);
                    }
                },

                CommandResult::BlockList { keys, timeout } => {
                    self.create_blocking_client(client_id, keys, response_tx, timeout);
                }

                CommandResult::BlockStream {
                    client_id,
                    streams,
                    timeout_ms,
                } => {
                    self.create_blocking_stream_client(client_id, streams, response_tx, timeout_ms);
                }

                CommandResult::Exec(cmds) => {
                    let mut responses = VecDeque::new();
                    for cmd in cmds {
                        if let CommandResult::Response(r) = self.execute_commands(cmd, client_id) {
                            responses.push_back(r);
                        }
                    }
                    send_cmd(response_tx, Resp::Array(responses));
                }

                CommandResult::None => {}
            }

            if Self::is_write_command(&command) {
                let slaves_count = self.send_slaves(&cmd_buffer);
            }
        }
    }
}
