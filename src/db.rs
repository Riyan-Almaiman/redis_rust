use crate::blocking_list::{BlockingClient, BlockingList};
use crate::blocking_stream::{BlockingStreamClient, BlockingStreams, StreamWait};
use crate::command_router::CommandResult;
use crate::commands_parser::RedisCommand;
use crate::lists::List;
use crate::parser;
use crate::resp::Resp;
use crate::valuetype::ValueType;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::fmt::format;
use std::time::{Duration, SystemTime};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

pub struct Master {
    pub replication_id: String,
    pub replication_offset: u64,
}
#[derive(Clone)]

pub enum Role {
    Master {
        replication_id: String,
        replication_offset: u64,
    },
    Slave {
        master: String,
        replication_id: String,
        port: String,
        replication_offset: u64,
    },
}
impl Role {
    pub fn get_replication(&self) -> String {
        match self {
            Role::Master {
                replication_id,
                replication_offset,
            } => format!(
                "{}\r\nmaster_replid:{}\r\nmaster_repl_offset:{}\r\n",
                self.get_role(),
                replication_id,
                replication_offset
            ),
            Role::Slave {
                master,
                replication_id,
                port,
                replication_offset,
            } => format!(
                "{}\r\nmaster_replid:{}\r\nmaster_repl_offset:{}\r\n",
                self.get_role(),
                replication_id,
                replication_offset
            ),
        }
    }
    pub fn get_repl_id(&self) -> String {
        match self {
            Role::Master {
                replication_id,
                replication_offset,
            } => replication_id.clone(),

            Role::Slave {
                master,
                replication_id,
                port,
                replication_offset,
            } => replication_id.clone(),
        }
    }
    pub fn get_repl_offset(&self) -> String {
        match self {
            Role::Master {
                replication_id,
                replication_offset,
            } => replication_offset.to_string(),
            Role::Slave {
                master,
                replication_id,
                port,
                replication_offset,
            } => replication_offset.to_string(),
        }
    }

    pub fn get_role(&self) -> String {
        match self {
            Role::Master { .. } => "role:master".to_string(),
            Role::Slave { .. } => "role:slave".to_string(),
        }
    }
}
pub struct DB {
    pub database: HashMap<Vec<u8>, KeyValue>,
    pub multi_list: HashMap<Uuid, Vec<RedisCommand>>,
    pub receiver: mpsc::Receiver<Client>,
    pub blocked_list: BlockingList,
    pub blocking_streams: BlockingStreams,
    pub sender: mpsc::Sender<Client>,
    pub role: Role,
    pub slaves: Vec<tokio::sync::mpsc::Sender<Vec<u8>>>
}
#[derive(Debug)]
pub struct Client {
    pub client_id: Uuid,
    pub timeout: Option<Duration>,
    pub response_tx: oneshot::Sender<Resp>,
    pub response_tx_slave: Option<tokio::sync::mpsc::Sender<Vec<u8>>>,
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
    pub async fn new(role: Role) -> Self {
        let (pipeline_tx, pipeline_rx) = tokio::sync::mpsc::channel::<Client>(1000);

        Self {
            database: HashMap::new(),
            sender: pipeline_tx,
            receiver: pipeline_rx,
            role,
            slaves: Vec::new(),
            multi_list: HashMap::new(),
            blocked_list: BlockingList {
                blocked_list: Default::default(),
                waiters: HashMap::new(),
            },
            blocking_streams: BlockingStreams {
                waiters: HashMap::new(),
                clients: HashMap::new(),
            },
        }
    }fn is_write_command(cmd: &RedisCommand) -> bool {
        matches!(cmd,
        RedisCommand::Set { .. } |
        RedisCommand::Incr { .. } |
        RedisCommand::RPush { .. } |
        RedisCommand::LPush { .. } |
        RedisCommand::XAdd { .. }
    )
    }
    pub fn create_blocking_stream_client(
        &mut self,
        client_id: Uuid,
        resolved_streams: Vec<StreamWait>,
        response_tx: oneshot::Sender<Resp>,
        timeout_ms: f64,
    ) {
        let client = BlockingStreamClient {
            id: client_id,
            response_tx,
            waits: resolved_streams,
        };

        for wait in &client.waits {
            self.blocking_streams
                .waiters
                .entry(wait.key.clone())
                .or_default()
                .push_back(client_id);
        }
        self.blocking_streams.clients.insert(client_id, client);

        if timeout_ms > 0.0 {
            let sender_clone = self.sender.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(timeout_ms as u64)).await;
                let _ = sender_clone
                    .send(Client {
                        client_id,
                        timeout: None,
                        response_tx: oneshot::channel().0,
                        response_tx_slave: None,
                        resp_command: Resp::Error(b"ok".to_vec()),
                        command: RedisCommand::InternalTimeoutCleanup { client_id },
                    })
                    .await;
            });
        }
    }

    pub fn create_blocking_client(
        &mut self,
        client_id: Uuid,
        keys: Vec<Vec<u8>>,
        response_tx: oneshot::Sender<Resp>,
        timeout: f64,
    ) {
        let client = BlockingClient {
            id: client_id,
            response_tx,
        };

        self.blocked_list.register(keys, client);
        if timeout > 0.0 {
            let sender_clone = self.sender.clone();
            let id = client_id;

            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs_f64(timeout)).await;

                let _ = sender_clone
                    .send(Client {
                        client_id: id,
                        timeout: None,
                        response_tx: oneshot::channel().0,
                        response_tx_slave: None,
                        resp_command: Resp::Error(b"ok".to_vec()),

                        command: RedisCommand::InternalTimeoutCleanup { client_id: id },
                    })
                    .await;
            });
        }
    }
    pub fn wake_stream_client(&mut self, client_id: Uuid) {
        if let Some(client) = self.blocking_streams.clients.remove(&client_id) {
            // Remove from all wait queues
            for q in self.blocking_streams.waiters.values_mut() {
                q.retain(|id| *id != client_id);
            }

            let mut result = VecDeque::new();

            for wait in &client.waits {
                if let Some(kv) = self.database.get(&wait.key) {
                    if let ValueType::Stream(stream) = &kv.value {
                        let entries = stream.get_read_streams(wait.time, wait.sequence);

                        if let Resp::Array(ref arr) = entries {
                            if !arr.is_empty() {
                                result.push_back(Resp::Array(VecDeque::from([
                                    Resp::BulkString(wait.key.clone()),
                                    entries,
                                ])));
                            }
                        }
                    }
                }
            }

            let _ = client.response_tx.send(Resp::Array(result));
        }
    }

    pub async fn start(&mut self) {
        while let Some(mut request) = self.receiver.recv().await {
            let Client {
                command,
                response_tx,
                client_id,
                ..
            } = request;

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

                        let _ = response_tx.send(Resp::Array(responses));
                    }

                    RedisCommand::Discard => {
                        self.multi_list.remove(&client_id);
                        let _ = response_tx.send(Resp::SimpleString(b"OK".to_vec()));
                    }

                    _ => {
                        client.push(command);
                        let _ = response_tx.send(Resp::SimpleString(b"QUEUED".to_vec()));
                    }
                }

                continue;
            }
            let outcome = self.execute_commands(command.clone(), client_id);
            if Self::is_write_command(&command){
                let mut buf = Vec::new();
                request.resp_command.write_format(&mut buf);

                for slave in &self.slaves {
                    let _ = slave.try_send(buf.clone());
                }
            }
            match outcome {
                CommandResult::Response(resp) => {
                    let _ = response_tx.send(resp);
                }
                CommandResult::RegisterSlave(resp) => {
                    if let Some(client) =  request.response_tx_slave {
                        self.slaves.push(client)

                    }
                    let _ = response_tx.send(resp);

                }

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
                    let _ = response_tx.send(Resp::Array(responses));
                }

                CommandResult::None => {}
            }
        }
    }
}
