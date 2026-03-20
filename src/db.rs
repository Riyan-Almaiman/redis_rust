use crate::blocking_list::{BlockingClient, BlockingList};
use crate::blocking_stream::{BlockingStreamClient, BlockingStreams, StreamWait};
use crate::commands::RedisCommand;
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
}
#[derive(Debug)]
pub struct Client {
    pub client_id: Uuid,
    pub timeout: Option<Duration>,
    pub response_tx: oneshot::Sender<Resp>,
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
        match &self.role {
            Role::Slave {
                master,
                replication_id,
                port,
                replication_offset,
            } => {
                if let Ok(mut stream) = TcpStream::connect(master).await {
                    let mut buf = [0; 1024];

                    let mut write = Vec::new();
                    let ping = Resp::Array(VecDeque::from([Resp::BulkString(b"PING".to_vec())]));
                    ping.write_format(&mut write);
                    let bytes = stream.write_all(write.as_slice()).await;
                    if let Ok(r) = bytes {
                        let n = stream.read(&mut buf).await.unwrap();
                        write.clear();
                        println!("Response: {:?}", &buf[..n]);
                        let port = Resp::Array(VecDeque::from([Resp::BulkString(
                         "REPLCONF".as_bytes().to_vec()
                        ), Resp::BulkString("listening-port".as_bytes().to_vec()), Resp::BulkString(port.as_bytes().to_vec())]));
                        port.write_format(&mut write);
                        let bytes = stream.write_all(&write.as_slice()).await;
                        if let Ok(r) = bytes {
                            let n = stream.read(&mut buf).await.unwrap();
                            write.clear();
                            println!("Response: {:?}", &buf[..n]);
                            let protocol = Resp::Array(VecDeque::from([Resp::BulkString(
                              "REPLCONF" 
                                    .as_bytes()
                                    .to_vec(),
                            ), Resp::BulkString("psync2".as_bytes().to_vec())]));
                            protocol.write_format(&mut write);
                            let bytes = stream.write_all(&write.as_slice()).await;
                        }
                    }
                } else {
                    panic!("couldnt connect {}", master);
                }
            }
            _ => (),
        }
        while let Some(request) = self.receiver.recv().await {
            let Client {
                command,
                response_tx,
                client_id,
                ..
            } = request;

            if let Some(client) = self.multi_list.get_mut(&client_id) {
                match command {
                    RedisCommand::Exec => {
                        let outcome = self.execute_commands(command, client_id);
                        match outcome {
                            Resp::Exec(redis_commands) => {
                                let mut responses = VecDeque::new();
                                for cmd in redis_commands {
                                    responses.push_back(self.execute_commands(cmd, client_id));
                                }
                                let _ = response_tx.send(Resp::Array(responses));
                            }
                            _ => (),
                        }
                    }
                    RedisCommand::Discard => {
                        let outcome = self.execute_commands(command, client_id);
                        let _ = response_tx.send(outcome);
                    }
                    _ => {
                        client.push(command);
                        let _ = response_tx.send(Resp::SimpleString(b"QUEUED".to_vec()));
                    }
                }
            } else {
                let outcome = self.execute_commands(command, client_id);
                match outcome {
                    Resp::Array(resps) => {
                        let _ = response_tx.send(Resp::Array(resps));
                    }
                    Resp::BulkString(items) => {
                        let _ = response_tx.send(Resp::BulkString(items));
                    }
                    Resp::Integer(v) => {
                        let _ = response_tx.send(Resp::Integer(v));
                    }
                    Resp::NullArray => {
                        let _ = response_tx.send(Resp::NullArray);
                    }
                    Resp::SimpleString(items) => {
                        let _ = response_tx.send(Resp::SimpleString(items));
                    }
                    Resp::NullBulkString => {
                        let _ = response_tx.send(Resp::NullBulkString);
                    }
                    Resp::BlockingClient { keys, timeout } => {
                        self.create_blocking_client(client_id, keys, response_tx, timeout)
                    }
                    Resp::Exec(redis_commands) => {
                        let mut responses = VecDeque::new();
                        for cmd in redis_commands {
                            responses.push_back(self.execute_commands(cmd, client_id));
                        }
                        let _ = response_tx.send(Resp::Array(responses));
                    }
                    Resp::BlockingStreamClient {
                        client_id,
                        resolved_streams,
                        timeout_ms,
                    } => self.create_blocking_stream_client(
                        client_id,
                        resolved_streams,
                        response_tx,
                        timeout_ms,
                    ),
                    Resp::None => (),
                    Resp::Error(items) => {
                        let _ = response_tx.send(Resp::Error(items));
                    }
                }
            }
        }
    }
}
