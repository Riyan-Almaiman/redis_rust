use crate::blocking_list::{BlockingClient, BlockingList};
use crate::blocking_stream::{BlockingStreamClient, BlockingStreams, StreamWait};
use crate::command_router::CommandResult;
use crate::commands_parser::RedisCommand;
use crate::lists::List;
use crate::{command_router, parser};
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
use crate::blocking_manger::BlockingManager;
use crate::role::Role;

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
    pub fn execute_commands(&mut self, command: RedisCommand, client_id: Uuid) -> CommandResult {
        command_router::route(self, command, client_id)
    }
    pub async fn new(role: Role) -> Self {
        let (pipeline_tx, pipeline_rx) = tokio::sync::mpsc::channel::<Client>(1000);

        Self {
            database: HashMap::new(),
            sender: pipeline_tx,
            receiver: pipeline_rx,
            role,
            slaves: Vec::new(),
            multi_list: HashMap::new(),
            blocking: BlockingManager {
                lists: Default::default(),
                streams: Default::default(),
            }
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


    pub async fn start(&mut self) {
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
