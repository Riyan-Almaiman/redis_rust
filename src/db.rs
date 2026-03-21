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
use crate::send::send_cmd;

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
    pub slaves: Vec<tokio::sync::mpsc::UnboundedSender<Vec<u8>>>
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
    }
    fn is_write_command(cmd: &RedisCommand) -> bool {
        matches!(cmd,
        RedisCommand::Set { .. } |
        RedisCommand::Incr { .. } |
        RedisCommand::RPush { .. } |
        RedisCommand::LPush { .. } |
        RedisCommand::XAdd { .. }
    )


    }

    fn send_slaves(&self, resp: &Vec<u8>) {
                for slave in &self.slaves{
            let _ = slave.send(resp.clone());
        }
    }
    pub async fn start(&mut self) {
        while let Some(request) = self.receiver.recv().await {


            let Client {
                command,
                response_tx,
                client_id,
                ..
            } = request;
            let mut cmd_buffer = Vec::new();
            request.resp_command.write_format(&mut cmd_buffer);


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
            if Self::is_write_command(&command){
                    self.send_slaves(&cmd_buffer)
            }
            match outcome {
                CommandResult::Response(resp) => {
                    send_cmd(response_tx, resp);
                }
                CommandResult::RegisterSlave(resp) => {
                        self.slaves.push(response_tx);
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
                    send_cmd(response_tx, Resp::Array(responses));
                }

                CommandResult::None => {}
            }
        }
    }
}
