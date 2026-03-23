pub mod auth;
pub mod loader;
pub mod runtime;

use crate::blocking_manger::BlockingManager;
use crate::commands;
use crate::commands_parser::RedisCommand;
use crate::resp::Resp;
use crate::role::Role;
use crate::user::{Flag, User};
use crate::valuetype::ValueType;
use std::collections::{HashMap, HashSet};
use std::mem;
use std::time::{Duration, SystemTime};
use tokio::sync::mpsc;
use uuid::Uuid;

pub struct Master {
    pub replication_id: String,
    pub replication_offset: u64,
}

#[derive(Default)]
pub struct ClientSession {
    pub authenticated_user: Option<String>,
    pub queued_commands: Vec<RedisCommand>,
    pub in_multi: bool,
    pub client_id: Uuid,
    pub response_tx: Option<mpsc::UnboundedSender<Vec<u8>>>,

    pub subscriptions: Vec<String>,
    pub subscriber_tx: Option<mpsc::UnboundedSender<Vec<u8>>>,
}

pub struct DB {
    pub database: HashMap<Vec<u8>, KeyValue>,
    pub sessions: HashMap<Uuid, ClientSession>,
    pub receiver: mpsc::Receiver<ClientRequest>,
    pub blocking: BlockingManager,
    pub sender: mpsc::Sender<ClientRequest>,
    pub role: Role,
    pub slaves: HashMap<Uuid, mpsc::UnboundedSender<Vec<u8>>>,
    pub ack_waiters: Vec<mpsc::UnboundedSender<(Uuid, u64)>>,
    pub dir: String,
    pub users: HashMap<String, User>,
    pub file_name: String,
}

#[derive(Debug)]
pub enum ClientRequest {
    Connected {
        client_id: Uuid,
        response_tx: mpsc::UnboundedSender<Vec<u8>>,
    },
    Command {
        client_id: Uuid,
        response_tx: mpsc::UnboundedSender<Vec<u8>>,
        resp_command: Resp,
        command: RedisCommand,
    },
       Disconnected {
        client_id: Uuid,
    },
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
    pub fn execute_commands(&mut self, command: RedisCommand, client_id: Uuid) -> commands::CommandResult {
        commands::route(self, command, client_id)
    }
    pub async fn new(role: Role, dir: String, file_name: String) -> Self {
        let (pipeline_tx, pipeline_rx) = tokio::sync::mpsc::channel::<ClientRequest>(1000);

        let user = User {
            name: "default".to_string(),
            passwords:  Vec::new(),
            allowed_commands: HashSet::new(),
            flags: HashSet::from([Flag::NoPass]),
        };
        let mut db = Self {
            database: HashMap::new(),
            sender: pipeline_tx,
            receiver: pipeline_rx,
            file_name,
            dir,
            role,
            users: HashMap::from([("default".to_string(), user)]),
            sessions: HashMap::new(),
            ack_waiters: Vec::new(),
            slaves: HashMap::new(),
            blocking: BlockingManager {
                lists: Default::default(),
                streams: Default::default(),
            },
        };

        db.load_rdb(&format!("{}/{}", db.dir, db.file_name));
        db
    }

    pub fn is_subscribed_client(&self, client_id: Uuid) -> bool {
        self.sessions
            .get(&client_id)
            .is_some_and(|session| !session.subscriptions.is_empty())
    }

    pub fn set_subscriber_tx(
        &mut self,
        client_id: Uuid,
        response_tx: mpsc::UnboundedSender<Vec<u8>>,
    ) {
        self.session_mut(client_id).subscriber_tx = Some(response_tx);
    }

    pub fn subscribe_client(&mut self, client_id: Uuid, channel: String) -> usize {
        let session = self.session_mut(client_id);
        if !session.subscriptions.contains(&channel) {
            session.subscriptions.push(channel);
        }
        session.subscriptions.len()
    }

    pub fn unsubscribe_client(&mut self, client_id: Uuid, channel: &str) -> usize {
        let session = self.session_mut(client_id);
        session.subscriptions.retain(|subscription| subscription != channel);
        if session.subscriptions.is_empty() {
            session.subscriber_tx = None;
        }
        session.subscriptions.len()
    }

    pub fn publish_message(&self, channel: &str, message: &str) -> usize {
        let mut count = 0;

        for session in self.sessions.values() {
            if !session.subscriptions.iter().any(|subscription| subscription == channel) {
                continue;
            }

            let Some(tx) = session.subscriber_tx.as_ref() else {
                continue;
            };

            let mut resp = Vec::new();
            Resp::Array(
                vec![
                    Resp::BulkString(b"message".to_vec()),
                    Resp::BulkString(channel.as_bytes().to_vec()),
                    Resp::BulkString(message.as_bytes().to_vec()),
                ]
                .into(),
            )
            .write_format(&mut resp);

            if tx.send(resp).is_ok() {
                count += 1;
            }
        }

        count
    }

    pub fn begin_multi(&mut self, client_id: Uuid) {
        let session = self.session_mut(client_id);
        session.in_multi = true;
        session.queued_commands.clear();
    }

    pub fn is_in_multi(&self, client_id: Uuid) -> bool {
        self.sessions
            .get(&client_id)
            .is_some_and(|session| session.in_multi)
    }

    pub fn queue_command(&mut self, client_id: Uuid, command: RedisCommand) {
        self.session_mut(client_id).queued_commands.push(command);
    }

    pub fn discard_multi(&mut self, client_id: Uuid) -> bool {
        let Some(session) = self.sessions.get_mut(&client_id) else {
            return false;
        };

        if !session.in_multi {
            return false;
        }

        session.in_multi = false;
        session.queued_commands.clear();
        true
    }

    pub fn take_multi_commands(&mut self, client_id: Uuid) -> Option<Vec<RedisCommand>> {
        let session = self.sessions.get_mut(&client_id)?;
        if !session.in_multi {
            return None;
        }

        session.in_multi = false;
        Some(mem::take(&mut session.queued_commands))
    }
}
