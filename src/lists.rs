use crate::resp::Resp;
use crate::resp::Resp::Integer;
use crate::RedisCommand;
use indexmap::IndexMap;
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};
use tokio::sync::oneshot::Sender;
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;
#[derive(Debug)]

pub struct Lists {
    lists: HashMap<String, List>,
    pub blocking_clients: HashMap<String, IndexMap<Uuid, BlockingClient>>,
    pub sender: mpsc::Sender<Client>,
    pub receiver: mpsc::Receiver<Client>,
}
#[derive(Debug)]

pub struct List {
    key: String,
    list: VecDeque<Vec<u8>>,
}

#[derive(Debug)]

pub struct BlockingClient {
    id: Uuid,
    pub response_tx: oneshot::Sender<Resp>,
    timeout: Duration,
    created_at: Instant,
}
#[derive(Debug)]
pub struct Client {
    pub client_id: Uuid,
    pub timeout: Option<Duration>,
    pub response_tx: oneshot::Sender<Resp>,
    pub command: RedisCommand,
}
enum CommandOutcome {
    Done(Resp),
    Blocked{
        keys: Vec<Vec<u8>>,
        timeout: u64,
        id: Uuid,
    },
}

impl Lists {
    pub fn new() -> Self {
        let (pipeline_tx, mut pipeline_rx) = tokio::sync::mpsc::channel::<Client>(100);

        Lists {
            lists: HashMap::new(),
            receiver: pipeline_rx,
            sender: pipeline_tx,
            blocking_clients: HashMap::new(),
        }
    }
    pub async fn start(&mut self) {
        while let Some(request) = self.receiver.recv().await {
            let response_tx = request.response_tx;

            let r = match request.command {
                RedisCommand::RPush { elements, key } => self.rpush(key, elements),
                RedisCommand::LRange { key, start, stop } => self.get_list_range(key, start, stop),
                RedisCommand::LPop { key, count } => self.lpop(&key, count),
                RedisCommand::LLen(key) => self.llen(&key),
                RedisCommand::LPush { key, elements } => self.lpush(&key, elements),
                RedisCommand::BLPop { keys, timeout } => {
                    self.blpop(&keys, timeout, request.client_id)
                }
                RedisCommand::InternalTimeoutCleanup { key, client_id } => {
                    if let Some(clients) = self.blocking_clients.get_mut(&key) {
                        if let Some(expired_client) = clients.shift_remove(&client_id) {
                            expired_client
                                .response_tx
                                .send(Resp::NullBulkString)
                                .expect("failed to send response");
                        }
                    };
                    CommandOutcome::Done(Resp::NullBulkString)
                }

                _ => {

                    CommandOutcome::Done(Resp::NullArray)
                }
            };
            match r {
                 CommandOutcome::Done(resp) => {
                     response_tx.send(resp).expect("Could not send response");
                 }
                CommandOutcome::Blocked{keys, timeout, id} => {
                    self.create_blocking_client(&keys, timeout, id, response_tx)
                }
            }

        }
    }
    fn blpop(
        &mut self,
        keys: &Vec<Vec<u8>>,
        timeout: u64,
        id: Uuid,
    ) -> CommandOutcome {
        for key in keys {
            let list = self.get_list(&key);
            match list {
                Ok(list) => {
                    if !list.list.is_empty() {
                        match self.lpop(key, 1) {
                            CommandOutcome::Done(resp) => {
                                return CommandOutcome::Done(Resp::Array(vec![Resp::BulkString(key.clone()),resp]));
                            }
                            CommandOutcome::Blocked{keys, timeout, id} => {

                            }
                        }
                    }
                }
                Err(_) => {}
            }
        }


        CommandOutcome::Blocked {
            keys: keys.clone(),
            timeout: timeout.clone(),
            id: id.clone(),
        }
    }
    fn create_blocking_client(
        &mut self,
        keys: &Vec<Vec<u8>>,
        timeout: u64,
        id: Uuid,
        response_tx: Sender<Resp>,
    )  {


        let key_str = String::from_utf8_lossy(keys[0].as_slice()).to_string();
        let sender_clone = self.sender.clone();
        if timeout > 0 {
            let key_clone = key_str.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(timeout)).await;
                let _ = sender_clone
                    .send(Client {
                        client_id: id,
                        timeout: None,
                        response_tx: oneshot::channel().0,
                        command: RedisCommand::InternalTimeoutCleanup {
                            key: key_clone,
                            client_id: id,
                        },
                    })
                    .await;
            });
        }
        let client = BlockingClient {
            id,
            response_tx,
            timeout: Duration::from_secs(timeout),
            created_at: Instant::now(),
        };

        self.blocking_clients
            .entry(key_str)
            .or_default()
            .insert(id, client);
    }
    fn rpush(&mut self, key: Vec<u8>, elements: Vec<Vec<u8>>) -> CommandOutcome {
        let key_str = match std::str::from_utf8(&key) {
            Ok(v) => v.to_string(),
            Err(e) => return CommandOutcome::Done(Resp::Error(e.to_string().as_bytes().to_vec())),
        };

        let list_entry = self.lists.entry(key_str.clone()).or_insert(List {
            key: key_str,
            list: VecDeque::new(),
        });
        let mut blocked_consumed_count = 0;

        if let Some(clients) = self.blocking_clients.get_mut(&list_entry.key) {
            for element in elements {
                if let Some(blocked_client) = clients.shift_remove_index(0) {

                    let _ = blocked_client.1.response_tx.send(Resp::Array(vec![Resp::BulkString(key.clone()), Resp::BulkString(element.clone()) ]));
                    blocked_consumed_count += 1;
                } else {
                    list_entry.list.push_back(element);
                }
            }
        } else {
            for element in elements {
                list_entry.list.push_back(element);
            }
        }


        CommandOutcome::Done(Integer(list_entry.list.len() + blocked_consumed_count))
    }

    fn lpop(&mut self, key: &Vec<u8>, count: usize) -> CommandOutcome {
        let key_str = match std::str::from_utf8(&key) {
            Ok(v) => v.to_string(),
            Err(_) => {
                return CommandOutcome::Done(Resp::Error(
                    "Invalid UTF-8 key".to_string().as_bytes().to_vec(),
                ));
            }
        };

        let list_entry = match self.lists.get_mut(&key_str) {
            Some(l) => l,
            None => {
                return CommandOutcome::Done(Resp::NullBulkString);
            }
        };

        if list_entry.list.is_empty() {
            return CommandOutcome::Done(Resp::NullBulkString);
        }

        let mut popped = Vec::new();
        while popped.len() < count {
            if let Some(element) = list_entry.list.pop_front() {
                popped.push(Resp::BulkString(element));
            } else {
                break;
            }
        }

        if popped.len() == 1 && count == 1 {
            return CommandOutcome::Done(popped.pop().unwrap());
        } else if popped.is_empty() {
            return CommandOutcome::Done(Resp::NullArray);
        } else {
            return CommandOutcome::Done(Resp::Array(popped));
        }
    }
    fn llen(&self, key: &Vec<u8>) -> CommandOutcome {
        let list = match self.get_list(key) {
            Ok(v) => v,
            Err(e) => {
                return CommandOutcome::Done(Integer(0));
            }
        };

        return CommandOutcome::Done(Integer(list.list.len()));
    }
    fn get_list(&self, key: &Vec<u8>) -> Result<&List, String> {
        let key = match std::str::from_utf8(key) {
            Ok(v) => v.to_string(),
            Err(e) => return Err(e.to_string()),
        };

        match self.lists.get(&key) {
            Some(l) => Ok(l),
            None => return Err(format!("key {} not found", key)),
        }
    }
    fn get_or_create_list(&mut self, key: &Vec<u8>) -> Result<&mut List, String> {
        let key = match std::str::from_utf8(&key) {
            Ok(v) => v.to_string(),
            Err(e) => return Err(e.to_string()),
        };

        Ok(self.lists.entry(key.clone()).or_insert(List {
            key,
            list: VecDeque::new(),
        }))
    }
    fn get_list_range(&self, key: Vec<u8>, start_raw: i64, end_raw: i64) -> CommandOutcome {
        let list_entry = match self.get_list(&key) {
            Ok(l) => l,
            Err(_) => {
                return CommandOutcome::Done(Resp::Array(Vec::new()));
            }
        };

        let len = list_entry.list.len() as i64;

        if len == 0 {
            return CommandOutcome::Done(Resp::Array(Vec::new()));
        }

        let start = if start_raw < 0 {
            (len + start_raw).max(0)
        } else {
            start_raw
        };

        let end = if end_raw < 0 {
            (len + end_raw).max(0)
        } else {
            end_raw
        };

        if start > end || start >= len {
            return CommandOutcome::Done(Resp::Array(Vec::new()));
        }

        let start = start as usize;
        let count = ((end as usize).min(len as usize - 1)) - start + 1;

        let items: Vec<Resp> = list_entry
            .list
            .iter()
            .skip(start)
            .take(count)
            .map(|v| Resp::BulkString(v.clone()))
            .collect();

        return CommandOutcome::Done(Resp::Array(items));
    }
    fn lpush(&mut self, key: &Vec<u8>, elements: Vec<Vec<u8>>) -> CommandOutcome {
        let list = match self.get_or_create_list(key) {
            Ok(l) => l,
            Err(e) => {
                return CommandOutcome::Done(Resp::Error(e.as_bytes().to_vec()));
            }
        };

        for (i, m) in elements.iter().enumerate() {
           
                list.list.push_front(elements[i].clone())
            
        }

        return CommandOutcome::Done(Integer(list.list.len()));
    }
}
