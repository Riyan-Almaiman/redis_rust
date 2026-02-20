use crate::db::{Client, CommandOutcome};
use crate::resp::Resp;
use crate::resp::Resp::Integer;
use crate::RedisCommand;
use indexmap::IndexMap;
use std::collections::VecDeque;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

#[derive(Debug)]

pub struct List {
    key: Vec<u8>,
    list: VecDeque<Vec<u8>>,
    pub blocking_clients: IndexMap<Uuid, BlockingClient>,
}
#[derive(Debug)]

pub struct BlockingClient {
    id: Uuid,
    pub response_tx: oneshot::Sender<Resp>,
    timeout: Duration,
    created_at: Instant,
}

impl List {
    pub fn new(key: Vec<u8>) -> Self {
        List {
            key,
            list: VecDeque::new(),
            blocking_clients: IndexMap::new(),
        }
    }
    pub fn try_blpop(
        &mut self,
        id: Uuid,
        tx: oneshot::Sender<Resp>,
        timeout: f64,
        sender: mpsc::Sender<Client>,
    ) {
        if let Some(element) = self.list.pop_front() {
            let _ = tx.send(Resp::Array(vec![
                Resp::BulkString(self.key.clone()),
                Resp::BulkString(element),
            ]));
        } else {
            self.create_blocking_client(&vec![self.key.clone()], timeout, id, tx, sender);
        }
    }
    pub fn blpop(&mut self, keys: &Vec<Vec<u8>>, timeout: f64, id: Uuid) -> CommandOutcome {
        for key in keys {
            if !self.list.is_empty() {
                return match self.lpop(key, 1) {
                    CommandOutcome::Done(resp) => {
                        CommandOutcome::Done(Resp::Array(vec![Resp::BulkString(key.clone()), resp]))
                    }
                    CommandOutcome::Blocked { keys, timeout, id } => {
                        CommandOutcome::Done(Resp::Error("error".as_bytes().to_vec()))
                    }
                };
            }
        }

        CommandOutcome::Blocked {
            keys: keys.clone(),
            timeout: timeout.clone(),
            id: id.clone(),
        }
    }
    // Add 'sender: mpsc::Sender<Client>' as an argument
    pub fn create_blocking_client(
        &mut self,
        keys: &Vec<Vec<u8>>,
        timeout: f64,
        id: Uuid,
        response_tx: oneshot::Sender<Resp>,
        sender: mpsc::Sender<Client>,
    ) {
        let key_bytes = keys[0].clone();
        let key_str = String::from_utf8_lossy(&key_bytes).to_string();

        if timeout > 0.0 {
            let sender_clone = sender.clone();
            let key_clone = key_str.clone();

            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs_f64(timeout)).await;
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
            timeout: Duration::from_secs_f64(timeout),
            created_at: Instant::now(),
        };

        self.blocking_clients.insert(id, client);
    }
    pub fn rpush(&mut self, elements: Vec<Vec<u8>>) -> CommandOutcome {
        let mut blocked_consumed_count = 0;

        if !self.blocking_clients.is_empty() {
            for element in elements {
                if let Some(blocked_client) = self.blocking_clients.shift_remove_index(0) {
                    let _ = blocked_client.1.response_tx.send(Resp::Array(vec![
                        Resp::BulkString(self.key.clone()),
                        Resp::BulkString(element.clone()),
                    ]));
                    blocked_consumed_count += 1;
                } else {
                    self.list.push_back(element);
                }
            }
        } else {
            for element in elements {
                self.list.push_back(element);
            }
        }

        CommandOutcome::Done(Integer(self.list.len() + blocked_consumed_count))
    }

    pub fn lpop(&mut self, key: &Vec<u8>, count: usize) -> CommandOutcome {
        let key_str = match std::str::from_utf8(&key) {
            Ok(v) => v.to_string(),
            Err(_) => {
                return CommandOutcome::Done(Resp::Error(
                    "Invalid UTF-8 key".to_string().as_bytes().to_vec(),
                ));
            }
        };

        if self.list.is_empty() {
            return CommandOutcome::Done(Resp::NullBulkString);
        }

        let mut popped = Vec::new();
        while popped.len() < count {
            if let Some(element) = self.list.pop_front() {
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
    pub fn llen(&self) -> CommandOutcome {
        return CommandOutcome::Done(Integer(self.list.len()));
    }

    pub fn get_list_range(&self, start_raw: i64, end_raw: i64) -> CommandOutcome {
        let len = self.list.len() as i64;

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

        let items: Vec<Resp> = self
            .list
            .iter()
            .skip(start)
            .take(count)
            .map(|v| Resp::BulkString(v.clone()))
            .collect();

        return CommandOutcome::Done(Resp::Array(items));
    }
    pub fn lpush(&mut self, elements: Vec<Vec<u8>>) -> CommandOutcome {
        for (i, m) in elements.iter().enumerate() {
            self.list.push_front(elements[i].clone())
        }

        return CommandOutcome::Done(Integer(self.list.len()));
    }
}
