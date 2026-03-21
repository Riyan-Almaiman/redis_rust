use std::collections::VecDeque;
use std::time::Duration;
use tokio::sync::oneshot;
use uuid::Uuid;
use crate::blocking_list::{BlockingClient, BlockingList};
use crate::blocking_stream::{BlockingStreamClient, BlockingStreams, StreamWait};
use crate::commands_parser::RedisCommand;
use crate::db::{Client, DB};
use crate::resp::Resp;
use crate::valuetype::ValueType;

pub struct BlockingManager {
    pub lists: BlockingList,
    pub streams: BlockingStreams,
}

impl DB {
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
            self.blocking.streams
                .waiters
                .entry(wait.key.clone())
                .or_default()
                .push_back(client_id);
        }
        self.blocking.streams.clients.insert(client_id, client);

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

        self.blocking.lists.register(keys, client);
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
        if let Some(client) = self.blocking.streams.clients.remove(&client_id) {
            // Remove from all wait queues
            for q in self.blocking.streams.waiters.values_mut() {
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
}