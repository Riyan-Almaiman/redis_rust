use crate::resp::Resp;
use std::collections::{HashMap, HashSet, VecDeque};
use tokio::sync::oneshot;
use uuid::Uuid;

pub struct BlockingList {
    pub waiters: HashMap<Vec<u8>, VecDeque<Uuid>>,
    pub blocked_list: HashMap<Uuid, BlockingClient>,
}

pub struct BlockingClient {
    pub id: Uuid,
    pub response_tx: oneshot::Sender<Resp>,
}
impl BlockingList {
    pub fn register(&mut self, key: Vec<Vec<u8>>, client: BlockingClient) {
        let client_id = client.id.clone();
        self.blocked_list.insert(client.id.clone(), client);
        for key in key {
            self.waiters.entry(key).or_default().push_front(client_id)
        }
    }

    pub fn wake_one(&mut self, key: &[u8], value: Vec<u8>) -> bool {
        let client_id = {
            if let Some(queue) = self.waiters.get_mut(key) {
                queue.pop_back()
            } else {
                None
            }
        };

        let client_id = match client_id {
            Some(id) => id,
            None => return false,
        };

        if let Some(client) = self.blocked_list.remove(&client_id) {
            for q in self.waiters.values_mut() {
                q.retain(|id| *id != client_id);
            }

            let _ = client.response_tx.send(Resp::Array(vec![
                Resp::BulkString(key.to_vec()),
                Resp::BulkString(value),
            ]));

            if let Some(queue) = self.waiters.get(key) {
                if queue.is_empty() {
                    self.waiters.remove(key);
                }
            }

            return true;
        }

        false
    }
}
