use std::collections::{HashMap, VecDeque};
use tokio::sync::oneshot;
use uuid::Uuid;
use crate::blocking_list::BlockingClient;
use crate::resp::Resp;

pub struct BlockingStreams {
    pub waiters: HashMap<Vec<u8>, VecDeque<Uuid>>,
    pub clients: HashMap<Uuid, BlockingStreamClient>,
}#[derive(Clone, Debug)]

pub struct StreamWait {
    pub key: Vec<u8>,
    pub time: u64,
    pub sequence: u64,
}

pub struct BlockingStreamClient {
    pub id: Uuid,
    pub response_tx: oneshot::Sender<Resp>,
    pub waits: Vec<StreamWait>,
}

