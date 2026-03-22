use std::collections::{HashMap, VecDeque};
use tokio::sync::mpsc::UnboundedSender;
use uuid::Uuid;

#[derive(Default)]
pub struct BlockingStreams {
    pub waiters: HashMap<Vec<u8>, VecDeque<Uuid>>,
    pub clients: HashMap<Uuid, BlockingStreamClient>,
}
#[derive(Clone, Debug)]

pub struct StreamWait {
    pub key: Vec<u8>,
    pub time: u64,
    pub sequence: u64,
}

pub struct BlockingStreamClient {
    pub id: Uuid,
    pub response_tx: UnboundedSender<Vec<u8>>,
    pub waits: Vec<StreamWait>,
}
