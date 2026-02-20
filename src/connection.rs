use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Mutex};
use uuid::Uuid;
use crate::KeyValue;
use crate::lists::Client;

pub struct Connection {
    pub tcp_stream: TcpStream,
    pub db: Arc<Mutex<HashMap<Vec<u8>, KeyValue>>>,
    pub list_pipeline_tx: mpsc::Sender<Client>,
    pub id: Uuid
}