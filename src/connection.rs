// use std::collections::HashMap;
// use std::sync::Arc;
// use tokio::net::TcpStream;
// use tokio::sync::{mpsc, Mutex};
// use uuid::Uuid;
// use crate::{KeyValue, ValueType};
// use crate::db::DB;
// use crate::lists::Client;
// use crate::resp::Resp;
//
// pub struct Connection {
//     pub tcp_stream: TcpStream,
//     pub id: Uuid
// }
// impl Connection {
//
//     fn handle_type_command(&self, key: &str) -> Resp {
//         match self.db.get(key) {
//             None => Resp::SimpleString("none".to_string().as_bytes().to_vec()),
//             Some(value) => match value {
//                 ValueType::String(_) => Resp::SimpleString("string".to_string().as_bytes().to_vec()),
//                 ValueType::List(_) => Resp::SimpleString("list".to_string().as_bytes().to_vec()),
//                 _ => panic!()
//             },
//         }
//     }
// }
