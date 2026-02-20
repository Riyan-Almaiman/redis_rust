#![allow(unused_imports)]
extern crate core;

mod resp;
mod parser;
mod commands;
mod connection;
mod lists;

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::Sender;
use tokio::sync::Mutex;
use uuid::Uuid;
use crate::commands::RedisCommand;
use crate::connection::{Connection };
use crate::lists::{Client, Lists};
use crate::parser::Parser;
use crate::resp::Resp;

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379")
        .await
        .expect("Failed to bind");
    let db: Arc<Mutex<HashMap<Vec<u8>, KeyValue>>> = Arc::new(Mutex::new(HashMap::new()));
    let mut lists = Lists::new();
    let sender = lists.sender.clone();
    tokio::spawn(async move {
        lists.start().await;
    });
    loop {
        let (stream, _) = listener.accept().await.expect("Accept failed");
        let db_copy = Arc::clone(&db);

        let connection = Connection {
            tcp_stream: stream,
            db: db_copy,
            list_pipeline_tx: sender.clone(),
            id: Uuid::new_v4()
        };

        tokio::spawn(async move {
            handle_stream(connection).await;
        });
    }
}


#[derive(Clone)]

enum ValueType {
    String(Vec<u8>),
    List(VecDeque<Vec<u8>>)
}

#[derive(Clone)]
struct KeyValue {
    expiry: Option<SystemTime>,
    value: ValueType,
}

async fn handle_stream(mut connection: Connection) {
    let mut parser = Parser::new();
    let mut temp = [0u8; 1024];
    loop {

        let n = match connection.tcp_stream.read(&mut temp).await {
            Ok(0) => return,
            Ok(n) => n,
            Err(_) => return,
        };

        parser.read_buffer.extend(&temp[..n]);
        let cmd =parser.parse();
        if let Some(Resp::Array(elements)) = &cmd {
            let readable: Vec<String> = elements.iter().map(|el| {
                match el {
                    Resp::BulkString(bytes) => String::from_utf8_lossy(bytes).into_owned(),
                    _ => format!("{:?}", el),
                }
            }).collect();

        }
        match cmd {
            Some(cmd) => match cmd{
                Resp::Array( arr) => {
                    let mut cmds : Vec<Vec<u8>> = Vec::new();
                    for cmd in arr {
                        cmds.push(Resp::get_bytes(&cmd));
                    }
                    execute_command(&mut connection.tcp_stream, connection.id.clone(), &cmds, &connection.db, &mut connection.list_pipeline_tx).await
                }
                _=> continue,
            } ,
            None => continue,
        }
    }
}



async fn execute_command(
    stream: &mut TcpStream,
    id: uuid::Uuid,
    cmds: &Vec<Vec<u8>>,
    values: &Arc<Mutex<HashMap<Vec<u8>, KeyValue>>>,
    tx: &mut Sender<Client>,
) {
    if cmds.len() == 0 {
        return;
    }

    let cmd = cmds[0]
        .iter()
        .map(|b| b.to_ascii_lowercase())
        .collect::<Vec<_>>();
    let parsed_command = RedisCommand::from_resp(cmds);
    match parsed_command {
        Ok(c) => {
            let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
            let client_req = Client {
                client_id: id,
                timeout: Some(Duration::from_secs_f64(0f64)),
                response_tx: resp_tx,
                command:  c
            };
            tx.send(client_req).await.expect("REASON");
            if let Ok(response) = resp_rx.await {

                stream.write_all(&*Resp::formate_cmd(&response)).await.expect("TODO: panic message");
            }
        }
        Err(e) => {
            match cmd.as_slice() {
                b"echo" => write_echo(stream, &cmds[1]).await,
                b"ping" => ping(stream).await,
                b"set" =>{
                    set(stream, &cmds[1..], values).await
                } ,
                b"get" => get(stream, &cmds[1..], values).await,

                _ => {}
            }
        }
    }


}

async fn set(
    stream: &mut TcpStream,
    message: &[Vec<u8>],
    values: &Arc<Mutex<HashMap<Vec<u8>, KeyValue>>>,
) {
    if message.len() < 2 {
        return;
    }

    let time_now = SystemTime::now();
    let mut expiry = None;
    if message.len() == 4 {
        let time_value = message.get(3).unwrap();
        let time_type = message.get(2).unwrap();
        let number = match std::str::from_utf8(time_value)
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
        {
            Some(n) => n,
            None => return,
        };
        let opt = time_type
            .iter()
            .map(|b| b.to_ascii_lowercase())
            .collect::<Vec<_>>();

        match opt.as_slice() {
            b"ex" => expiry = time_now.checked_add(Duration::from_secs(number as u64)),
            b"px" => expiry = time_now.checked_add(Duration::from_millis(number as u64)),
            _ => {}
        }
    }
    let value = KeyValue {
        value: ValueType::String(message[1].clone()),
        expiry,
    };
    {
        let mut v = values.lock().await;
        v.insert(message[0].clone(), value);
    }
    if stream.write_all(b"+OK\r\n").await.is_err() {
        return;
    }
}
async fn get(
    stream: &mut TcpStream,
    message: &[Vec<u8>],
    values: &Arc<Mutex<HashMap<Vec<u8>, KeyValue>>>,
) {
    let get_value = {
        let db = values.lock().await;
        db.get(&message[0]).cloned()
    };
    let value = match get_value {
        Some(value) => value,
        None => {
            if stream.write_all(b"$-1\r\n").await.is_err() {
                return;
            }
            return;
        }
    };
    let val_str = match value.value {
        ValueType::String(s) => s,
        _ => return,
    };
    match value.expiry {
        Some(expiry) => {
            if expiry < SystemTime::now() {
                {
                    let mut db = values.lock().await;
                    db.remove(&message[0]);
                }
                if stream.write_all(b"$-1\r\n").await.is_err() {
                    return;
                }
            } else {
                write_echo(stream, &*val_str).await
            }
        }
        None => write_echo(stream, &*val_str).await,
    }
}


async fn write_echo(stream: &mut TcpStream, message: &[u8]) {
    let response = format!("${}\r\n", message.len()).into_bytes();
    let mut out = response;
    out.extend_from_slice(message);
    out.extend_from_slice(b"\r\n");
    let _ = stream.write_all(&out).await;
}
async fn write_array( stream: &mut TcpStream,message: Vec<&[u8]>) {
    let mut response = format!("*{}\r\n", message.len()).into_bytes();
    for m in message {
        let mut r = format!("${}\r\n", m.len()).into_bytes();
        r.extend_from_slice(m);
        r.extend_from_slice(b"\r\n");
        response.extend_from_slice(&r);
    }
    let _ = stream.write_all(&response).await;

}


async fn ping(stream: &mut TcpStream) {
    let _ = stream.write_all(b"+PONG\r\n").await;
}
