#![allow(unused_imports)]
extern crate core;

mod resp;
mod parser;
mod commands;
mod connection;
mod lists;
mod db;

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, Mutex};
use uuid::Uuid;
use crate::commands::RedisCommand;
use crate::db::{ DB};
use crate::lists::{Client, List};
use crate::parser::Parser;
use crate::resp::Resp;

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.expect("Failed to bind");

    let mut db = DB::new();
    let db_tx = db.sender.clone();

    tokio::spawn(async move {
        db.start().await;
    });

    loop {
        let (stream, _) = listener.accept().await.expect("Accept failed");
        let connection_tx = db_tx.clone();

        tokio::spawn(async move {
            handle_stream(stream, connection_tx, Uuid::new_v4()).await;
        });
    }
}


enum ValueType {
    String(Vec<u8>),
    List(List),
    Set,
    ZSet,
    Hash,
    Stream,
    VectorSet
}

struct KeyValue {
    expiry: Option<SystemTime>,
    value: ValueType,
}


async fn handle_stream(mut connection: TcpStream, connection_tx: Sender<Client>, uuid: Uuid) {
    let mut parser = Parser::new();
    let mut temp = [0u8; 1024];
    loop {

        let n = match connection.read(&mut temp).await {
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
                    execute_command(&mut connection, connection_tx.clone(), uuid, &cmds).await
                }
                _=> continue,
            } ,
            None => continue,
        }
    }
}



async fn execute_command(
    stream: &mut TcpStream,
    tx: mpsc::Sender<Client>, // Use the DB sender
    id: Uuid,
    cmds: &Vec<Vec<u8>>,
) {
    let parsed_command = RedisCommand::from_resp(cmds);

    match parsed_command {
        Ok(c) => {
            let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
            let client_req = Client {
                client_id: id,
                timeout: None,
                response_tx: resp_tx,
                command: c
            };

            let _ = tx.send(client_req).await;

            if let Ok(response) = resp_rx.await {
                let bytes = Resp::formate_cmd(&response);
                let _ = stream.write_all(&bytes).await;
            }
        }
        Err(_) => {
            let _ = stream.write_all(b"-ERR unknown command\r\n").await;
        }
    }
}

