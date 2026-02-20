#![allow(unused_imports)]
extern crate core;

mod resp;
mod parser;
mod commands;
mod connection;
mod lists;
mod db;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::commands::RedisCommand;
use crate::db::DB;
use crate::db::Client;
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

// main.rs
async fn handle_stream(mut connection: TcpStream, connection_tx: mpsc::Sender<Client>, uuid: Uuid) {
    let mut parser = Parser::new();
    let mut buffer = [0u8; 1024];

    loop {
        let n = match connection.read(&mut buffer).await {
            Ok(0) => return,
            Ok(n) => n,
            Err(_) => return,
        };

        parser.read_buffer.extend_from_slice(&buffer[..n]);

        while let Some(cmd_resp) = parser.parse() {
            if let Resp::Array(arr) = cmd_resp {
                let mut cmds: Vec<Vec<u8>> = arr.iter()
                    .map(|item| Resp::get_bytes(item))
                    .collect();

                execute_command(&mut connection, connection_tx.clone(), uuid, &cmds).await;
            }
        }
    }
}

async fn execute_command(
    stream: &mut TcpStream,
    tx: mpsc::Sender<Client>,
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

            if let Err(_) = tx.send(client_req).await {
                return;
            }

            if let Ok(response) = resp_rx.await {
                let bytes = Resp::formate_cmd(&response);
                let _ = stream.write_all(&bytes).await;
            }
        }
        Err(e) => {
            let error_msg = format!("-ERR {}\r\n", e);
            let _ = stream.write_all(error_msg.as_bytes()).await;
        }
    }
}