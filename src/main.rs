#![allow(unused_imports)]

extern crate core;

mod resp;

mod parser;

mod commands;

mod connection;

mod lists;

mod db;
mod stream;
mod valuetype;
mod xrange;

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
    let listener = TcpListener::bind("127.0.0.1:6379")
        .await
        .expect("Failed to bind");

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

        let mut commands = Vec::new();

        while let Some(cmd_resp) = parser.parse() {
            if let Resp::Array(ref arr) = cmd_resp {
                let cmds: Vec<Vec<u8>> = arr.iter().map(|item| Resp::get_bytes(item)).collect();

                commands.push(RedisCommand::from_resp(&*cmds));
            }
        }

        execute_commands(&mut connection, connection_tx.clone(), uuid, commands).await;
    }
}

async fn execute_commands(
    stream: &mut TcpStream,
    tx: mpsc::Sender<Client>,
    id: Uuid,
    cmds: Vec<Result<RedisCommand, String>>,
) {
    let mut bytes_batch = Vec::with_capacity(4096);

    for parsed_command in cmds {
        match parsed_command {
            Ok(c) => {
                let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
                let client_req = Client {
                    client_id: id,
                    timeout: None,
                    response_tx: resp_tx,
                    command: c,
                };

                if tx.send(client_req).await.is_err() {
                    return;
                }

                if let Ok(response) = resp_rx.await {
                    response.write_format(&mut bytes_batch);
                }
            }
            Err(e) => {
                bytes_batch.extend_from_slice(b"-ERR ");
                bytes_batch.extend_from_slice(e.as_bytes());
                bytes_batch.extend_from_slice(b"\r\n");
            }
        }
    }

    if !bytes_batch.is_empty() {
        let _ = stream.write_all(&bytes_batch).await;
    }
}
