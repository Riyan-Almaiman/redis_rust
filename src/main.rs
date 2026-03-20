#![allow(unused_imports)]

extern crate core;

mod resp;

mod parser;

mod commands;

mod connection;

mod lists;

mod blocking_list;
mod blocking_stream;
mod db;
mod db_commands;
mod stream;
mod valuetype;
mod xrange;

use std::env;

use rand::distr::Alphanumeric;
use rand::{RngExt, TryRng, rng};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use tokio::net::{TcpListener, TcpStream};

use tokio::sync::mpsc;

use uuid::Uuid;

use crate::commands::RedisCommand;

use crate::db::{DB, Master, Role};

use crate::db::Client;

use crate::parser::Parser;

use crate::resp::Resp;

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]

async fn main() {
    let args: Vec<String> = env::args().collect();
    let ip_address = "127.0.0.1".to_string();
    let mut port = "6379".to_string();
    let mut replica_master = String::new();
    for i in 0..args.len() {
        if args[i] == "--port" && i + 1 < args.len() {
            port = args[i + 1].clone();
        }
        if args[i] == "--replicaof" && i + 1 < args.len() {
            replica_master = args[i + 1].clone();
        }
    }
    let role = if replica_master.is_empty() {
        let mut id = [0u8; 40];
        rand::rng().try_fill_bytes(&mut id);

           Role::Master { replication_id: String::from_utf8(id.to_vec()).unwrap(), replication_offset: 0 }
    }else{
        Role::Slave { master: replica_master }
    };
    let listener = TcpListener::bind(format!("{}:{}", ip_address, port))
        .await
        .expect("Failed to bind");

    let mut db = DB::new(role);

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

        while let Some(mut cmd_resp) = parser.parse() {
            if let Resp::Array(ref mut arr) = cmd_resp {
                let cmd_name = arr.pop_front();
                if let Some(cmd) = cmd_name {
                    let cmd_name_bytes = Resp::get_bytes(&cmd).unwrap();
                    let cmd_name = std::str::from_utf8(cmd_name_bytes.as_ref())
                        .expect("Invalid UTF-8 in command");

                    let args: Vec<&str> = arr
                        .iter()
                        .map(|a| {
                            let bytes: &[u8] = Resp::get_bytes(a).unwrap();
                            std::str::from_utf8(bytes).expect("Invalid UTF-8")
                        })
                        .collect();

                    let command = match RedisCommand::from_parts(cmd_name, &args) {
                        Ok(cmd) => cmd,
                        Err(e) => RedisCommand::Error(e),
                    };
                    commands.push(command);
                }
            }
        }

        execute_commands(&mut connection, connection_tx.clone(), uuid, commands).await;
    }
}

async fn execute_commands(
    stream: &mut TcpStream,
    tx: mpsc::Sender<Client>,
    id: Uuid,
    cmds: Vec<RedisCommand>,
) {
    let mut bytes_batch = Vec::with_capacity(4096);

    for parsed_command in cmds {
        let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();

        let client_req = Client {
            client_id: id,
            timeout: None,
            response_tx: resp_tx,
            command: parsed_command,
        };

        if tx.send(client_req).await.is_err() {
            return;
        }

        if let Ok(response) = resp_rx.await {
            response.write_format(&mut bytes_batch);
        }
    }

    if !bytes_batch.is_empty() {
        let _ = stream.write_all(&bytes_batch).await;
    }
}
