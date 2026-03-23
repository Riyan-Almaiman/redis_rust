#![allow(unused_imports)]

extern crate core;

mod resp;

mod parser;

mod commands_parser;
mod geo_list;
mod lists;
mod sorted_list;
mod blocking_list;
mod blocking_manger;
mod blocking_stream;
mod command_router;
mod commands;
mod db;
mod replication;
mod role;
mod send;
mod stream;
mod valuetype;
use base64::Engine;
use core::panic;
use rand::{Rng, RngExt, TryRng};
use std::any::Any;
use std::env;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use tokio::net::{TcpListener, TcpStream};

use tokio::sync::mpsc;

use uuid::Uuid;

use crate::commands_parser::RedisCommand;

use crate::db::DB;

use crate::db::Client;

use crate::parser::Parser;

use crate::resp::Resp;
use crate::role::Role;
use rand::distr::{Alphanumeric, SampleString};
use rdb::{RdbError, RdbResult};

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]

async fn main() {
    let args: Vec<String> = env::args().collect();
    const EMPTY_RDB_BASE64: &str = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";
    let ip_address = "localhost".to_string();
    let mut server_port = "6379".to_string();
    let mut replica_master = String::new();
    let mut dir = String::from("");
    let mut file_name = String::from("db.rdb");
    for i in 0..args.len() {
        if args[i] == "--port" && i + 1 < args.len() {
            server_port = args[i + 1].clone();
        }
        if args[i] == "--replicaof" && i + 1 < args.len() {
            replica_master = args[i + 1].clone();
        }
        if args[i] == "--dir" && i + 1 < args.len() {
            dir = args[i + 1].clone();
        }
        if args[i] == "--dbfilename" && i + 1 < args.len() {
            file_name = args[i + 1].clone();
        }
    }




    let role = if replica_master.is_empty() {
        let id = Alphanumeric.sample_string(&mut rand::rng(), 40);

        Role::Master {
            replication_id: id,
            replication_offset: 0,
        }
    } else {
        let mut conn = replica_master.split_whitespace();
        let ip = conn.next();
        let port = conn.next();

        let id: String = Alphanumeric.sample_string(&mut rand::rng(), 40);

        if let (Some(ip), Some(port)) = (ip, port) {
            Role::Slave {
                port: server_port.clone(),
                master: format!("{}:{}", ip, port),
                replication_id: id,
                replication_offset: 0,
            }
        } else {
            panic!("invalid replicaof format");
        }
    };

    let role_for_replication = role.clone();

    let listener: TcpListener = TcpListener::bind(format!("{}:{}", ip_address, server_port))
        .await
        .expect("Failed to bind");

    let mut db = DB::new(role, dir, file_name).await;
    let db_tx = db.sender.clone();

    // DB loop
    tokio::spawn(async move {
        db.start().await;
    });

    if let Role::Slave { master, .. } = role_for_replication {
        let tx = db_tx.clone();

        tokio::spawn(async move {
            replication::start_replication(master, tx, server_port.clone()).await;
        });
    }

    loop {
        let (stream, _) = listener.accept().await.expect("Accept failed");

        let connection_tx = db_tx.clone();

        tokio::spawn(async move {
            handle_stream(stream, connection_tx, Uuid::new_v4()).await;
        });
    }
}
async fn handle_stream(connection: TcpStream, connection_tx: mpsc::Sender<Client>, uuid: Uuid) {
    let mut parser = Parser::new();
    let (mut reader, mut writer) = connection.into_split();
    let mut buffer = [0u8; 1024];

    let (conn_tx, mut conn_rx) = mpsc::unbounded_channel::<Vec<u8>>();
    tokio::spawn(async move {
        while let Some(data) = conn_rx.recv().await {
            let _ = writer.write_all(&data).await;
        }
    });
    loop {
        let n = match reader.read(&mut buffer).await {
            Ok(0) => return,

            Ok(n) => n,

            Err(_) => return,
        };

        parser.read_buffer.extend_from_slice(&buffer[..n]);

        let mut commands = Vec::new();
        let mut resp_commands = Vec::new();
        while let Some(mut cmd_resp) = parser.parse() {
            resp_commands.push(cmd_resp.clone());

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

                    let command = RedisCommand::from_parts(cmd_name, &args)
                        .unwrap_or_else(|e| RedisCommand::Error(e));
                    commands.push(command);
                }
            }
        }
        for (i, parsed_command) in commands.iter().enumerate() {
            let client_req = Client {
                client_id: uuid,
                timeout: None,
                response_tx: conn_tx.clone(),
                resp_command: resp_commands[i].clone(),
                command: parsed_command.clone(),
            };

            if connection_tx.send(client_req).await.is_err() {
                return;
            }
        }
    }
}
