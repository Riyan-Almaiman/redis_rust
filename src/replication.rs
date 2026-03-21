use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::{mpsc, oneshot},
};
use uuid::Uuid;

use crate::{
    commands_parser::RedisCommand,
    db::Client,
    parser::Parser,
    resp::Resp,
};

pub async fn start_replication(
    master_addr: String,
    db_tx: mpsc::Sender<Client>,
    port: String
) {
    println!("Connecting to master at {}", master_addr);

    let mut stream = TcpStream::connect(master_addr)
        .await
        .expect("Failed to connect to master");


    async fn send_command(stream: &mut TcpStream, cmd: Resp) {
        let mut buf = Vec::new();
        cmd.write_format(&mut buf);
        stream.write_all(&buf).await.unwrap();
    }

    async fn read_response(stream: &mut TcpStream, parser: &mut Parser) {
        let mut buffer = [0u8; 1024];

        let n = stream.read(&mut buffer).await.unwrap();
        if n == 0 {
            panic!("Master closed connection during handshake");
        }

        parser.read_buffer.extend_from_slice(&buffer[..n]);

        let _ = parser.parse();
    }

    let mut parser = Parser::new();

    send_command(
        &mut stream,
        Resp::Array([Resp::BulkString(b"PING".to_vec())].into()),
    )
        .await;
    read_response(&mut stream, &mut parser).await;

    send_command(
        &mut stream,
        Resp::Array(
            [
                Resp::BulkString(b"REPLCONF".to_vec()),
                Resp::BulkString(b"listening-port".to_vec()),
                Resp::BulkString(port.as_bytes().to_vec()),
            ]
                .into(),
        ),
    )
        .await;
    read_response(&mut stream, &mut parser).await;

    send_command(
        &mut stream,
        Resp::Array(
            [
                Resp::BulkString(b"REPLCONF".to_vec()),
                Resp::BulkString(b"capa".to_vec()),
                Resp::BulkString(b"psync2".to_vec()),
            ]
                .into(),
        ),
    )
        .await;
    read_response(&mut stream, &mut parser).await;

    send_command(
        &mut stream,
        Resp::Array(
            [
                Resp::BulkString(b"PSYNC".to_vec()),
                Resp::BulkString(b"?".to_vec()),
                Resp::BulkString(b"-1".to_vec()),
            ]
                .into(),
        ),
    )
        .await;



    let mut buffer = [0u8; 4096];

    loop {
        let n = match stream.read(&mut buffer).await {
            Ok(0) => {
                println!("Master disconnected");
                return;
            }
            Ok(n) => n,
            Err(e) => {
                println!("Read error: {:?}", e);
                return;
            }
        };

        parser.read_buffer.extend_from_slice(&buffer[..n]);

        while let Some(mut resp) = parser.parse() {
            match resp {
                Resp::SimpleString(s) => {
                    if s.starts_with(b"FULLRESYNC") {
                        println!("FULLRESYNC received");
                        continue;
                    }
                }

                Resp::BulkString(data) => {
                    println!("RDB received ({} bytes)", data.len());
                    continue;
                }

                Resp::Array(ref mut arr) => {
                    if arr.is_empty() {
                        continue;
                    }

                    let cmd_name = arr.pop_front().unwrap();
                    let cmd_name_bytes = match Resp::get_bytes(&cmd_name) {
                        Some(b) => b,
                        None => continue,
                    };

                    let cmd_name = match std::str::from_utf8(cmd_name_bytes) {
                        Ok(s) => s,
                        Err(_) => continue,
                    };

                    let args: Vec<&str> = arr
                        .iter()
                        .filter_map(|a| Resp::get_bytes(a))
                        .filter_map(|b| std::str::from_utf8(b).ok())
                        .collect();
                    for arg in &args {
                        println!("ARG: {} {}", arg, cmd_name);
                    }
                    let command = match RedisCommand::from_parts(cmd_name, &args) {
                        Ok(cmd) => cmd,
                        Err(e) => {
                            println!("Failed to parse command from master: {}", e);
                            continue;
                        }
                    };


                    let (tx, _rx) = oneshot::channel();

                let client = Client {
                    client_id: Uuid::new_v4(),
                    command,
                    resp_command: resp,
                    response_tx: tx,
                    response_tx_slave: None,
                    timeout: None,
                };

                if db_tx.send(client).await.is_err() {
                    println!("DB channel closed");
                    return;
                }
            }
                _ => {}}
            }
    }
}