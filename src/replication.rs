use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::{mpsc, oneshot},
};
use uuid::Uuid;

use crate::{commands_parser::RedisCommand, db::Client, parser::Parser, resp::Resp};

pub async fn start_replication(master_addr: String, db_tx: mpsc::Sender<Client>, port: String) {
    println!("Connecting to master at {}", master_addr);

    let stream = TcpStream::connect(master_addr)
        .await
        .expect("Failed to connect to master");

    let (mut reader, mut writer) = stream.into_split();

    let mut parser = Parser::new();

    async fn send(writer: &mut tokio::net::tcp::OwnedWriteHalf, resp: Resp) {
        let mut buf = Vec::new();
        resp.write_format(&mut buf);
        writer.write_all(&buf).await.unwrap();
    }
    async fn read_one(reader: &mut tokio::net::tcp::OwnedReadHalf, parser: &mut Parser) {
        let mut buffer = [0u8; 1024];

        let n = reader.read(&mut buffer).await.unwrap();
        if n == 0 {
            panic!("Master closed connection during handshake");
        }

        parser.read_buffer.extend_from_slice(&buffer[..n]);
        let _ = parser.parse();
    }
    send(
        &mut writer,
        Resp::Array([Resp::BulkString(b"PING".to_vec())].into()),
    )
    .await;
    read_one(&mut reader, &mut parser).await;

    send(        &mut writer,

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
    read_one(&mut reader, &mut parser).await;

    send(
        &mut writer,
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
    read_one(&mut reader, &mut parser).await;

    send(
        &mut writer,
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
    let (tx, mut rx) = mpsc::unbounded_channel::<Vec<u8>>();

    loop {

        let n = match reader.read(&mut buffer).await {
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
                    continue;
                }

                Resp::Array(ref mut arr) => {
                    if arr.is_empty() {
                        continue;
                    }

                    let cmd_name = arr.pop_front().unwrap();
                    let cmd_name = match Resp::get_bytes(&cmd_name)
                        .and_then(|b| std::str::from_utf8(b).ok())
                    {
                        Some(s) => s.to_lowercase(),
                        None => continue,
                    };

                    let args: Vec<String> = arr
                        .iter()
                        .filter_map(|a| Resp::get_bytes(a))
                        .filter_map(|b| std::str::from_utf8(b).ok())
                        .map(|s| s.to_string())
                        .collect();
                    println!("FROM MASTER: {} {:?}", cmd_name, args);

                    if cmd_name == "replconf" {
                        println!("REPLCONF received");
                        if args.len() >= 2 && args[0] .to_lowercase()== "getack" && args[1] == "*" {
                            let response = Resp::Array(
                                vec![
                                    Resp::BulkString(b"REPLCONF".to_vec()),
                                    Resp::BulkString(b"ACK".to_vec()),
                                    Resp::BulkString(b"0".to_vec()),
                                ]
                                .into(),
                            );

                            let mut buf = Vec::new();
                            response.write_format(&mut buf);

                            writer.write_all(&buf).await.unwrap();
                            continue;
                        }
                    }

                    let command = match RedisCommand::from_parts(
                        &cmd_name,
                        &args.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                    ) {
                        Ok(cmd) => cmd,
                        Err(e) => {
                            println!("Failed to parse command: {}", e);
                            continue;
                        }
                    };

                    let client = Client {
                        client_id: Uuid::new_v4(),
                        command,
                        resp_command: resp,
                        response_tx: tx.clone(),
                        timeout: None,
                    };

                    if db_tx.send(client).await.is_err() {
                        println!("DB channel closed");
                        return;
                    }
                }

                _ => {}
            }
        }
    }
}
