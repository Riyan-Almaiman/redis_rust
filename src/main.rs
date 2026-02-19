#![allow(unused_imports)]

use std::collections::{HashMap, HashSet};
use std::fmt::format;
use std::sync::{Arc};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{ Mutex};
#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379")
        .await
        .expect("Failed to bind");
    let  db: Arc<Mutex<HashMap<Vec<u8>, Vec<u8>>>> = Arc::new(Mutex::new(HashMap::new()));

    loop {
        let (stream, _) = listener.accept().await.expect("Accept failed");
        let db_copy = Arc::clone(&db);
        tokio::spawn(async move {
            handle_stream( stream, db_copy).await;
        });
    }
}

#[derive(PartialEq)]
enum ParseStep {
    Star,
    ArgCount,
    SimpleString,
    ArgLength,
    Arg(usize),
}
struct KeyValuePair{
    key: String,
    value: String,
}
async fn handle_stream(mut stream: TcpStream, mut values: Arc<Mutex<HashMap<Vec<u8>, Vec<u8>>>>) {
    let mut read_buffer: Vec<u8> = Vec::new();
    let mut temp = [0u8; 1024];
    let mut current_step: Option<ParseStep> = None;
    let mut current = Vec::new();
    let mut current_cmd: Vec<Vec<u8>> = Vec::new();
    let mut arg_count = 0;
    let mut args_read = 0;

    loop {
        let n = match stream.read(&mut temp).await {
            Ok(0) => return,
            Ok(n) => n,
            Err(_) => return,
        };

        read_buffer.extend_from_slice(&temp[..n]);

        let mut i = 0;

        while i < read_buffer.len() {
            let ch = read_buffer[i];

            if ch == b'*' {
                current_step = Some(ParseStep::Star);
                current.clear();
                arg_count = 0;
                args_read = 0;
                i += 1;
                continue;
            }

            match current_step {
                Some(ParseStep::Star) => {
                    current.push(ch);
                    i += 1;

                    if current.ends_with(b"\r\n") {
                        if let Some(n) = parse_number(&current[..current.len() - 2]) {
                            arg_count = n;
                            current.clear();
                            current_step = Some(ParseStep::ArgCount);
                        } else {
                            reset(&mut current_step, &mut current, &mut current_cmd);
                        }
                    }
                }

                Some(ParseStep::ArgCount) => {
                    match ch {
                        b'$' => {
                            current_step = Some(ParseStep::ArgLength);
                            current.clear();
                            i += 1;
                        }
                        b'+' => {
                            current_step = Some(ParseStep::SimpleString);
                            current.clear();
                            i += 1;
                        }
                        _ => {
                            reset(&mut current_step, &mut current, &mut current_cmd);
                            i += 1;
                        }
                    }
                }

                Some(ParseStep::SimpleString) => {
                    current.push(ch);
                    i += 1;

                    if current.ends_with(b"\r\n") {
                        current_cmd.push(current[..current.len() - 2].to_vec());
                        current.clear();
                        args_read += 1;

                        if args_read == arg_count {
                            execute_command( &mut stream, &current_cmd,  &mut values).await;
                            reset(&mut current_step, &mut current, &mut current_cmd);
                            args_read = 0;
                        } else {
                            current_step = Some(ParseStep::ArgCount);
                        }
                    }
                }

                Some(ParseStep::ArgLength) => {
                    current.push(ch);
                    i += 1;

                    if current.ends_with(b"\r\n") {
                        if let Some(n) = parse_number(&current[..current.len() - 2]) {
                            current_step = Some(ParseStep::Arg(n as usize));
                            current.clear();
                        } else {
                            reset(&mut current_step, &mut current, &mut current_cmd);
                        }
                    }
                }

                Some(ParseStep::Arg(n)) => {
                    current.push(ch);
                    i += 1;

                    if current.len() == n + 2 {
                        if current.ends_with(b"\r\n") {
                            current_cmd.push(current[..n].to_vec());
                            current.clear();
                            args_read += 1;

                            if args_read == arg_count {
                                execute_command( &mut stream, &current_cmd, &mut values).await;
                                reset(&mut current_step, &mut current, &mut current_cmd);
                                args_read = 0;
                            } else {
                                current_step = Some(ParseStep::ArgCount);
                            }
                        } else {
                            reset(&mut current_step, &mut current, &mut current_cmd);
                        }
                    }
                }

                None => {
                    i += 1;
                }
            }
        }

        read_buffer.clear();
    }
}

fn reset(
    step: &mut Option<ParseStep>,
    current: &mut Vec<u8>,
    cmd: &mut Vec<Vec<u8>>,
) {
    *step = None;
    current.clear();
    cmd.clear();
}

async fn execute_command(stream: &mut TcpStream, cmds: &Vec<Vec<u8>>, values: &mut Arc<Mutex<HashMap<Vec<u8>, Vec<u8>>>>) {
    match bytes_to_string(&cmds[0]).to_ascii_lowercase().as_str() {
        "echo" => write_echo(stream, &cmds[1]).await,
        "ping" => ping(stream).await,
        "set" => set(stream, &cmds[1..], values).await,
        "get" => get(stream, &cmds[1..], values).await,

        _ => {}
    }
}
async  fn set (stream: &mut TcpStream, message: &[Vec<u8>], values: &mut Arc<Mutex<HashMap<Vec<u8>, Vec<u8>>>>) {
    if message.len() != 2 {
        return;
    }
    let mut v =  values.lock().await;

            v.insert(message[0].clone(), message[1].clone());
            stream.write_all("+OK\r\n".to_string().as_bytes()).await.unwrap();


      

}
async  fn get (stream: &mut TcpStream, message: &[Vec<u8>], values: &mut Arc<Mutex<HashMap<Vec<u8>, Vec<u8>>>>) {
    let mut v =  values.lock().await;
     
            let value =  v.get(&message[0]);
            match value {
                Some(val) => write_echo(stream, val).await,
                None => { stream.write_all("$-1\r\n".to_string().as_bytes()).await.unwrap(); }
            }
}
fn parse_number(data: &[u8]) -> Option<i32> {
    std::str::from_utf8(data).ok()?.parse().ok()
}

async fn write_echo(stream: &mut TcpStream, message: &[u8]) {
    let response = format!("${}\r\n", message.len()).into_bytes();
    let mut out = response;
    out.extend_from_slice(message);
    out.extend_from_slice(b"\r\n");
    let _ = stream.write_all(&out).await;
}

fn bytes_to_string(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).to_string()
}

async fn ping(stream: &mut TcpStream) {
    let _ = stream.write_all(b"+PONG\r\n").await;
}
