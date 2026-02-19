#![allow(unused_imports)]

mod test;

use std::io::Read;
use std::iter::{Enumerate, Peekable};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() {
    let listener = match TcpListener::bind("127.0.0.1:6379").await {
        Ok(listener) => listener,
        Err(e) => {
            eprintln!("Failed to bind: {}", e);
            return;
        }
    };

    loop {
        let conn = listener.accept().await;

        match conn {
            Ok((stream, addr)) => {
                tokio::spawn(async move { handle_stream(stream).await });
            }
            Err(e) => {
                eprintln!("Failed to accept connection: {}", e);
            }
        }
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
async fn handle_stream(mut stream: TcpStream) {
    let mut buffer = [0u8; 1024];

    loop {
        let n = stream.read_exact(&mut buffer).await;
        match n {
            Ok(n) => {
                parse_buffer(&buffer[0..n], &mut stream).await;
            }

            Err(e) => {
                break;
            }
        }
    }
}
async fn parse_buffer(buffer: &[u8], stream: &mut TcpStream) {
    let mut current_step = None;
    let mut current = Vec::new();

    // CHANGE: Ensure this stores owned Vecs, not references
    let mut current_cmd: Vec<Vec<u8>> = Vec::new();

    let mut iter = buffer.iter().enumerate().peekable();
    let mut arg_count = 0;
    let mut args_read = 0;

    while let Some((i, ch)) = iter.next() {
        let ch = *ch;// Destructure *ch here to get u8
        if ch == b'*' {
            current_step = Some(ParseStep::Star);
            current.clear(); // Ensure scratchpad is clean
            continue;
        }

        match current_step {
            Some(ParseStep::Star) => {
                current.push(ch);
                if current.ends_with(b"\r\n") {
                    if let Some(n) = parse_number(&current[..current.len() - 2]) {
                        arg_count = n;
                        current.clear();
                        current_step = Some(ParseStep::ArgCount);
                    } else {
                        current.clear();
                        current_cmd.clear();
                        current_step = None;
                    }
                }
            },
            Some(ParseStep::ArgCount) => match ch {
                b'$' => {
                    current_step = Some(ParseStep::ArgLength);
                    current.clear();
                }
                b'+' => {
                    current_step = Some(ParseStep::SimpleString);
                    current.clear();
                }
                _ => {
                    current.clear();
                    current_cmd.clear();
                    current_step = None;
                }
            },
            Some(ParseStep::SimpleString) => {
                current.push(ch);
                if current.ends_with(b"\r\n") {
                    // FIX: Push the owned Vec, no &mut needed
                    current_cmd.push(current[..current.len() - 2].to_vec());
                    current.clear();
                    args_read += 1;

                    // Logic check: if this was the last arg, execute
                    if args_read == arg_count {
                        execute_command(stream, &current_cmd).await;
                        current_cmd.clear();
                        args_read = 0;
                        current_step = None;
                    } else {
                        current_step = Some(ParseStep::ArgCount);
                    }
                }
            },
            Some(ParseStep::ArgLength) => {
                current.push(ch);
                if current.ends_with(b"\r\n") {
                    if let Some(n) = parse_number(&current[..current.len() - 2]) {
                        current_step = Some(ParseStep::Arg(n as usize));
                        current.clear();
                    } else {
                        current_cmd.clear();
                        current.clear();
                        current_step = None;
                    }
                }
            },
            Some(ParseStep::Arg(n)) => {
                current.push(ch);
                if current.len() == n + 2 {
                    if current.ends_with(b"\r\n") {
                        // FIX: Push the owned Vec directly
                        current_cmd.push(current[..n].to_vec());

                        args_read += 1;
                        current.clear();

                        if args_read == arg_count {
                            execute_command(stream, &current_cmd).await;
                            current_cmd.clear();
                            args_read = 0;
                            current_step = None;
                        } else {
                            current_step = Some(ParseStep::ArgCount);
                        }
                    } else {
                        current_cmd.clear();
                        current.clear();
                        current_step = None;
                    }
                }
            }
            None => {}
        }
    }
}

    async fn execute_command(stream: &mut TcpStream, cmds: &Vec<Vec<u8>>) {
        match bytes_to_string(&cmds[0]).to_ascii_lowercase().as_str() {
            "echo" => {
                write_echo(stream, &cmds[1]).await;
            }
            "ping" => {
                ping(stream).await;
            }
            _ => {}
        }
    }

fn parse_number(data: &[u8]) -> Option<i32> {
    let number_string = std::str::from_utf8(data);
    match number_string {
        Ok(s) => match s.parse() {
            Ok(n) => Some(n),
            Err(_) => None,
        },
        Err(e) => None,
    }
}
async fn write_echo(stream: &mut TcpStream, message: &[u8]) {
    let mut response = Vec::new();
    let clrf = b"\r\n";

    let mut message_length = message.len().to_string().as_str().as_bytes().to_vec();
    response.push(b'$');
    response.append(&mut message_length);
    response.append(clrf.to_vec().as_mut());
    response.append(message.to_vec().as_mut());
    response.append(clrf.to_vec().as_mut());

    if stream.write_all(&response).await.is_err() {
        println!("Failed to ECHO");
    };
}
fn bytes_to_string(bytes: &[u8]) -> String {
    String::from_utf8_lossy(&bytes).to_string()
}
async fn ping(stream: &mut TcpStream) {
    if stream.write_all(b"+PONG\r\n").await.is_err() {
        println!("Failed to send PONG");
    };
}
