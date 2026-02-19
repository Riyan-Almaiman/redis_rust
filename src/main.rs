#![allow(unused_imports)]

use std::env::args;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

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
    Parse,
}
async fn handle_stream(mut stream: TcpStream) {
    let mut buf: Vec<Vec<u8>> = Vec::new();
    let mut byte = [0u8; 1];

    let mut arg_count = 0;
    let mut current = Vec::new();
    let mut current_step = None;
    let mut args_read = 0;
    loop {
        if current_step == Some(ParseStep::Parse) {

            match bytes_to_string(&buf[0]).to_ascii_lowercase().as_str() {
                "echo" => {
                    write_echo(&mut stream, &buf[1]).await;
                    buf.clear();
                    current.clear();
                    current_step = None;
                }
                "ping" => {
                    ping(&mut stream).await;
                    buf.clear();
                    current.clear();
                    current_step = None;
                }
                _ => {
                    buf.clear();
                    current.clear();
                    current_step = None;
                }
            }
        }
        let n = stream.read_exact(&mut byte).await;
        println!("current buf {}", bytes_to_string(&buf.iter().flatten().cloned().collect::<Vec<_>>()));
        match n {
            Ok(n) => {
                if byte[0] == b'*' {
                    current_step = Some(ParseStep::Star);
                    arg_count = 0;
                    args_read = 0;
                    continue;
                }
                if current_step == Some(ParseStep::Parse) {
                    println!("reached parse step ")
                }
                match current_step {

                    Some(ParseStep::Star) => {
                        println!("in star");
                        current.push(byte[0]);

                        if current.ends_with(b"\r\n") {
                            match parse_number(&current[..current.len() - 2]) {
                                Some(n) => {
                                    arg_count = n;
                                    current.clear();
                                    current_step = Some(ParseStep::ArgCount);
                                    println!("Arg count: {}", arg_count);
                                }
                                None => {
                                    buf.clear();
                                    current.clear();
                                    current_step = None;
                                }
                            }
                        }
                    }
                    Some(ParseStep::ArgCount) => match byte[0]{

                        b'$' => {
                            println!("in arg count");

                            current_step = Some(ParseStep::ArgLength);

                            current.clear();
                        },
                        b'+' => {
                            println!("in arg count");

                            current_step = Some(ParseStep::SimpleString);

                            current.clear();
                        },
                        _ => {
                            println!("in arg count clear");

                            buf.clear();
                            current.clear();
                            current_step = None;
                        }
                    },
                    Some(ParseStep::SimpleString) => {
                        println!("in simple string");

                        if current.ends_with(b"\r\n") {
                            buf.push(current[..current.len() - 2].to_vec());
                            current.clear();
                            current.push(byte[0]);
                            args_read += 1;
                        } else {
                            current.push(byte[0]);
                        }
                    }
                    Some(ParseStep::ArgLength) => {
                        println!("arg length");
                        current.push(byte[0]);

                        if current.ends_with(b"\r\n") {

                            match parse_number(&current[..current.len() - 2]) {
                                Some(n) => {

                                    current_step = Some(ParseStep::Arg(n as usize));
                                    current.clear();


                                }
                                None => {
                                    buf.clear();
                                    current.clear();
                                    current_step = None;
                                }
                            }
                        }
                    }
                    Some(ParseStep::Arg(n)) => {

                        current.push(byte[0]);

                        if current.len() == n+2{
                            if current.ends_with(b"\r\n"){
                                println!("found {}", bytes_to_string(&current));
                                buf.push(current[..current.len() - 2].to_vec());
                                args_read+=1;
                                current.clear();
                                println!("arg count: {}", arg_count);
                                println!("arg len: {}", args_read);
                                if args_read == arg_count {
                                    println!("setting parse: {}", arg_count);
                                    current_step = Some(ParseStep::Parse);

                                }else{
                                    current_step = Some(ParseStep::ArgCount);


                                }
                            }
                            else{
                                buf.clear();
                                current.clear();
                                current_step = None;
                            }
                        }


                    },
                    Some(ParseStep::Parse) => {
                         current_step =  None;


                    }
                    None => {
                        println!("in none");

                    }
                }
            }
            Err(e) => {
                println!("in error");

                break;
            }
        }
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
