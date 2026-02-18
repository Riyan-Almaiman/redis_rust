#![allow(unused_imports)]

use std::io::{BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");


    let listener = match TcpListener::bind("127.0.0.1:6379") {
        Ok(listener) => {
            listener
        }
        Err(e) => {
            eprintln!("Failed to bind: {}", e);
            return;
        }
    };

    for mut stream in listener.incoming() {
        match &mut  stream {
            Ok( _stream) => {
                let mut buf = Vec::new();
                loop {
                    let mut read_buf = vec![0; 1024];

                    let n = _stream.read(&mut read_buf);
                    match n {
                        Ok(n) => {
                            buf.append(&mut read_buf[0..n-1].to_vec());
                            println!("{}", String::from_utf8_lossy(&buf));
                            if n == 0 {
                                break; // connection closed
                            }
                            if bytes_to_string(&buf).contains("PING") {
                                ping(_stream);
                            }
                        },
                        Err(e) => {
                            break;
                        }
                    }

                }

            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }

    fn bytes_to_string(bytes: &[u8]) -> String {
        String::from_utf8_lossy(bytes).to_string()
    }
    fn ping(stream: &mut TcpStream ){
        stream.write_all(b"+PONG\r\n").unwrap();
    }
}
