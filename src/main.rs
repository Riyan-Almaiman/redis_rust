#![allow(unused_imports)]

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
            Ok(( stream, addr)) => {
                tokio::spawn(async move { handle_stream(stream).await });
            }
            Err(e) => {
                eprintln!("Failed to accept connection: {}", e);
            }
        }
    }


    async fn handle_stream(mut stream: TcpStream) {
        let mut buf = Vec::new();
        loop {
            let mut read_buf = vec![0; 1024];

            let n = stream.read(&mut read_buf).await;
            match n {
                Ok(n) => {
                    buf.append(&mut read_buf[0..n].to_vec());
                    println!("{}", String::from_utf8_lossy(&buf));
                    if n == 0 {
                        break; // connection closed
                    }
                    if bytes_to_string(&buf).contains("PING") {
                        ping(&mut stream).await;
                    }
                }
                Err(e) => {
                    break;
                }
            }
        }

    }

    fn bytes_to_string(bytes: &[u8]) -> String {
        String::from_utf8_lossy(bytes).to_string()
    }
    async fn ping(stream: &mut TcpStream) {
        if stream.write_all(b"+PONG\r\n").await.is_err() {
            println!("Failed to send PONG");
        };
    }
}
