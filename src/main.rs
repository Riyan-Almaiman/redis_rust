#![allow(unused_imports)]

use std::io::Read;
use bytes::Buf;
use tokio::fs::read;
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
                    if n == 0 {
                        break;
                    }
                    let current_str = bytes_to_string(&read_buf[0..n]);
                    if current_str.starts_with("ECHO "){
                         echo(&mut stream, &buf[0..n]).await;
                        buf.clear();

                    }

                    if bytes_to_string(&buf) ==  "PING"  {
                        ping(&mut stream).await;
                        buf.clear();
                    }
                }
                Err(e) => {
                    break;
                }
            }
        }

    }
    async fn echo(stream: &mut TcpStream, current_buf: &[u8]) {
        println!("ECHO");
        match current_buf.last(){
            Some(&b) => {
                if b == b'\n' {
                    write_echo(stream, &current_buf[5..]).await;
                }
            }
            None => {}
        }
        let mut buf = Vec::new();

        loop {
                let mut read_buf = vec![0; 1024];

                let n = stream.read(&mut read_buf).await;
                match n {
                    Ok(n) => {
                        buf.append(&mut read_buf[0..n].to_vec());
                        if n == 0 {
                            break;
                        }
                        if buf.len() > 0 {
                        if(buf[buf.len() - 1] == b'\n') {
                            write_echo( stream, &buf).await;
                            break;
                        }}

                    }
                    Err(e) => {
                        break;
                    }
                }
            }

    }

    async fn write_echo(stream: &mut TcpStream, message: &[u8] ){

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
        };;
    }
    fn bytes_to_string(bytes: &[u8]) -> String {
        String::from_utf8_lossy(&bytes).to_string()
    }
    async fn ping(stream: &mut TcpStream) {
        if stream.write_all(b"+PONG\r\n").await.is_err() {
            println!("Failed to send PONG");
        };
    }
}
