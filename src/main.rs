#![allow(unused_imports)]

use std::io::{stdout, Read};
use std::path::Path;
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
            let current = bytes_to_string(&buf);
            let n = stream.read(&mut read_buf).await;
            match n {
                Ok(n) => {
                    buf.append(&mut read_buf[0..n].to_vec());
                    let succeeded = parse_array(&buf, &mut stream).await;
                    if succeeded {
                        buf.clear();
                        println!("Successfully read {} bytes", buf.len());
                        print!("{}[2J{}[1;1H", 27 as char, 27 as char);
                    }
                    if n == 0 {
                        break;
                    }

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

    async fn parse_array(data: &[u8], stream: &mut TcpStream ) -> bool {

        let mut result: Vec<Vec<u8>> = Vec::new();
        let mut current = Vec::new();
        let mut iter = data.iter().enumerate();
        while let Some((i, &val)) = iter.next() {
              if val == b'\r' || val == b'\n' {
                  result.push(current.clone());
                  iter.next();
                  current.clear();
              }else {
                  current.push(val);
              }

        }
        for r in &result {
            println!("{:?}", bytes_to_string(&r));
        }
        if result.len() > 0{
            match get_number_of_args(&result[0]){
                Some(n) => {
                    if (result.len()-1) / 2 == n as usize {
                         if result.len() > 2{
                             match bytes_to_string(&result[2]).to_ascii_lowercase().as_str() {
                                 "ping" =>  {
                                     ping(stream).await;
                                     return true;
                                 },
                                 "echo"=> {
                                     match result.get(4) {
                                         Some(v) => write_echo(stream, v).await,
                                         None => return true,
                                     }
                                 }
                                 _ => return true,
                             }
                         }
                    }
                    else{
                        if (result.len()-1) / 2 < n as usize {
                            return false;
                        }
                    }
                }
                None => { return true; }
            }
        }
    false

    }

    fn get_number_of_args(number_of_args: &Vec<u8>) -> Option<u32> {
        if number_of_args.len() !=2 {
            return None;
        };
        if number_of_args[0] != b'*'{
            return None;
        };
        match (number_of_args[1] as char).to_digit(10){
            Some(n) => Some(n),
            None => None
        }

    }
}
