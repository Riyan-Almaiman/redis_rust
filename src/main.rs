#![allow(unused_imports)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379")
        .await
        .expect("Failed to bind");
    let db: Arc<Mutex<HashMap<Vec<u8>, KeyValue>>> = Arc::new(Mutex::new(HashMap::new()));

    loop {
        let (stream, _) = listener.accept().await.expect("Accept failed");
        let db_copy = Arc::clone(&db);
        tokio::spawn(async move {
            handle_stream(stream, db_copy).await;
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
#[derive(Clone)]

enum ValueType {
    String(Vec<u8>),
    List(Vec<Vec<u8>>),
}

#[derive(Clone)]
struct KeyValue {
    expiry: Option<SystemTime>,
    value: ValueType,
}
async fn handle_stream(mut stream: TcpStream, mut values: Arc<Mutex<HashMap<Vec<u8>, KeyValue>>>) {
    let mut read_buffer: Vec<u8> = Vec::new();
    let mut temp = [0u8; 1024];
    let mut current_step: Option<ParseStep> = None;
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
        while !read_buffer.is_empty() {
            if read_buffer[0] != b'*' {
                read_buffer.drain(..1);
                continue;
            }

            match try_parse_command(&read_buffer) {
                Some((cmd, consumed)) => {
                    execute_command(&mut stream, &cmd, &values).await;
                    read_buffer.drain(..consumed);
                }
                None => break, // incomplete frame
            }
        }
    }
}
fn try_parse_command(buf: &[u8]) -> Option<(Vec<Vec<u8>>, usize)> {
    if buf.is_empty() || buf[0] != b'*' {
        return None;
    }

    let (arg_count, mut offset) = parse_number(&buf[1..])?;

    let mut args = Vec::new();
    if arg_count == 0 {
        return None;
    }

    for _ in 0..arg_count {
        if buf.get(offset)? != &b'$' {
            return None;
        }

        let (len, new_offset) = parse_number(&buf[offset + 1..])?;
        offset = offset + 1 + new_offset;

        if buf.len() < offset + len + 2 {
            return None;
        }
        if &buf[offset + len..offset + len + 2] != b"\r\n" {
            return None;
        }

        let data = buf[offset..offset + len].to_vec();
        args.push(data);

        offset += len + 2;
    }

    Some((args, offset))
}

fn reset(step: &mut Option<ParseStep>, current: &mut Vec<u8>, cmd: &mut Vec<Vec<u8>>) {
    *step = None;
    current.clear();
    cmd.clear();
}

async fn execute_command(
    stream: &mut TcpStream,
    cmds: &Vec<Vec<u8>>,
    values: &Arc<Mutex<HashMap<Vec<u8>, KeyValue>>>,
) {
    let cmd = cmds[0]
        .iter()
        .map(|b| b.to_ascii_lowercase())
        .collect::<Vec<_>>();

    match cmd.as_slice() {
        b"echo" => write_echo(stream, &cmds[1]).await,
        b"ping" => ping(stream).await,
        b"set" => set(stream, &cmds[1..], values).await,
        b"get" => get(stream, &cmds[1..], values).await,
        b"rpush" => list(stream, &cmds[1..], values).await,
        _ => {}
    }
}
async fn list(
    stream: &mut TcpStream,
    message: &[Vec<u8>],
    values: &Arc<Mutex<HashMap<Vec<u8>, KeyValue>>>,
) {
    let mut expiry = None;

    if message.len() < 2 {
        return;
    }
    let get_value = {
        let db = values.lock().await;
        db.get(&message[0]).cloned()
    };
    let mut value = match get_value {
        Some(value) => value,
        None => {
            let mut new_list: Vec<Vec<u8>> = Vec::new();
            new_list.push(message[1].clone().to_vec());
            let value = KeyValue {
                value: ValueType::List(new_list),
                expiry,
            };
            {
                let mut v = values.lock().await;
                v.insert(message[0].clone(), value.clone());

            }
            let response = format!(":{}\r\n", 1);
            if stream.write_all(response.as_bytes()).await.is_err() {
                return;
            }
            return;
        }
    };
    let new_list = match value.value {
        ValueType::List(mut list) => {
            list.push(message[1].clone().to_vec());
            list
        }
        _=> return
    };
    let response = format!(":{}\r\n", new_list.len());

    value.value = ValueType::List(new_list);
    {
        let mut v = values.lock().await;
        v.insert(message[0].clone(),value.clone() );
    }

    if stream.write_all(response.as_bytes()).await.is_err() {
        return;
    }
}
async fn set(
    stream: &mut TcpStream,
    message: &[Vec<u8>],
    values: &Arc<Mutex<HashMap<Vec<u8>, KeyValue>>>,
) {
    if message.len() < 2 {
        return;
    }

    let time_now = SystemTime::now();
    let mut expiry = None;
    if message.len() == 4 {
        let time_value = message.get(3).unwrap();
        let time_type = message.get(2).unwrap();
        let number = match std::str::from_utf8(time_value)
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
        {
            Some(n) => n,
            None => return,
        };
        let opt = time_type
            .iter()
            .map(|b| b.to_ascii_lowercase())
            .collect::<Vec<_>>();

        match opt.as_slice() {
            b"ex" => expiry = time_now.checked_add(Duration::from_secs(number as u64)),
            b"px" => expiry = time_now.checked_add(Duration::from_millis(number as u64)),
            _ => {}
        }
    }
    let value = KeyValue {
        value: ValueType::String(message[1].clone()),
        expiry,
    };
    {
        let mut v = values.lock().await;
        v.insert(message[0].clone(), value);
    }
    if stream.write_all(b"+OK\r\n").await.is_err() {
        return;
    }
}
async fn get(
    stream: &mut TcpStream,
    message: &[Vec<u8>],
    values: &Arc<Mutex<HashMap<Vec<u8>, KeyValue>>>,
) {
    let get_value = {
        let db = values.lock().await;
        db.get(&message[0]).cloned()
    };
    let value = match get_value {
        Some(value) => value,
        None => {
            if stream.write_all(b"$-1\r\n").await.is_err() {
                return;
            }
            return;
        }
    };
    let val_str = match value.value {
        ValueType::String(s) => s,
        _ => return,
    };
    match value.expiry {
        Some(expiry) => {
            if expiry < SystemTime::now() {
                {
                    let mut db = values.lock().await;
                    db.remove(&message[0]);
                }
                if stream.write_all(b"$-1\r\n").await.is_err() {
                    return;
                }
            } else {
                write_echo(stream, &*val_str).await
            }
        }
        None => write_echo(stream, &*val_str).await,
    }
}
fn parse_number(buffer: &[u8]) -> Option<(usize, usize)> {
    let crlf_index = buffer.windows(2).position(|w| w == b"\r\n")?;
    let before_bytes = &buffer[..crlf_index];

    let number = std::str::from_utf8(before_bytes)
        .ok()?
        .parse::<usize>()
        .ok()?;

    let after_index = crlf_index + 2;

    Some((number as usize, after_index))
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
