use core::panic;
use std::collections::VecDeque;
use std::str::Utf8Error;

use tokio::sync::oneshot;
use uuid::Uuid;

use crate::blocking_stream::StreamWait;
use crate::commands::RedisCommand;

#[derive(Clone, Debug)]
pub enum Resp {
    Array(VecDeque<Resp>),
    BulkString(Vec<u8>),
    Integer(usize),
    NullArray,
    SimpleString(Vec<u8>),
    NullBulkString,
    BlockingClient {
        keys: Vec<Vec<u8>>,
        timeout: f64
    },
    Exec(Vec<RedisCommand>),
    BlockingStreamClient {
        client_id: Uuid, resolved_streams:Vec<StreamWait> , timeout_ms:f64 
    },
    None,
    Error(Vec<u8>),
}

impl Resp {
    pub fn get_bytes(resp: &Resp) -> Option<&[u8]> {
        match resp {
            Resp::SimpleString(bytes) => Some(bytes),
            Resp::BulkString(bytes) => Some(bytes),
            Resp::Error(bytes) => Some(bytes),
            _ => None,
        }
    }
    pub fn write_format(&self, out: &mut Vec<u8>) {
        match self {
            Resp::SimpleString(bytes) => {
                out.push(b'+');
                out.extend_from_slice(bytes);
                out.extend_from_slice(b"\r\n");
            }
            Resp::Integer(number) => {
                out.push(b':');
                out.extend_from_slice(number.to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
            }
            Resp::BulkString(bytes) => {
                out.push(b'$');
                out.extend_from_slice(bytes.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                out.extend_from_slice(bytes);
                out.extend_from_slice(b"\r\n");
            }
            Resp::Array(items) => {
                out.push(b'*');
                out.extend_from_slice(items.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                for item in items {
                    item.write_format(out);
                }
            }
            Resp::Error(bytes) => {
                out.push(b'-');
                out.extend_from_slice(bytes);
                out.extend_from_slice(b"\r\n");
            }
            Resp::NullBulkString => out.extend_from_slice(b"$-1\r\n"),
            Resp::NullArray => out.extend_from_slice(b"*-1\r\n"),
            _=> panic!("")
        }
    }
    pub fn formate_cmd(&self) -> Vec<u8> {
        match self {
            Resp::SimpleString(bytes) => {
                let mut out = b"+".to_vec();
                out.extend(bytes);
                out.extend(b"\r\n");
                out
            }
            Resp::Integer(number_bytes) => {
                let mut out = b":".to_vec();
                out.extend(number_bytes.to_string().as_bytes());
                out.extend(b"\r\n");
                out
            }
            Resp::BulkString(bytes) => {
                let mut out = b"$".to_vec();
                out.extend(bytes.len().to_string().as_bytes());
                out.extend(b"\r\n");
                out.extend(bytes);
                out.extend(b"\r\n");
                out
            }
            Resp::Array(items) => {
                let mut out = b"*".to_vec();
                out.extend(items.len().to_string().as_bytes());
                out.extend(b"\r\n");
                for item in items {
                    out.extend(Self::formate_cmd(item));
                }
                out
            }
            Resp::NullBulkString => b"$-1\r\n".to_vec(),
            Resp::NullArray => b"*-1\r\n".to_vec(),
            Resp::Error(bytes) => {
                let mut out = (b"-").to_vec();
                out.extend(bytes);
                out.extend(b"\r\n");
                out
            }            _=> panic!("")

        }
    }
}
