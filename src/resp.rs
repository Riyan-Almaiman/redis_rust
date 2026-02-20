#[derive(Clone, Debug)]
pub enum Resp {
    Array(Vec<Resp>),
    BulkString(Vec<u8>),
    Integer(usize),
    NullArray,
    SimpleString(Vec<u8>),
    NullBulkString,
    Error(Vec<u8>),
}

impl Resp {
    pub fn get_bytes(resp: &Resp) -> Vec<u8> {
        return match resp {
            Resp::SimpleString(bytes) => return bytes.clone(),
            Resp::Integer(number) => return number.to_be_bytes().to_vec(),
            Resp::BulkString(bytes) => return bytes.clone(),

            Resp::Error(bytes) => return bytes.clone(),
            _ => Vec::new(),
        };
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
            }
        }
    }
}
