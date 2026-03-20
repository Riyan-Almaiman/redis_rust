use crate::lists::List;
use crate::stream::Stream;

pub enum ValueType {
    String(Vec<u8>),
    List(List),
    Stream(Stream),
}
impl ValueType {
    pub fn stream() -> Self {
        ValueType::Stream(Stream::new())
    }

    pub fn get_value_type(&self) -> &[u8] {
        match self {
            ValueType::String(_) => b"string",
            ValueType::List(_) => b"list",
            ValueType::Stream(_) => b"stream",
            _ => b"none",
        }
    }
}
