use std::collections::{BTreeSet, HashMap};

use ordered_float::OrderedFloat;

use crate::lists::List;
use crate::sorted_list::SortedList;
use crate::stream::Stream;

pub enum ValueType {
    String(Vec<u8>),
    List(List),
    Stream(Stream),
    SortedList(SortedList)

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
    pub fn create_sorted_list() -> Self {
         ValueType::SortedList(SortedList::new())
    }
}
