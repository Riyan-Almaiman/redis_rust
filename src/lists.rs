use crate::resp::Resp;
use crate::resp::Resp::Integer;
use std::collections::VecDeque;

#[derive(Debug)]

pub struct List {
    pub(crate) list: VecDeque<Vec<u8>>,
}

impl List {
    pub fn new() -> Self {
        List {
            list: VecDeque::new(),
        }
    }

    pub fn rpush(&mut self, elements: Vec<Vec<u8>>) -> usize {
        for element in elements {
            self.list.push_back(element);
        }

        self.list.len()
    }

    pub fn lpop(&mut self, key: &Vec<u8>, count: usize) -> Resp {
        match std::str::from_utf8(&key) {
            Ok(v) => v.to_string(),
            Err(_) => {
                return Resp::Error("Invalid UTF-8 key".to_string().as_bytes().to_vec());
            }
        };

        if self.list.is_empty() {
            return Resp::NullBulkString;
        }

        let mut popped = Vec::new();
        while popped.len() < count {
            if let Some(element) = self.list.pop_front() {
                popped.push(Resp::BulkString(element));
            } else {
                break;
            }
        }

        if popped.len() == 1 && count == 1 {
            return popped.pop().unwrap();
        } else if popped.is_empty() {
            return Resp::NullArray;
        } else {
            return Resp::Array(VecDeque::from(popped));
        }
    }
    pub fn llen(&self) -> Resp {
        return Integer(self.list.len());
    }

    pub fn get_list_range(&self, start_raw: i64, end_raw: i64) -> Resp {
        let len = self.list.len() as i64;

        if len == 0 {
            return Resp::Array(Vec::new().into());
        }

        let start = if start_raw < 0 {
            (len + start_raw).max(0)
        } else {
            start_raw
        };

        let end = if end_raw < 0 {
            (len + end_raw).max(0)
        } else {
            end_raw
        };

        if start > end || start >= len {
            return Resp::Array(Vec::new().into());
        }

        let start = start as usize;
        let count = ((end as usize).min(len as usize - 1)) - start + 1;

        let items: Vec<Resp> = self
            .list
            .iter()
            .skip(start)
            .take(count)
            .map(|v| Resp::BulkString(v.clone()))
            .collect();

        return Resp::Array(VecDeque::from(items));
    }
    pub fn lpush(&mut self, elements: Vec<Vec<u8>>) -> Resp {
        for (i, _) in elements.iter().enumerate() {
            self.list.push_front(elements[i].clone())
        }

        return Integer(self.list.len());
    }
}
