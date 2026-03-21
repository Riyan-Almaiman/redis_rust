use crate::parser::CurrentState::{
    Incomplete, InvalidCommand, ReadingArrayElementCount, ReadingBulkString, ReadingElementType,
    ReadingError, ReadingInteger, ReadingSimpleString,
};
use crate::resp::Resp;
use crate::resp::Resp::Array;
use std::collections::VecDeque;

pub struct Parser {
    pub(crate) read_buffer: Vec<u8>,
    pub(crate) current_index: usize,
    pub(crate) current_command: CommandArray,
}
#[derive(Clone, Debug)]
pub struct CommandArray {
    elements: VecDeque<Resp>,
    pub(crate) current_state: CurrentState,
    pub element_count: i64,
}

#[derive(Clone, Debug)]
pub enum CurrentState {
    None,
    ReadingElementType,
    ReadingArrayElementCount,
    ReadingBulkString,
    ReadingInteger,
    ReadingSimpleString,
    ReadingError,
    Incomplete,
    InvalidCommand,
}
impl CommandArray {
    pub fn new() -> Self {
        Self {
            current_state: CurrentState::None,
            elements: VecDeque::new(),
            element_count: 0,
        }
    }
}

impl Parser {
    pub fn new() -> Self {
        Self {
            read_buffer: Vec::new(),
            current_index: 0,
            current_command: CommandArray::new(),
        }
    }
    fn reset(&mut self) {
        self.read_buffer.drain(..self.current_index);
        self.current_index = 0;
        self.current_command = CommandArray::new();
    }
    pub fn parse(&mut self) -> Option<Resp> {
        while self.current_index < self.read_buffer.len() {
            match self.current_command.current_state {
                    CurrentState::None => {
                        let byte = self.read_buffer[self.current_index];

                        match byte {
                            b'*' => {
                                self.current_command.current_state = ReadingArrayElementCount;
                                self.current_index += 1;
                            }

                            b'+' => {
                                self.current_index += 1;
                                if let Some(s) = self.read_until_clrf() {
                                    self.reset();
                                    return Some(Resp::SimpleString(s));
                                } else {
                                    return None;
                                }
                            }

                            b'$' => {
                                let saved = self.current_index;
                                self.current_index += 1;

                                let len_bytes = match self.read_until_clrf() {
                                    Some(b) => b,
                                    None => { self.current_index = saved; return None; }
                                };
                                let len = match Self::vec_to_i64(&len_bytes) {
                                    Some(n) => n as usize,
                                    None => { self.current_index = saved; return None; }
                                };

                                if self.read_buffer.len() < self.current_index + len + 2 {
                                    self.current_index = saved;
                                    return None;
                                }

                                let data = self.read_buffer[self.current_index..self.current_index + len].to_vec();
                                self.current_index += len + 2;
                                self.reset();
                                return Some(Resp::BulkString(data));
                            }

                            b':' => {
                                self.current_index += 1;
                                let num = self.read_until_clrf()?;
                                let val = Self::vec_to_i64(&num)? as usize;

                                self.reset();
                                return Some(Resp::Integer(val));
                            }

                            b'-' => {
                                self.current_index += 1;
                                let err = self.read_until_clrf()?;

                                self.reset();
                                return Some(Resp::Error(err));
                            }

                            _ => return None,
                        }
                    }


                _ => {
                    self.current_command.current_state = self.parse_command_array();
                    if self.current_command.elements.len()
                        == self.current_command.element_count as usize
                    {
                        let result = Resp::Array(self.current_command.elements.clone());

                        self.read_buffer.drain(..self.current_index);
                        self.current_index = 0;
                        self.current_command = CommandArray::new();

                        return Some(result);
                    }
                }
            }
        }
        None
    }
    fn parse_command_array(&mut self) -> CurrentState {
        match self.current_command.current_state {
            ReadingArrayElementCount => {
                let num = self.read_until_clrf();
                match num {
                    Some(n) => {
                        if let Some(n) = Self::vec_to_i64(&n) {
                            self.current_command.element_count = n;
                        } else {
                            return InvalidCommand;
                        }

                        ReadingElementType
                    }
                    None => Incomplete,
                }
            }
            ReadingElementType => self.get_type(),

            ReadingInteger { .. } => self.read_integer(),
            ReadingBulkString { .. } => self.read_bulk_string_size(),
            ReadingSimpleString { .. } => self.read_simple_or_error_string(),
            ReadingError { .. } => self.read_simple_or_error_string(),
            _ => CurrentState::None,
        }
    }
    fn read_simple_or_error_string(&mut self) -> CurrentState {
        let string = self.read_until_clrf();
        match string {
            Some(s) => {
                self.current_command
                    .elements
                    .push_back(Resp::SimpleString(s));
                return ReadingElementType;
            }
            None => Incomplete,
        }
    }
    fn read_bulk_string_size(&mut self) -> CurrentState {
        let mut string = Vec::new();
        let num = self.read_until_clrf();
        let number = match num {
            Some(n) => match Self::vec_to_i64(&n) {
                Some(n) => n,
                None => return InvalidCommand,
            },
            None => return InvalidCommand,
        };
        if self.read_buffer.len() < (self.current_index + number as usize + 2) {
            return Incomplete;
        }
        if self.read_buffer[self.current_index + number as usize] != b'\r'
            || self.read_buffer[self.current_index + number as usize + 1] != b'\n'
        {
            return InvalidCommand;
        }
        let mut count = 0;
        while self.current_index + count < self.read_buffer.len() {
            string.push(self.read_buffer[self.current_index + count]);
            count += 1;
            if count == number as usize {
                count += 2;
                self.current_command
                    .elements
                    .push_back(Resp::BulkString(string));
                self.current_index += count;
                return ReadingElementType;
            }
        }

        return InvalidCommand;
    }
    fn read_integer(&mut self) -> CurrentState {
        let num = self.read_until_clrf();

        match num {
            Some(n) => {
                let val = Self::vec_to_i64(&n);
                if let Some(val) = val {
                    self.current_command
                        .elements
                        .push_back(Resp::Integer(val as usize));

                    ReadingElementType
                } else {
                    InvalidCommand
                }
            }
            None => Incomplete,
        }
    }
    fn get_type(&mut self) -> CurrentState {
        let next_state_type = match self.read_buffer[self.current_index] {
            b'+' => ReadingSimpleString,
            b':' => ReadingInteger,
            b'$' => ReadingBulkString,
            b'-' => ReadingError,
            b'*' => ReadingArrayElementCount,
            _ => CurrentState::None,
        };
        self.current_index += 1;
        next_state_type
    }

    fn read_until_clrf(&mut self) -> Option<Vec<u8>> {
        let mut num = Vec::new();
        while self.current_index < self.read_buffer.len() {
            if self.check_for_clrf() {
                return Some(num);
            }
            num.push(self.read_buffer[self.current_index]);
            self.current_index += 1;
        }
        None
    }
    fn vec_to_i64(vec: &Vec<u8>) -> Option<i64> {
        let s = std::str::from_utf8(&vec);
        if let Ok(s) = s {
            let parsed = s.parse::<i64>();
            if let Ok(parsed) = parsed {
                Some(parsed)
            } else {
                None
            }
        } else {
            None
        }
    }

    fn check_for_clrf(&mut self) -> bool {
        if self.read_buffer[self.current_index] == b'\r' {
            if self.current_index + 1 < self.read_buffer.len() {
                if self.read_buffer[self.current_index + 1] == b'\n' {
                    self.current_index += 2;
                    return true;
                }
            }
        }
        false
    }
}
