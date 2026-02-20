use crate::parser::CurrentState::{
    Incomplete, InvalidCommand, ReadingArrayElementCount, ReadingBulkString, ReadingElementType,
    ReadingError, ReadingInteger, ReadingSimpleString,
};
use crate::resp::Resp;
use crate::resp::Resp::Array;

pub enum ReturnType {}
pub struct Parser {
    pub(crate) read_buffer: Vec<u8>,
    pub(crate) current_index: usize,
    pub(crate) current_command: CommandArray,
}
#[derive(Clone, Debug)]
pub struct CommandArray {
    elements: Vec<Resp>,
    pub(crate) current_element: Option<Resp>,
    pub(crate) current_state: CurrentState,
    pub(crate) states: Vec<CurrentState>,
    pub element_count: i64,
    pub pos: usize,
}
// Array(Vec<Resp>),
// BulkString(Vec<u8>),
// Integer(usize),
// Null,
// SimpleString(Vec<u8>),
#[derive(Clone, Debug)]
pub enum CurrentState {
    None,
    ReadingElementType {
        element_symbol: Option<char>,
    },
    ReadingArrayElementCount {
        array: Resp,
        starting_buffer_index: usize,
        final_buffer_index: Option<usize>,
    },

    ReadingBulkString {
        element_index: usize,
        size: Option<usize>,
        parsed_size: Option<Vec<u8>>,
        parsed: Resp,
        starting_buffer_index: usize,
        final_buffer_index: Option<usize>,
    },
    ReadingInteger {
        element_index: usize,
        parsed: Resp,
        starting_buffer_index: usize,
        final_buffer_index: Option<usize>,
    },
    ReadingSimpleString {
        element_index: usize,
        parsed: Resp,
        starting_buffer_index: usize,
        final_buffer_index: Option<usize>,
    },
    ReadingError {
        element_index: usize,
        parsed: Resp,
        starting_buffer_index: usize,
        final_buffer_index: Option<usize>,
    },
    Incomplete,
    InvalidCommand,
}
impl CommandArray {
    pub fn new() -> Self {
        Self {
            current_element: None,
            current_state: CurrentState::None,
            states: Vec::new(),
            elements: Vec::new(),
            element_count: 0,
            pos: 0,
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

    pub fn parse(&mut self) -> Option<Resp> {
        while self.current_index < self.read_buffer.len() {
            match self.current_command.current_state {
                CurrentState::None => {
                    let byte = self.read_buffer[self.current_index];

                    if byte == b'*' {
                        self.current_command.current_state = ReadingArrayElementCount {
                            array: Array(Vec::new()),
                            starting_buffer_index: 0,
                            final_buffer_index: None,
                        };
                        self.current_index += 1;
                    } else {
                        if let Some(line_slice) = self.read_until_clrf() {
                            let line_vec = line_slice.to_vec();

                            self.read_buffer.drain(..self.current_index);
                            self.current_index = 0;

                            let parts: Vec<Resp> = line_vec
                                .split(|b| *b == b' ')
                                .map(|s| Resp::BulkString(s.to_vec()))
                                .collect();

                            return Some(Resp::Array(parts));
                        } else {
                            return None;
                        }
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
        let state = std::mem::replace(&mut self.current_command.current_state, CurrentState::None);

        match state {
            ReadingArrayElementCount { .. } => {
                let n_option = self.read_until_clrf().and_then(|b| Self::vec_to_u64(b));

                if let Some(n) = n_option {
                    self.current_command.element_count = n;
                    self.current_command.states.push(state);
                    ReadingElementType {
                        element_symbol: None,
                    }
                } else {
                    Incomplete
                }
            }
            ReadingElementType { .. } => self.get_type(),
            ReadingInteger { .. } => self.read_integer(),
            ReadingBulkString { .. } => self.read_bulk_string_size(),
            ReadingSimpleString { .. } | ReadingError { .. } => self.read_simple_or_error_string(),
            _ => CurrentState::None,
        }
    }
    fn read_simple_or_error_string(&mut self) -> CurrentState {
        let data_opt = self.read_until_clrf().map(|s| s.to_vec());

        if let Some(data) = data_opt {
            self.current_command.elements.push(Resp::SimpleString(data));
            ReadingElementType {
                element_symbol: None,
            }
        } else {
            Incomplete
        }
    }
    fn read_bulk_string_size(&mut self) -> CurrentState {
        let num = match self.read_until_clrf() {
            Some(n) => match Self::vec_to_u64(n) {
                Some(n) => n,
                None => return InvalidCommand,
            },
            None => return Incomplete,
        };

        let size = num as usize;
        let total_needed = self.current_index + size + 2;

        if self.read_buffer.len() < total_needed {
            return Incomplete;
        }

        let string_data = &self.read_buffer[self.current_index..self.current_index + size];

        if self.read_buffer[self.current_index + size] != b'\r'
            || self.read_buffer[self.current_index + size + 1] != b'\n'
        {
            return InvalidCommand;
        }

        self.current_command
            .elements
            .push(Resp::BulkString(string_data.to_vec()));
        self.current_index += size + 2;

        ReadingElementType {
            element_symbol: None,
        }
    }
    fn read_integer(&mut self) -> CurrentState {
        let val_opt = self.read_until_clrf().and_then(|b| Self::vec_to_u64(b));

        if let Some(val) = val_opt {
            self.current_command
                .elements
                .push(Resp::Integer(val as usize));
            ReadingElementType {
                element_symbol: None,
            }
        } else {
            Incomplete
        }
    }
    fn get_type(&mut self) -> CurrentState {
        let next_state_type = match self.read_buffer[self.current_index] {
            b'+' => ReadingSimpleString {
                element_index: self.current_command.elements.len(),
                parsed: Resp::SimpleString(Vec::new()),
                starting_buffer_index: self.current_index,
                final_buffer_index: None,
            },
            b':' => ReadingInteger {
                element_index: self.current_command.elements.len(),
                parsed: Resp::Integer(0),
                starting_buffer_index: self.current_index,
                final_buffer_index: None,
            },
            b'$' => ReadingBulkString {
                parsed: Resp::BulkString(Vec::new()),
                parsed_size: None,
                size: None,
                element_index: self.current_command.elements.len(),
                starting_buffer_index: self.current_index,
                final_buffer_index: None,
            },
            b'-' => ReadingError {
                element_index: self.current_command.elements.len(),
                parsed: Resp::Error(Vec::new()),
                starting_buffer_index: self.current_index,
                final_buffer_index: None,
            },
            b'*' => ReadingArrayElementCount {
                array: Resp::Array(Vec::new()),
                starting_buffer_index: self.current_index,
                final_buffer_index: None,
            },
            _ => CurrentState::None,
        };
        self.current_index += 1;
        next_state_type
    }

    fn read_until_clrf(&mut self) -> Option<&[u8]> {
        let start = self.current_index;
        let mut i = start;

        while i + 1 < self.read_buffer.len() {
            if self.read_buffer[i] == b'\r' && self.read_buffer[i + 1] == b'\n' {
                self.current_index = i + 2;
                return Some(&self.read_buffer[start..i]);
            }
            i += 1;
        }
        None
    }
    fn vec_to_u64(vec: &[u8]) -> Option<i64> {
        let s = std::str::from_utf8(&vec).unwrap();
        Some(s.parse().unwrap())
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
