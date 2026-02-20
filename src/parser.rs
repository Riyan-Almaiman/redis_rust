use crate::parser::CurrentState::{Incomplete, InvalidCommand, ReadingArrayElementCount, ReadingBulkString, ReadingElementType, ReadingError, ReadingInteger, ReadingSimpleString};
use crate::resp::Resp;
use crate::resp::Resp::Array;

pub enum ReturnType {

}
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
    InvalidCommand
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
                        return if let Some(line) = self.read_until_clrf() {
                            self.read_buffer.drain(..self.current_index);
                            self.current_index = 0;

                            let parts: Vec<Resp> = line
                                .split(|b| *b == b' ')
                                .map(|s| Resp::BulkString(s.to_vec()))
                                .collect();

                            Some(Resp::Array(parts))
                        } else {
                            None
                        }
                    }
                }

                _ => {
                    self.current_command.current_state = self.parse_command_array();
                    if self.current_command.elements.len() == self.current_command.element_count as usize {
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
            ReadingArrayElementCount {
                array: _,
                starting_buffer_index: _,
                mut final_buffer_index,
            } => {
                let num = self.read_until_clrf();
                match num {
                    Some(n) => {
                        final_buffer_index = Some(self.current_index);

                        if let Some(n) = Self::vec_to_i64(&n) {
                            self.current_command.element_count = n;
                        } else {
                            return InvalidCommand;
                        }

                        self.current_command
                            .states
                            .push(self.current_command.current_state.clone());
                        ReadingElementType {
                            element_symbol: None,
                        }
                    }
                    None => Incomplete,
                }
            }
            ReadingElementType { element_symbol } => self.get_type(),

            ReadingInteger { .. } =>  self.read_integer(),
            ReadingBulkString { .. } => self.read_bulk_string_size(),
            ReadingSimpleString {..} => self.read_simple_or_error_string(),
            ReadingError {..} => self.read_simple_or_error_string(),
            _ => CurrentState::None,
        }
    }
    fn read_simple_or_error_string(&mut self) -> CurrentState {
        let string = self.read_until_clrf();
        match string {
            Some(s) => {
                self.current_command.elements.push(Resp::SimpleString(s));
                return ReadingElementType {
                    element_symbol: None,
                };
            }
            None => Incomplete,

        }

    }
    fn read_bulk_string_size(&mut self) -> CurrentState {

        let mut string = Vec::new();
        let num = self.read_until_clrf();
        let read = 0;
        let number = match num {
            Some(n) => match Self::vec_to_i64(&n) {
                Some(n) => n,
                None =>   {
                    return InvalidCommand},
            },
            None =>          {
                return InvalidCommand}
        };
        if self.read_buffer.len() < (self.current_index + number as usize + 2) {

            return Incomplete;
        }
        if self.read_buffer[self.current_index + number as usize ] != b'\r'
            || self.read_buffer[self.current_index + number as usize + 1] != b'\n'
        {
            return InvalidCommand;
        }
        let mut count = 0;
        while self.current_index < self.read_buffer.len() {
            string.push(self.read_buffer[self.current_index + count]);
            count += 1;
            if count   ==  number as usize {
                count+=2;
                self.current_command.elements.push(Resp::BulkString(string));
                self.current_index+=count;
                return ReadingElementType {
                    element_symbol: None,
                };
            }
        }

        panic!("Invalid command size");
    }
    fn read_integer(&mut self) -> CurrentState{
        let num = self.read_until_clrf();
        match num {
            Some(n) => match &mut self.current_command.current_state {
                ReadingInteger {
                    parsed,
                    final_buffer_index,
                    ..
                } => {
                    match parsed {
                        Resp::Integer(val) => {
                            *val = Self::vec_to_i64(&n).unwrap() as usize;
                            self.current_command.elements.push(parsed.clone())
                        }
                        _ => panic!("unreachable"),
                    };

                    *final_buffer_index = Some(self.current_index);

                    ReadingElementType {
                        element_symbol: None,
                    }
                }
                _ => panic!("unreachable"),
            },
            None => Incomplete,
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
        self.current_index+=1;
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
