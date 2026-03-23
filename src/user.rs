use std::collections::{HashMap, HashSet};

use crate::commands_parser::RedisCommand;

pub struct User {
    pub name: String,
    pub password: Option<String>,
    pub allowed_commands: HashMap<String, RedisCommand>, 
    pub flags: HashMap<String, Flag>,
}
pub enum Flag {
    All,
    On, 
    AllKeys,
    NoPass,
    AllCommands,
}
impl Flag {
    pub fn from_str(flag: &str) -> Option<Flag> {
        match flag.to_ascii_lowercase().as_str() {
            "on" => Some(Flag::On),
            "all" => Some(Flag::All),
            "nopass" => Some(Flag::NoPass),
            "allkeys" => Some(Flag::AllKeys),
            "allcommands" => Some(Flag::AllCommands),
            _ => None,
        }
    }
    pub fn to_str(&self) -> &str {
        match self {
            Flag::All => "all",
            Flag::NoPass => "nopass",
            Flag::On => "on",
            Flag::AllKeys => "allkeys",
            Flag::AllCommands => "allcommands",
        }
    }

}

impl User {
    pub fn can_execute(&self, cmd_name: &RedisCommand) -> bool {
        self.allowed_commands.contains_key(cmd_name.name()) 
    }
    pub fn has_flag(&self, flag: &str) -> bool {
        self.flags.contains_key(flag) || self.flags.contains_key("all")
    }
}
