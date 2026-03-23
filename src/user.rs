use std::collections::{HashMap, HashSet};

use crate::commands_parser::RedisCommand;

pub struct User {
    pub name: String,
    pub password: Option<String>,
    pub allowed_commands: HashSet<String>, 
    pub flags: HashSet<String>,
}
pub enum Flag {
    All,
    On, 
    AllKeys,
    AllCommands,
}
impl Flag {
    pub fn from_str(flag: &str) -> Option<Flag> {
        match flag.to_ascii_lowercase().as_str() {
            "on" => Some(Flag::On),
            "all" => Some(Flag::All),
            "allkeys" => Some(Flag::AllKeys),
            "allcommands" => Some(Flag::AllCommands),
            _ => None,
        }
    }
    pub fn to_str(&self) -> &str {
        match self {
            Flag::All => "all",
            Flag::On => "on",
            Flag::AllKeys => "allkeys",
            Flag::AllCommands => "allcommands",
        }
    }

}

impl User {
    pub fn can_execute(&self, cmd_name: &RedisCommand) -> bool {
        self.allowed_commands.contains(cmd_name.name()) || self.allowed_commands.contains("*") || self.flags.contains("allcommands")
    }
    pub fn has_flag(&self, flag: &str) -> bool {
        self.flags.contains(flag) || self.flags.contains("all")
    }
}
