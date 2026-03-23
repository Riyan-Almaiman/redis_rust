use std::collections::{HashMap, HashSet};

use crate::commands_parser::RedisCommand;

pub struct User {
    pub name: String,
    pub passwords: Vec<String>,
    pub allowed_commands: HashSet< String>, 
    pub flags: HashSet< Flag>,
}
#[derive(Hash, Eq, PartialEq)]

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
     pub fn can_execute(&self, cmd: &RedisCommand) -> bool {
        let cmd_name = cmd.name().to_ascii_lowercase();
        self.allowed_commands.contains(&cmd_name) || self.allowed_commands.contains("all")
    }
    pub fn has_flag(&self, flag: &Flag) -> bool {
        self.flags.contains(flag) || self.flags.contains(&Flag::All)
    }
}
