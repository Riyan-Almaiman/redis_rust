use std::collections::{HashMap, HashSet, VecDeque};

use crate::{commands_parser::RedisCommand, resp::Resp};

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
       pub fn get_passwords_for_user(&self) -> Vec<String> {
        let passwords: Vec<String> = self
            .passwords
            .iter()
            .map(|pw| pw.clone())
            .collect();
        passwords
    }
     pub fn can_execute(&self, cmd: &RedisCommand) -> bool {
        let cmd_name = cmd.name().to_ascii_lowercase();
        self.allowed_commands.contains(&cmd_name) || self.allowed_commands.contains("all")
    }
    pub fn has_flag(&self, flag: &Flag) -> bool {
        self.flags.contains(flag) || self.flags.contains(&Flag::All)
    }
      pub  fn get_flags_for_user(&self) -> Vec<String> {
        let flags: Vec<String> = self
            .flags
            .iter()
            .map(|flag| flag.to_str().to_string())
            .collect();

        flags
    }
}
