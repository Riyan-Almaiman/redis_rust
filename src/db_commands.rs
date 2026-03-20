use base64::Engine;
use base64::engine::general_purpose;

use uuid::Uuid;

use crate::command_router::{self, CommandResult};
use crate::commands_parser::RedisCommand;
use crate::db::DB;

impl DB {
    pub fn execute_commands(&mut self, command: RedisCommand, client_id: Uuid) -> CommandResult {

        command_router::route(self, command, client_id)
    }
}
