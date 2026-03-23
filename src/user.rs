pub struct User {
    pub password: String,
    pub allowed_commands: Option<Vec<String>>, // None means all commands
}

impl User {
    pub fn can_execute(&self, cmd_name: &str) -> bool {
        match &self.allowed_commands {
            Some(list) => list.contains(&cmd_name.to_lowercase()),
            None => true,
        }
    }
}
