#[derive(Clone)]

pub enum Role {
    Master {
        replication_id: String,
        replication_offset: u64,
    },
    Slave {
        master: String,
        replication_id: String,
        port: String,
        replication_offset: u64,
    },
}
impl Role {
    pub fn increment_offset(&mut self, bytes: usize) {
        match self {
            Role::Master {
                replication_offset, ..
            } => *replication_offset += bytes as u64,
            Role::Slave {
                replication_offset, ..
            } => *replication_offset += bytes as u64,
        }
    }

    pub fn set_offset(&mut self, offset: u64) {
        match self {
            Role::Master {
                replication_offset, ..
            } => *replication_offset = offset,
            Role::Slave {
                replication_offset, ..
            } => *replication_offset = offset,
        }
    }
    pub fn get_replication(&self) -> String {
        match self {
            Role::Master {
                replication_id,
                replication_offset,
            } => format!(
                "{}\r\nmaster_replid:{}\r\nmaster_repl_offset:{}\r\n",
                self.get_role(),
                replication_id,
                replication_offset
            ),
            Role::Slave {
                master,
                replication_id,
                port,
                replication_offset,
            } => format!(
                "{}\r\nslave_replid:{}\r\nslave_repl_offset:{}\r\n",
                self.get_role(),
                replication_id,
                replication_offset
            ),
        }
    }
    pub fn get_repl_id(&self) -> String {
        match self {
            Role::Master {
                replication_id,
                replication_offset,
            } => replication_id.clone(),

            Role::Slave {
                master,
                replication_id,
                port,
                replication_offset,
            } => replication_id.clone(),
        }
    }
    pub fn get_repl_offset(&self) -> String {
        match self {
            Role::Master {
                replication_id,
                replication_offset,
            } => replication_offset.to_string(),
            Role::Slave {
                master,
                replication_id,
                port,
                replication_offset,
            } => replication_offset.to_string(),
        }
    }

    pub fn get_role(&self) -> String {
        match self {
            Role::Master { .. } => "role:master".to_string(),
            Role::Slave { .. } => "role:slave".to_string(),
        }
    }
}
