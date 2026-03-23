use sha2::Digest;
use uuid::Uuid;

use crate::{db::{ClientSession, DB}, user::{Flag, User}};

impl DB {
    pub fn authenticated_user(&self, client_id: Uuid) -> Option<&str> {
        self.sessions
            .get(&client_id)
            .and_then(|session| session.authenticated_user.as_deref())
    }

    pub fn set_authenticated_user(&mut self, client_id: Uuid, username: String) {
        self.session_mut(client_id).authenticated_user = Some(username);
    }

    pub fn session_mut(&mut self, client_id: Uuid) -> &mut ClientSession {
        self.sessions.entry(client_id).or_default()
    }
    
    pub fn hash_password(password: &str) -> String {
        let mut hasher = sha2::Sha256::new();
        hasher.update(password.as_bytes());
        let result = hasher.finalize();
        hex::encode(result)
    }

    pub fn validate_password(password: &str, stored_hash: &str) -> bool {
        Self::hash_password(password) == stored_hash
    }
    pub fn authenticate_user(&mut self, username: &str, password: &str, client_id: Uuid) -> bool {
         let user = self.users.get(username);
         
        if let Some(user) = user {
            if user.flags.contains(&Flag::NoPass)  {
                return true;
            }
            for stored_hash in &user.passwords {
                if Self::validate_password(&password, stored_hash) {
                    self.set_authenticated_user(client_id, username.to_string());
                    return true;
                }
            }
            false
        } else {
            false
        }
    }
}
