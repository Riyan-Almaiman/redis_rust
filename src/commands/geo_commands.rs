
pub struct GeoCommands;
use std::collections::VecDeque;

use crate::command_router::CommandResult;
use crate::db::{KeyValue, DB};
use crate::geo_list::GeoList;
use crate::lists::List;
use crate::resp::Resp;
use crate::sorted_list::SortedList;
use crate::valuetype::ValueType;
impl GeoCommands {
    
    pub fn geoadd(db: &mut DB, key: String, longitude: f64, latitude: f64, member: String) -> CommandResult {
        
        let geo_list = db.database.entry(key.into_bytes()).or_insert_with(|| KeyValue { expiry: None, value: ValueType::GeoList(GeoList::new()) });
        match &mut geo_list.value {
            ValueType::GeoList(gl) => {
                let is_new = gl.insert_or_update(member, latitude, longitude);
                CommandResult::Response(Resp::Integer(if is_new { 1 } else { 0 }))
            },
            _ => CommandResult::Response(Resp::Error(b"WRONGTYPE Operation against a key holding the wrong kind of value".to_vec())),
        }
    }
}