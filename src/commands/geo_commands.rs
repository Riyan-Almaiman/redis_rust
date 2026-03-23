
pub struct GeoCommands;
use std::collections::VecDeque;

use crate::command_router::CommandResult;
use crate::db::{KeyValue, DB};
use crate::lists::List;
use crate::resp::Resp;
use crate::sorted_list::{GeoPoint, SortedList};
use crate::valuetype::ValueType;
impl GeoCommands {  
    pub fn geosearch(db: &mut DB, key: String, longitude: f64, latitude: f64, radius: f64) -> CommandResult {
        let results = match db.database.get(key.as_bytes()) {
            Some(kv) => match &kv.value {
                ValueType::SortedList(sorted_list) => sorted_list.geosearch(GeoPoint { lat: latitude, lon: longitude }, radius),
                _ => return CommandResult::Response(Resp::Error(b"WRONGTYPE Operation against a key holding the wrong kind of value".to_vec())),
            },
            None => Vec::new(),
        };
        CommandResult::Response(Resp::from_strings(results))
    }
    pub fn geodist(db: &mut DB, key: String, member1: String, member2: String) -> CommandResult {
        let distance = match db.database.get(key.as_bytes()) {
            Some(kv) => match &kv.value {
                ValueType::SortedList(sorted_list) => sorted_list.geodist(&member1, &member2),
                _ => None,
            },
            None => None,
        };
        if let Some(dist) = distance {
            CommandResult::Response(Resp::BulkString(format!("{}", dist).into_bytes()))
        } else {
            CommandResult::Response(Resp::NullBulkString)
        }
    }
    pub fn geoadd(db: &mut DB, key: String, longitude: f64, latitude: f64, member: String) -> CommandResult {
        if latitude < -85.05112878 || latitude > 85.05112878 {
            return CommandResult::Response(Resp::Error(format!("ERR invalid longitude,latitude pair {},{}", longitude, latitude).into_bytes()));
        }
        if longitude < -180.0 || longitude > 180.0 {
            return CommandResult::Response(Resp::Error(format!("ERR invalid longitude,latitude pair {},{}", longitude, latitude).into_bytes()));
        }

        let geo_list = db.database.entry(key.into_bytes()).or_insert_with(|| KeyValue { expiry: None, value: ValueType::SortedList(SortedList::new()) });
        match &mut geo_list.value {
            ValueType::SortedList(gl) => {
                let is_new = gl.geoadd(member, GeoPoint { lat: latitude, lon: longitude });
                CommandResult::Response(Resp::Integer(if is_new { 1 } else { 0 }))
            },
            _ => CommandResult::Response(Resp::Error(b"WRONGTYPE Operation against a key holding the wrong kind of value".to_vec())),
        }
    }
    pub fn geopos(db: &mut DB, key: String, members: Vec<String>) -> CommandResult {
        let mut res = Vec::new();
         match db.database.get(key.as_bytes()) {
            
            Some(kv) => match &kv.value {
                ValueType::SortedList(sorted_list) => {
                                    for member in members {

                    let geo_point = sorted_list.geopos(&member);
                    if let Some(geo_point) = geo_point {
                        let lat = geo_point.lat;
                        let lon = geo_point.lon;
                        res.push(Resp::Array(vec![
                                                    Resp::BulkString(format!("{}", lon).into_bytes()),
                                                    Resp::BulkString(format!("{}", lat).into_bytes()),
                                                ].into()));
                    } else {
                        res.push(Resp::NullArray);
                    }
                }
                },
                _ => return CommandResult::Response(Resp::Error(b"WRONGTYPE Operation against a key holding the wrong kind of value".to_vec())),
            },
            None => return CommandResult::Response(Resp::Array(vec![Resp::NullArray; members.len()].into())),
        }
        CommandResult::Response(Resp::Array(res.into()))
}}