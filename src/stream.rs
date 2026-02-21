use crate::commands::StreamEntryIdCommandType;
use std::collections::{BTreeMap, HashMap};
use std::time::{SystemTime, UNIX_EPOCH};
use crate::resp::Resp;

#[derive(Debug, Clone)]
pub struct EntryId {
    pub time: u64,
    pub sequence: u64,
}

impl EntryId {
    pub fn get_id_string(&self) -> String {
        format!("{}-{}", self.time, self.sequence)
    }
}

#[derive(Debug, Clone)]
pub struct StreamEntry {
    pub entry_id: EntryId,
    pub fields: Vec<(Vec<u8>, Vec<u8>)>,
}

#[derive(Debug, Clone)]
pub struct Sequences {
    pub entries: BTreeMap<u64, StreamEntry>,
    pub sequence_count: u64,
}

#[derive(Debug, Clone)]
pub struct Stream {
    pub time_stamp_entries: BTreeMap<u64, Sequences>,
    pub last_id: Option<EntryId>,
}

impl Stream {
    pub fn new() -> Self {
        Stream {
            time_stamp_entries: BTreeMap::new(),
            last_id: None,
        }
    }
    pub fn get_range(&self, start_time: u64, end_time: u64, start_sequence: u64, end_sequence: u64) -> Resp {
        let mut results = Vec::new();
        println!("{start_time}- {end_time}");
        for (timestamp, sequences) in self.time_stamp_entries.range(start_time..=end_time) {
            for (sequence, entry) in sequences.entries.range(start_sequence..=end_sequence) {
                let id_str = entry.entry_id.get_id_string();
                let mut fields_resp = Vec::new();
                for (k, v) in &entry.fields {
                    fields_resp.push(Resp::BulkString(k.clone()));
                    fields_resp.push(Resp::BulkString(v.clone()));
                }

                results.push(Resp::Array(vec![
                    Resp::BulkString(id_str.into_bytes()),
                    Resp::Array(fields_resp),
                ]));
            }
        }

        Resp::Array(results)
    }
    pub fn add_entry(
        &mut self,
        fields: Vec<(Vec<u8>, Vec<u8>)>,
        id_type: StreamEntryIdCommandType,
    ) -> Result<EntryId, String> {
        let generated_id = match id_type {
            StreamEntryIdCommandType::Explicit { sequence, time } => {
                let id = EntryId { sequence, time };
                self.validate_id_greater_than_last(&id)?;
                id
            }
            StreamEntryIdCommandType::GenerateTimeAndSequence => self.auto_generate_id(),
            StreamEntryIdCommandType::GenerateOnlySequence { time } => {
                self.auto_generate_seq(time)?
            }
        };

        let entry = StreamEntry {
            entry_id: generated_id.clone(),
            fields,
        };

        let sequences = self
            .time_stamp_entries
            .entry(generated_id.time)
            .or_insert(Sequences {
                entries: BTreeMap::new(),
                sequence_count: 0,
            });

        sequences.sequence_count = generated_id.sequence;
        sequences.entries.insert(generated_id.sequence, entry);

        self.last_id = Some(generated_id.clone());

        Ok(generated_id)
    }

    fn validate_id_greater_than_last(&self, new_id: &EntryId) -> Result<(), String> {
        if new_id.time == 0 && new_id.sequence == 0 {
            return Err("ERR The ID specified in XADD must be greater than 0-0".into());
        }

        if let Some(last) = &self.last_id {
            if new_id.time < last.time
                || (new_id.time == last.time && new_id.sequence <= last.sequence)
            {
                return Err("ERR The ID specified in XADD is equal or smaller than the target stream top item".into());
            }
        }
        Ok(())
    }

    fn auto_generate_id(&self) -> EntryId {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        match &self.last_id {
            Some(last) if now <= last.time => EntryId {
                time: last.time,
                sequence: last.sequence + 1,
            },
            _ => EntryId {
                time: now,
                sequence: (now == 0) as u64,
            },
        }
    }

    fn auto_generate_seq(&self, time: u64) -> Result<EntryId, String> {
        let sequence = match &self.last_id {
            Some(last) if time == last.time => last.sequence + 1,
            _ if time == 0 => 1,
            _ => 0,
        };
        let new_id = EntryId { time, sequence };
        self.validate_id_greater_than_last(&new_id)?;
        Ok(new_id)
    }
}
