use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use ordered_float::{Float, FloatCore, OrderedFloat, Pow};
use crate::resp::Resp;

/// 2D point
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GeoPoint {
    pub lat: f64,
    pub lon: f64,
}

/// A unified sorted list supporting numeric or geo items
pub struct SortedList {
    pub scores: BTreeMap<OrderedFloat<f64>, BTreeSet<String>>, // score -> set of names
    pub values: HashMap<String, OrderedFloat<f64>>,           // name -> score
    pub geo_points: HashMap<String, GeoPoint>,               // optional geo info
}
const MIN_LATITUDE: f64 = -85.05112878;
const MAX_LATITUDE: f64 = 85.05112878;
const MIN_LONGITUDE: f64 = -180.0;
const MAX_LONGITUDE: f64 = 180.0;
const LATITUDE_RANGE: f64 = MAX_LATITUDE - MIN_LATITUDE;
const LONGITUDE_RANGE: f64 = MAX_LONGITUDE - MIN_LONGITUDE;

impl SortedList {
    pub fn new() -> Self {
        Self {
            scores: BTreeMap::new(),
            values: HashMap::new(),
            geo_points: HashMap::new(),
        }
    }

    /// Insert/update numeric score
    pub fn zadd(&mut self, name: String, score: f64) -> bool {
        let score_ord = OrderedFloat(score);
        let mut is_new = true;
        if let Some(&old_score) = self.values.get(&name) {
            is_new = false; 
            if old_score == score_ord {
                return false;
            }
            if let Some(set) = self.scores.get_mut(&old_score) {
                set.remove(&name);
                if set.is_empty() {
                    self.scores.remove(&old_score);
                }
            }
        }

        self.scores.entry(score_ord).or_default().insert(name.clone());
        self.values.insert(name, score_ord);
        is_new
    }

    /// Insert/update geo point (doesn't change numeric score unless given explicitly)
    pub fn geoadd(&mut self, name: String, point: GeoPoint) -> bool {
        let mut normalized_lat = ((2.0.pow(26) * (point.lat - MIN_LATITUDE) / LATITUDE_RANGE) as f64).trunc();
        let mut normalized_lon = ((2.0.pow(26)  * (point.lon - MIN_LONGITUDE) / LONGITUDE_RANGE) as f64).trunc();
        let result = SortedList::interleave(normalized_lat, normalized_lon);
        
        self.geo_points.insert(name.clone(), point);
        // If no score exists yet, assign 0
        if !self.values.contains_key(&name) {
            self.zadd(name, result);
            true
        } else {
            false
        }
    }

    /// Remove a member
    pub fn remove(&mut self, name: &str) -> bool {
        if let Some(&score) = self.values.get(name) {
            if let Some(set) = self.scores.get_mut(&score) {
                set.remove(name);
                if set.is_empty() { self.scores.remove(&score); }
            }
            self.values.remove(name);
            self.geo_points.remove(name);
            true
        } else { false }
    }

    /// Get rank (0-based)
    pub fn rank_of(&self, name: &str) -> Option<usize> {
        let score = *self.values.get(name)?;
        let mut rank = 0;
        for (&s, set) in &self.scores {
            if s < score { rank += set.len(); }
            else if s == score {
                let mut lex_rank = 0;
                for v in set {
                    if v == name { return Some(rank + lex_rank); }
                    lex_rank += 1;
                }
            } else { break; }
        }
        None
    }
    pub fn interleave(x: f64, y: f64) -> f64 {
        let x_int = SortedList::spread(x as u64);
        let y_int = SortedList::spread(y as u64);
        let y_shifted = y_int << 1;
        let interleaved = x_int | y_shifted;
        interleaved as f64
    }
    fn spread(v: u64) -> u64 {
        let mut v =  v & 0xFFFFFFFF;
        v = (v | (v << 16)) & 0x0000FFFF0000FFFF;
        v = (v | (v << 8))  & 0x00FF00FF00FF00FF;
        v = (v | (v << 4))  & 0x0F0F0F0F0F0F0F0F;
        v = (v | (v << 2))  & 0x3333333333333333;
        v = (v | (v << 1))  & 0x5555555555555555;
        v 
    }
    /// Zrange with support for negative indices; returns VecDeque<Resp>
    pub fn zrange(&self, start: isize, end: isize) -> VecDeque<Resp> {
        let mut result = VecDeque::new();
        let total_len: usize = self.scores.values().map(|s| s.len()).sum();
        if total_len == 0 { return result; }

        let start_idx = if start < 0 { (total_len as isize + start).max(0) as usize } else { (start as usize).min(total_len) };
        let end_idx = if end < 0 { (total_len as isize + end).max(0) as usize } else { (end as usize).min(total_len - 1) };
        if start_idx > end_idx { return result; }

        let mut idx = 0;
        for (_score, set) in &self.scores {
            for name in set {
                if idx >= start_idx && idx <= end_idx {
                                          result.push_back(Resp::BulkString(name.clone().into_bytes()));

                }
                idx += 1;
                if idx > end_idx { return result; }
            }
        }
        result
    }

    /// Get numeric score
    pub fn zscore(&self, name: &str) -> Option<f64> {
        self.values.get(name).map(|s| s.0)
    }

    /// Length
    pub fn len(&self) -> usize {
        self.values.len()
    }
}