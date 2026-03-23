use std::collections::{BTreeMap, BTreeSet, HashMap};
use ordered_float::OrderedFloat;

pub struct SortedList {
    scores: BTreeMap<OrderedFloat<f64>, BTreeSet<String>>, // score -> set of values
    values: HashMap<String, OrderedFloat<f64>>,           // value -> score
}

impl SortedList {
    pub fn new() -> Self {
        Self {
            scores: BTreeMap::new(),
            values: HashMap::new(),
        }
    }

    /// Insert or update a value. Returns true if created, false if updated.
    pub fn insert_or_update(&mut self, value: String, score: f64) -> bool {
        let score_ord = OrderedFloat(score);
        if let Some(&old_score) = self.values.get(&value) {
            if old_score == score_ord {
                return false; // same score, nothing to do
            }
            // remove from old score set
            if let Some(set) = self.scores.get_mut(&old_score) {
                set.remove(&value);
                if set.is_empty() {
                    self.scores.remove(&old_score);
                }
            }
            // insert into new score
            self.scores.entry(score_ord).or_default().insert(value.clone());
            self.values.insert(value, score_ord);
            false
        } else {
            self.scores.entry(score_ord).or_default().insert(value.clone());
            self.values.insert(value, score_ord);
            true
        }
    }

    /// Remove a value
    pub fn remove(&mut self, value: &str) -> bool {
        if let Some(score) = self.values.remove(value) {
            if let Some(set) = self.scores.get_mut(&score) {
                set.remove(value);
                if set.is_empty() {
                    self.scores.remove(&score);
                }
            }
            true
        } else {
            false
        }
    }

pub fn rank_of(&self, value: &str) -> Option<usize> {
    let score = *self.values.get(value)?;
    let mut rank = 0;
    for (&s, set) in &self.scores {
        if s < score {
            rank += set.len();
        } else if s == score {
            let mut lex_rank = 0; 
            for v in set {
                if v == value {
                    return Some(rank + lex_rank); 
                }
                lex_rank += 1;
            }
        } else {
            break;
        }
    }
    None
}
    /// Get min item
    pub fn min(&self) -> Option<(&OrderedFloat<f64>, &str)> {
        self.scores.iter().next().and_then(|(score, set)| set.iter().next().map(|v| (score, v.as_str())))
    }

    /// Get max item
    pub fn max(&self) -> Option<(&OrderedFloat<f64>, &str)> {
        self.scores.iter().next_back().and_then(|(score, set)| set.iter().next_back().map(|v| (score, v.as_str())))
    }

    /// Top N items
    pub fn top_n(&self, n: usize) -> Vec<(&OrderedFloat<f64>, &str)> {
        let mut result = Vec::new();
        for (score, set) in self.scores.iter().rev() {
            for v in set.iter().rev() {
                result.push((score, v.as_str()));
                if result.len() == n {
                    return result;
                }
            }
        }
        result
    }

    /// Bottom N items
    pub fn bottom_n(&self, n: usize) -> Vec<(&OrderedFloat<f64>, &str)> {
        let mut result = Vec::new();
        for (score, set) in &self.scores {
            for v in set {
                result.push((score, v.as_str()));
                if result.len() == n {
                    return result;
                }
            }
        }
        result
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn contains_value(&self, value: &str) -> bool {
        self.values.contains_key(value)
    }

    pub fn clear(&mut self) {
        self.scores.clear();
        self.values.clear();
    }
}