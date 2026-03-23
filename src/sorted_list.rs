use std::collections::{BTreeSet, HashSet};
use ordered_float::OrderedFloat;

pub struct SortedList {
    set: BTreeSet<(OrderedFloat<f64>, String)>,
    values: HashSet<String>,
}

impl SortedList {
    /// Create a new empty SortedList
    pub fn new() -> Self {
        Self {
            set: BTreeSet::new(),
            values: HashSet::new(),
        }
    }
  pub fn insert_or_update(&mut self, value: String, score: f64) -> bool {
        if self.values.contains(&value) {
            if let Some(old_pair) = self
                .set
                .iter()
                .find(|(_, v)| *v == value)
                .cloned()
            {
                self.set.remove(&old_pair);
            }
            self.set.insert((OrderedFloat(score), value));
            false
            
        } else {
            self.set.insert((OrderedFloat(score), value.clone()));
            self.values.insert(value);
            true 
        }}
    /// Insert a value with a score
    pub fn insert(&mut self, value: String, score: f64) {
        if self.set.insert((OrderedFloat(score), value.clone())) {
            self.values.insert(value);
        }
    }

    /// Remove an exact value with a score
    pub fn remove(&mut self, value: &str, score: f64) -> bool {
        if self.set.remove(&(OrderedFloat(score), value.to_string())) {
            self.values.remove(value);
            true
        } else {
            false
        }
    }

    /// Check if a (value, score) pair exists
    pub fn contains(&self, value: &str, score: f64) -> bool {
        self.set.contains(&(OrderedFloat(score), value.to_string()))
    }

    /// Check if a value exists regardless of score (fast O(1))
    pub fn contains_value(&self, value: &str) -> bool {
        self.values.contains(value)
    }

    /// Number of items in the set
    pub fn len(&self) -> usize {
        self.set.len()
    }

    /// Is the set empty?
    pub fn is_empty(&self) -> bool {
        self.set.is_empty()
    }

    /// Iterate over items (sorted by score ascending)
    pub fn iter(&self) -> impl Iterator<Item = &(OrderedFloat<f64>, String)> {
        self.set.iter()
    }

    /// Get the lowest-score item
    pub fn min(&self) -> Option<&(OrderedFloat<f64>, String)> {
        self.set.first()
    }

    /// Get the highest-score item
    pub fn max(&self) -> Option<&(OrderedFloat<f64>, String)> {
        self.set.last()
    }

    /// Get top N highest scores
    pub fn top_n(&self, n: usize) -> Vec<&(OrderedFloat<f64>, String)> {
        self.set.iter().rev().take(n).collect()
    }

    /// Get bottom N lowest scores
    pub fn bottom_n(&self, n: usize) -> Vec<&(OrderedFloat<f64>, String)> {
        self.set.iter().take(n).collect()
    }

    /// Clear everything
    pub fn clear(&mut self) {
        self.set.clear();
        self.values.clear();
    }
}