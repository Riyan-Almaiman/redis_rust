use std::collections::BTreeSet;
use ordered_float::OrderedFloat;

pub struct SortedList {
    set: BTreeSet<(OrderedFloat<f64>, String)>,
}

impl SortedList {
    /// Create a new empty SortedList
    pub fn new() -> Self {
        Self {
            set: BTreeSet::new(),
        }
    }

    /// Insert a value with a score
    pub fn insert(&mut self, value: String, score: f64) {
        self.set.insert((OrderedFloat(score), value));
    }

    /// Remove an exact value with a score
    pub fn remove(&mut self, value: &str, score: f64) -> bool {
        self.set.remove(&(OrderedFloat(score), value.to_string()))
    }

    /// Get the lowest-score item
    pub fn min(&self) -> Option<&(OrderedFloat<f64>, String)> {
        self.set.first()
    }

    /// Get the highest-score item
    pub fn max(&self) -> Option<&(OrderedFloat<f64>, String)> {
        self.set.last()
    }

    /// Check if a value with a score exists
    pub fn contains(&self, value: &str, score: f64) -> bool {
        self.set.contains(&(OrderedFloat(score), value.to_string()))
    }

    /// Number of items in the set
    pub fn len(&self) -> usize {
        self.set.len()
    }

    /// Iterate over items (sorted by score ascending)
    pub fn iter(&self) -> impl Iterator<Item = &(OrderedFloat<f64>, String)> {
        self.set.iter()
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
    }
}