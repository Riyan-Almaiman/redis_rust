use std::collections::{BTreeMap, HashMap, VecDeque};
use ordered_float::OrderedFloat;

use crate::resp::Resp;

/// A simple 2D point
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GeoPoint {
    pub lat: f64,
    pub lon: f64,
}

/// A struct for geospatial items: name -> point
pub struct GeoList {
    pub points: HashMap<String, GeoPoint>, 
    pub by_lat: BTreeMap<OrderedFloat<f64>, Vec<String>>, // latitude -> names
}

impl GeoList {
    /// Create a new empty GeoList
    pub fn new() -> Self {
        Self {
            points: HashMap::new(),
            by_lat: BTreeMap::new(),
        }
    }

    /// Insert or update a point. Returns true if new, false if updated
    pub fn insert_or_update(&mut self, name: String, lat: f64, lon: f64) -> bool {
        let is_new = !self.points.contains_key(&name);
        if let Some(old) = self.points.insert(name.clone(), GeoPoint { lat, lon }) {
            // Remove from old latitude map
            let old_lat_ord = OrderedFloat(old.lat);
            if let Some(vec) = self.by_lat.get_mut(&old_lat_ord) {
                vec.retain(|n| n != &name);
                if vec.is_empty() {
                    self.by_lat.remove(&old_lat_ord);
                }
            }
        }

        let lat_ord = OrderedFloat(lat);
        self.by_lat.entry(lat_ord).or_default().push(name);
        is_new
    }

    /// Remove a point by name
    pub fn remove(&mut self, name: &str) -> bool {
        if let Some(point) = self.points.remove(name) {
            let lat_ord = OrderedFloat(point.lat);
            if let Some(vec) = self.by_lat.get_mut(&lat_ord) {
                vec.retain(|n| n != name);
                if vec.is_empty() {
                    self.by_lat.remove(&lat_ord);
                }
            }
            true
        } else {
            false
        }
    }

    /// Check if a point exists
    pub fn contains(&self, name: &str) -> bool {
        self.points.contains_key(name)
    }

    /// Get the point by name
    pub fn get(&self, name: &str) -> Option<&GeoPoint> {
        self.points.get(name)
    }

    /// Range query by latitude (inclusive)
    pub fn range_lat(&self, min_lat: f64, max_lat: f64) -> VecDeque<Resp> {
        let mut result = VecDeque::new();
        let min_ord = OrderedFloat(min_lat);
        let max_ord = OrderedFloat(max_lat);

        for (_lat, names) in self.by_lat.range(min_ord..=max_ord) {
            for name in names {
                if let Some(point) = self.points.get(name) {
                    result.push_back(Resp::BulkString(
                        format!("{}:{}:{}", name, point.lat, point.lon).into_bytes()
                    ));
                }
            }
        }
        result
    }

    /// Count of points
    pub fn len(&self) -> usize {
        self.points.len()
    }

    /// Clear all points
    pub fn clear(&mut self) {
        self.points.clear();
        self.by_lat.clear();
    }
}