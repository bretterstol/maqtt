//! What `to geo` shows: one marker per key, where it is now, and a short trail of where it was.
//! A single point is kept under the topic (or `$key` after a `by`); a list is a snapshot of
//! the whole map, its points kept under their labels.

use std::collections::{HashMap, HashSet};

use serde::{Serialize, Serializer};

/// Earlier positions kept per marker.
pub const TRAIL: usize = 20;
/// Markers kept at most; past that, the one updated longest ago goes.
pub const MAX_MARKERS: usize = 1_000;

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Marker {
    pub key: String,
    pub label: Option<String>,
    pub lat: f64,
    pub lon: f64,
    /// Where it was before, oldest first, as `[lat, lon]`.
    pub trail: Vec<[f64; 2]>,
    pub at_ms: u64,
}

/// One point for the map.
#[derive(Debug, Clone, PartialEq)]
pub struct Point {
    pub key: String,
    pub label: Option<String>,
    pub lat: f64,
    pub lon: f64,
}

/// The markers of one Pipeline (or one Preview), sent to the tab sorted by key.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Markers {
    by_key: HashMap<String, Marker>,
}

impl Markers {
    /// Moves a key's marker to a new position, or places a new one.
    pub fn place(&mut self, key: String, label: Option<String>, lat: f64, lon: f64, at_ms: u64) {
        if let Some(marker) = self.by_key.get_mut(&key) {
            if marker.trail.len() == TRAIL {
                marker.trail.remove(0);
            }
            marker.trail.push([marker.lat, marker.lon]);
            (marker.lat, marker.lon, marker.at_ms) = (lat, lon, at_ms);
            if label.is_some() {
                marker.label = label;
            }
            return;
        }
        if self.by_key.len() == MAX_MARKERS
            && let Some(oldest) = self
                .by_key
                .values()
                .min_by_key(|m| m.at_ms)
                .map(|m| m.key.clone())
        {
            self.by_key.remove(&oldest);
        }
        self.by_key.insert(
            key.clone(),
            Marker {
                key,
                label,
                lat,
                lon,
                trail: Vec::new(),
                at_ms,
            },
        );
    }

    /// Makes the map exactly these points: markers they name move on (keeping their trails),
    /// new ones are placed, and the rest are removed.
    pub fn replace(&mut self, points: Vec<Point>, at_ms: u64) {
        let named: HashSet<&str> = points.iter().map(|p| p.key.as_str()).collect();
        self.by_key.retain(|key, _| named.contains(key.as_str()));
        for point in points {
            self.place(point.key, point.label, point.lat, point.lon, at_ms);
        }
    }

    pub fn list(&self) -> Vec<&Marker> {
        let mut list: Vec<&Marker> = self.by_key.values().collect();
        list.sort_by(|a, b| a.key.cmp(&b.key));
        list
    }
}

impl Serialize for Markers {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(self.list())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_marker_moves_and_leaves_a_short_trail() {
        let mut markers = Markers::default();
        for i in 0..30 {
            markers.place("truck".into(), None, 60.0 + f64::from(i), 10.0, i as u64);
        }
        markers.place("truck".into(), Some("T1".into()), 1.0, 2.0, 99);
        let list = markers.list();
        assert_eq!(list.len(), 1);
        let truck = list[0];
        assert_eq!(
            (truck.lat, truck.lon, truck.label.as_deref()),
            (1.0, 2.0, Some("T1"))
        );
        assert_eq!(truck.trail.len(), TRAIL);
        assert_eq!(truck.trail.last(), Some(&[89.0, 10.0]));
    }

    #[test]
    fn past_the_limit_the_longest_quiet_marker_goes() {
        let mut markers = Markers::default();
        for i in 0..MAX_MARKERS {
            markers.place(format!("{i:04}"), None, 0.0, 0.0, 10 + i as u64);
        }
        markers.place("0000".into(), None, 1.0, 1.0, 5_000);
        markers.place("new".into(), None, 0.0, 0.0, 6_000);
        assert_eq!(markers.list().len(), MAX_MARKERS);
        let keys: Vec<&str> = markers.list().iter().map(|m| m.key.as_str()).collect();
        assert!(keys.contains(&"0000") && keys.contains(&"new") && !keys.contains(&"0001"));
    }

    #[test]
    fn a_snapshot_keeps_named_markers_and_their_trails_and_drops_the_rest() {
        let point = |key: &str, lat: f64| Point {
            key: key.into(),
            label: Some(key.into()),
            lat,
            lon: 0.0,
        };
        let mut markers = Markers::default();
        markers.place("single".into(), None, 1.0, 1.0, 1);
        markers.replace(vec![point("a", 1.0), point("b", 1.0)], 2);
        markers.replace(vec![point("a", 2.0), point("c", 1.0)], 3);
        let list = markers.list();
        let keys: Vec<&str> = list.iter().map(|m| m.key.as_str()).collect();
        assert_eq!(keys, ["a", "c"]);
        assert_eq!(
            (list[0].lat, list[0].trail.clone()),
            (2.0, vec![[1.0, 0.0]])
        );

        markers.replace(Vec::new(), 4);
        assert!(markers.list().is_empty());
    }
}
