//! What a Pipeline's Sinks show: the one place that knows what each Effect does to the visible
//! outputs (markers, chart lines, inspected values, counts, errors), shared by a running
//! Pipeline and by Preview.
//!
//! What has to leave måqtt's own memory (a Derived Topic onto the bus, a republished message,
//! a line in a file) comes back as a [`Delivery`] for the caller: a running Pipeline delivers
//! it, Preview lists it as what would be sent. Nothing here touches threads, files or the
//! network, so it's tested by feeding it Effects.

use std::collections::VecDeque;

use serde::Serialize;

use super::lang::{FileFormat, Rotate};
use super::lines::Lines;
use super::markers::Markers;
use super::stages::Effect;

/// `inspect` values kept, newest last.
pub const INSPECTED_KEPT: usize = 50;

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Inspected {
    pub label: Option<String>,
    pub topic: String,
    pub value: String,
    pub at_ms: u64,
}

/// A Pipeline's visible outputs, as the tab shows them.
#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Outputs {
    /// Values its Sinks took: delivered Derived Topics, messages and file lines, map points
    /// and chart values.
    pub emitted: u64,
    pub errors: u64,
    pub last_error: Option<String>,
    /// The latest `inspect` values, newest last.
    pub inspected: VecDeque<Inspected>,
    /// Where its `to geo` values put their markers.
    pub markers: Markers,
    /// Its `to chart` values, a line per series.
    pub chart: Lines,
}

/// What a Sink sends out of måqtt's own memory, for the caller to deliver (or, in Preview,
/// to show as what would be sent). Once delivered, the caller says so with
/// [`Outputs::delivered`] or [`Outputs::failed`].
#[derive(Debug, Clone, PartialEq)]
pub enum Delivery {
    /// A Derived Topic, its path within the Pipeline.
    Derived { topic: String, payload: Vec<u8> },
    /// A message to republish on the Connection with this name.
    Republish {
        broker: String,
        topic: String,
        payload: Vec<u8>,
    },
    /// A value for a file.
    Write {
        path: String,
        format: FileFormat,
        rotate: Option<Rotate>,
        record: serde_json::Value,
    },
}

impl Outputs {
    /// Takes in what a Stage produced at `at_ms`; returns what still has to be delivered.
    pub fn record(&mut self, effect: Effect, at_ms: u64) -> Option<Delivery> {
        match effect {
            Effect::Derived { topic, payload } => Some(Delivery::Derived { topic, payload }),
            Effect::Republish {
                broker,
                topic,
                payload,
            } => Some(Delivery::Republish {
                broker,
                topic,
                payload,
            }),
            Effect::Write {
                path,
                format,
                rotate,
                record,
            } => Some(Delivery::Write {
                path,
                format,
                rotate,
                record,
            }),
            Effect::Point {
                key,
                label,
                lat,
                lon,
                at_ms,
            } => {
                self.emitted += 1;
                self.markers.place(key, label, lat, lon, at_ms);
                None
            }
            Effect::Snapshot { points, at_ms } => {
                self.emitted += 1;
                self.markers.replace(points, at_ms);
                None
            }
            Effect::Sample {
                series,
                value,
                at_ms,
            } => {
                self.emitted += 1;
                self.chart.add(&series, at_ms, value);
                None
            }
            Effect::Inspected {
                label,
                topic,
                value,
            } => {
                if self.inspected.len() == INSPECTED_KEPT {
                    self.inspected.pop_front();
                }
                self.inspected.push_back(Inspected {
                    label,
                    topic,
                    value,
                    at_ms,
                });
                None
            }
            Effect::Error(error) => {
                self.failed(error.to_string());
                None
            }
        }
    }

    /// A [`Delivery`] went out.
    pub fn delivered(&mut self) {
        self.emitted += 1;
    }

    /// A [`Delivery`] (or a value) couldn't go out, for this reason.
    pub fn failed(&mut self, error: String) {
        self.errors += 1;
        self.last_error = Some(error);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::processing::markers::Point;
    use crate::processing::stages::StageError;

    #[test]
    fn shown_effects_are_kept_and_deliveries_handed_back() {
        let mut outputs = Outputs::default();
        let derived = outputs.record(
            Effect::Derived {
                topic: "t".into(),
                payload: b"1".to_vec(),
            },
            0,
        );
        assert_eq!(
            derived,
            Some(Delivery::Derived {
                topic: "t".into(),
                payload: b"1".to_vec()
            })
        );
        assert_eq!(outputs.emitted, 0, "counted once it's delivered");
        outputs.delivered();

        let shown = [
            Effect::Point {
                key: "a".into(),
                label: None,
                lat: 1.0,
                lon: 2.0,
                at_ms: 5,
            },
            Effect::Snapshot {
                points: vec![Point {
                    key: "b".into(),
                    label: None,
                    lat: 3.0,
                    lon: 4.0,
                }],
                at_ms: 6,
            },
            Effect::Sample {
                series: "s".into(),
                value: 7.0,
                at_ms: 7,
            },
            Effect::Inspected {
                label: Some("x".into()),
                topic: "t".into(),
                value: "1".into(),
            },
        ];
        for effect in shown {
            assert_eq!(outputs.record(effect, 9), None);
        }
        assert_eq!(outputs.emitted, 4);
        let keys: Vec<&str> = outputs
            .markers
            .list()
            .iter()
            .map(|m| m.key.as_str())
            .collect();
        assert_eq!(keys, ["b"], "the snapshot replaced the point");
        assert_eq!(outputs.chart.lines[0].points[0], [7.0, 7.0]);
        assert_eq!(outputs.inspected[0].at_ms, 9);

        let bad = StageError {
            stage: "map".into(),
            topic: "t".into(),
            reason: "bad".into(),
        };
        assert_eq!(outputs.record(Effect::Error(bad), 0), None);
        outputs.failed("worse".into());
        assert_eq!(
            (outputs.errors, outputs.last_error.as_deref()),
            (2, Some("worse"))
        );
    }

    #[test]
    fn only_so_many_inspected_values_are_kept() {
        let mut outputs = Outputs::default();
        for i in 0..INSPECTED_KEPT + 5 {
            outputs.record(
                Effect::Inspected {
                    label: None,
                    topic: "t".into(),
                    value: i.to_string(),
                },
                0,
            );
        }
        assert_eq!(outputs.inspected.len(), INSPECTED_KEPT);
        assert_eq!(outputs.inspected[0].value, "5");
    }
}
