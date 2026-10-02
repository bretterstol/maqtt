//! What `to chart` shows: a line per series (the topic, `$key` after a `by`, or the value's own
//! `series`), each its recent values over time.
//!
//! Lines keep the order they first appeared in, which is what their colour follows, so a new
//! series never repaints the others. Past [`MAX_LINES`] a series isn't drawn but counted: the
//! chart's palette has that many distinct colours, and a generated ninth would be confused
//! with one of them.

use std::collections::VecDeque;

use serde::Serialize;

/// Values kept per line.
pub const POINTS: usize = 600;
/// Lines drawn at most.
pub const MAX_LINES: usize = 8;
/// Series past the limit remembered by name, to count them.
const MAX_HIDDEN: usize = 1_000;

#[derive(Debug, Clone, PartialEq, Serialize, ts_rs::TS)]
#[ts(export, export_to = "processing/generated/")]
pub struct Line {
    pub name: String,
    /// `[time in ms, value]`, oldest first.
    #[ts(as = "Vec<[f64; 2]>")]
    pub points: VecDeque<[f64; 2]>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
#[ts(export, export_to = "processing/generated/")]
pub struct Lines {
    pub lines: Vec<Line>,
    /// Series that arrived once the chart was full, and aren't drawn.
    pub hidden: usize,
    #[serde(skip)]
    hidden_names: Vec<String>,
}

impl Lines {
    pub fn add(&mut self, series: &str, at_ms: u64, value: f64) {
        let line = match self.lines.iter().position(|l| l.name == series) {
            Some(i) => &mut self.lines[i],
            None if self.lines.len() < MAX_LINES => {
                self.lines.push(Line {
                    name: series.to_string(),
                    points: VecDeque::new(),
                });
                self.lines.last_mut().unwrap()
            }
            None => {
                if self.hidden_names.len() < MAX_HIDDEN
                    && !self.hidden_names.iter().any(|n| n == series)
                {
                    self.hidden_names.push(series.to_string());
                    self.hidden = self.hidden_names.len();
                }
                return;
            }
        };
        if line.points.len() == POINTS {
            line.points.pop_front();
        }
        line.points.push_back([at_ms as f64, value]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lines_keep_their_order_and_recent_values() {
        let mut lines = Lines::default();
        for i in 0..(POINTS + 10) {
            lines.add("b", i as u64, i as f64);
        }
        lines.add("a", 5, 1.0);
        assert_eq!(
            lines
                .lines
                .iter()
                .map(|l| l.name.as_str())
                .collect::<Vec<_>>(),
            ["b", "a"]
        );
        let b = &lines.lines[0].points;
        assert_eq!(
            (b.len(), b[0], b[POINTS - 1]),
            (POINTS, [10.0, 10.0], [609.0, 609.0])
        );
    }

    #[test]
    fn series_past_the_palette_are_counted_not_drawn() {
        let mut lines = Lines::default();
        for i in 0..MAX_LINES + 3 {
            lines.add(&format!("s{i}"), 0, 1.0);
            lines.add(&format!("s{i}"), 1, 2.0);
        }
        assert_eq!((lines.lines.len(), lines.hidden), (MAX_LINES, 3));
    }
}
