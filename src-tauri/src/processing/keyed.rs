//! State kept per key (a topic, or a `by` value) with a cap, so a Pipeline over ever-new topics
//! or keys can't grow without end.

use std::collections::HashMap;

/// The most keys one Stage keeps state for.
pub const MAX_KEYS: usize = 10_000;

/// A map that forgets its least recently used tenth once it's full.
pub struct Keyed<T> {
    entries: HashMap<String, (T, u64)>,
    clock: u64,
    cap: usize,
}

impl<T> Default for Keyed<T> {
    fn default() -> Self {
        Self::with_cap(MAX_KEYS)
    }
}

impl<T> Keyed<T> {
    pub fn with_cap(cap: usize) -> Self {
        Self {
            entries: HashMap::new(),
            clock: 0,
            cap: cap.max(1),
        }
    }

    fn tick(&mut self) -> u64 {
        self.clock += 1;
        self.clock
    }

    pub fn get(&mut self, key: &str) -> Option<&T> {
        let now = self.tick();
        self.entries.get_mut(key).map(|(value, used)| {
            *used = now;
            &*value
        })
    }

    pub fn insert(&mut self, key: String, value: T) {
        if !self.entries.contains_key(&key) {
            self.make_room();
        }
        let now = self.tick();
        self.entries.insert(key, (value, now));
    }

    /// The value for `key`, made with `make` if there is none.
    pub fn entry(&mut self, key: String, make: impl FnOnce() -> T) -> &mut T {
        if !self.entries.contains_key(&key) {
            self.make_room();
        }
        let now = self.tick();
        let (value, used) = self.entries.entry(key).or_insert_with(|| (make(), now));
        *used = now;
        value
    }

    pub fn remove(&mut self, key: &str) -> Option<T> {
        self.entries.remove(key).map(|(value, _)| value)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &T)> {
        self.entries.iter().map(|(key, (value, _))| (key, value))
    }

    pub fn values_mut(&mut self) -> impl Iterator<Item = &mut T> {
        self.entries.values_mut().map(|(value, _)| value)
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// When full, forgets the least recently used tenth, so this runs once per that many new keys.
    fn make_room(&mut self) {
        if self.entries.len() < self.cap {
            return;
        }
        let mut by_age: Vec<(u64, String)> = self
            .entries
            .iter()
            .map(|(key, (_, used))| (*used, key.clone()))
            .collect();
        by_age.sort_unstable();
        let forget = (self.cap / 10).max(1);
        for (_, key) in by_age.into_iter().take(forget) {
            self.entries.remove(&key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn forgets_the_least_recently_used_when_full() {
        let mut keyed = Keyed::with_cap(10);
        for i in 0..10 {
            keyed.insert(i.to_string(), i);
        }
        // Touch 0 so it counts as recent.
        assert_eq!(keyed.get("0"), Some(&0));
        keyed.insert("new".into(), 99);
        assert_eq!(keyed.len(), 10);
        assert!(keyed.get("0").is_some(), "recently used stays");
        assert!(keyed.get("1").is_none(), "the oldest went");
        assert_eq!(keyed.get("new"), Some(&99));
    }

    #[test]
    fn never_grows_past_its_cap() {
        let mut keyed = Keyed::with_cap(100);
        for i in 0..10_000 {
            *keyed.entry(i.to_string(), || 0) += 1;
        }
        assert!(keyed.len() <= 100);
    }
}
