use chrono::Utc;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct Timestamp {
    pub timestamp: i64,
}

impl Timestamp {
    pub fn new() -> Self {
        Self {
            timestamp: Utc::now().timestamp_millis(),
        }
    }

    pub fn new_from_timestamp(timestamp: &Self) -> Self {
        Self {
            timestamp: timestamp.timestamp,
        }
    }

    pub fn new_from_i64(timestamp: i64) -> Self {
        Self { timestamp }
    }

    pub fn is_newer_than(&self, timestamp: Timestamp) -> bool {
        self.timestamp > timestamp.timestamp
    }

    pub fn is_older_than(&self, timestamp: Timestamp) -> bool {
        self.timestamp < timestamp.timestamp
    }

    pub fn has_perished_hours(&self, hours: i64) -> bool {
        let milliseconds = hours * 1000 * 60 * 60;
        Utc::now().timestamp_millis() > self.timestamp + milliseconds
    }
}

impl Default for Timestamp {
    fn default() -> Self {
        Self::new()
    }
}
