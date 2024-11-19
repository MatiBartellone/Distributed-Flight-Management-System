use chrono::Utc;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct Timestamp {
    timestamp: i64,
}

impl Timestamp {
    pub fn new() -> Self {
        Self {
            timestamp: Utc::now().timestamp_millis(),
        }
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
