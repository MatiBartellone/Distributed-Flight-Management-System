use serde::{Deserialize, Serialize};
use crate::queries::query::Query;
use crate::utils::constants::HINTED_HANDOFF_HOURS;
use crate::utils::errors::Errors;
use crate::utils::timestamp::Timestamp;

#[derive(Serialize, Deserialize)]
pub struct StoredQuery {
    query: dyn Query,
    timestamp: Timestamp,
}

impl StoredQuery {
    pub fn new(query: Box<dyn Query>) -> Result<Self, Errors> {
        Ok(Self { query, timestamp: Timestamp::new() })
    }

    pub fn has_perished(&self) -> bool {
        self.timestamp.has_perished_hours(HINTED_HANDOFF_HOURS)
    }
}