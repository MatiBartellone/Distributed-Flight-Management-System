use crate::queries::query::{Query, QueryEnum};
use crate::utils::constants::HINTED_HANDOFF_HOURS;
use crate::utils::errors::Errors;
use crate::utils::timestamp::Timestamp;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct StoredQuery {
    pub query: QueryEnum,
    pub timestamp: Timestamp,
}

impl StoredQuery {
    #[allow(clippy::borrowed_box)]
    pub fn new(query: &Box<dyn Query>) -> Result<Self, Errors> {
        let Some(query_enum) = QueryEnum::from_query(query) else {
            return Err(Errors::ServerError(String::from("")));
        };
        Ok(Self {
            query: query_enum,
            timestamp: Timestamp::new(),
        })
    }

    pub fn get_query(&self) -> Box<dyn Query> {
        QueryEnum::into_query(QueryEnum::new_from(&self.query))
    }

    pub fn has_perished(&self) -> bool {
        self.timestamp.has_perished_hours(HINTED_HANDOFF_HOURS)
    }
}
