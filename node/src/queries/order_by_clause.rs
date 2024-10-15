use serde::{Deserialize, Serialize};

#[derive(PartialEq, Debug, Serialize, Deserialize, Clone)]
pub struct OrderByClause {
    pub column: String,
    pub order: String,
}

impl OrderByClause {
    pub fn new(column: String) -> Self {
        OrderByClause {
            column,
            order: String::from("ASC"),
        }
    }

    pub fn new_with_order(column: String, order: String) -> Self {
        OrderByClause { column, order }
    }
}
