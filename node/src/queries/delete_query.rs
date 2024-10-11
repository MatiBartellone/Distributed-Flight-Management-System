use super::where_logic::where_clause::WhereClause;

#[derive(PartialEq, Debug)]
pub struct DeleteQuery {
    pub table: String,
    pub where_clause: Option<WhereClause>,
}

impl DeleteQuery {
    pub fn new() -> Self {
        Self {
            table: String::new(),
            where_clause: None,
        }
    }
}

impl Default for DeleteQuery {
    fn default() -> Self {
        Self::new()
    }
}
