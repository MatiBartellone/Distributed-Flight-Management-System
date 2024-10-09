use crate::parsers::query_parsers::where_clause_::where_clause::WhereClause;

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
