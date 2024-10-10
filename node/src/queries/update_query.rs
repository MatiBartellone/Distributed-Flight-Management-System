use std::collections::HashMap;

use crate::parsers::{query_parsers::where_clause_::where_clause::WhereClause, tokens::token::{AritmeticasMath, Literal}};

#[derive(PartialEq, Debug)]
pub struct UpdateQuery {
    pub table: String,
    pub changes: HashMap<String, AssigmentValue>,
    pub where_clause: Option<WhereClause>
}

#[derive(Debug, PartialEq)]
pub enum AssigmentValue {
    Simple(Literal),
    Column(String),
    Arithmetic(String, AritmeticasMath, Literal),
}

impl UpdateQuery {
    pub fn new() -> Self {
        Self {
            table: String::new(),
            changes: HashMap::new(),
            where_clause: None,
        }
    }
}

impl Default for UpdateQuery {
    fn default() -> Self {
        Self::new()
    }
}
