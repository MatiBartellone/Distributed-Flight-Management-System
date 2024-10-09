use crate::queries::order_by_clause::OrderByClause;

pub struct SelectQuery {
    pub table: String,
    pub columns: Vec<String>,
    pub where_clause: Vec<String>,
    pub order_clauses: Vec<OrderByClause>,
}

impl SelectQuery {
    pub fn new() -> Self {
        Self{
            table: String::new(),
            columns: Vec::new(),
            where_clause: Vec::new(),
            order_clauses: Vec::new(),
        }
    }
}