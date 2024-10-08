use crate::parsers::query_parsers::where_clause::boolean_expression::BooleanExpression;

#[derive(PartialEq, Debug)]
pub struct DeleteQuery {
    pub table: String,
    pub where_clause: Option<BooleanExpression>,
}

impl DeleteQuery {
    pub fn new() -> Self {
        Self {
            table: String::new(),
            where_clause: None,
        }
    }
}
