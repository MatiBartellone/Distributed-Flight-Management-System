use crate::parsers::tokens::token::Token;

pub struct OrderByClause {
    pub column: Token::Identifier,
    pub order: Token::Reserved()
}

impl OrderByClause {
    pub fn new(column: Token::Identifier()) -> Self {
        OrderByClause {
            column,
            order: Token::Reserved(String::from("ASC")),
        }
    }
}