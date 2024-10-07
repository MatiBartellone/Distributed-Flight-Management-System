use std::vec::IntoIter;
use crate::parsers::tokens::token::Token;
use crate::queries::order_by_clause::OrderByClause;
use crate::utils::errors::Errors;

const ASC : &str = "ASC";
const DESC  : &str = "DESC";
pub struct OrderByClauseParser;

impl OrderByClauseParser {
    pub fn parse(tokens: Vec<Token>) -> Result<Vec<OrderByClause>, Errors> {
        let mut order_clauses = Vec::<OrderByClause>::new();
        column(&mut tokens.into_iter(), &mut order_clauses)?;
        Ok(order_clauses)
    }
}

fn column(tokens: &mut IntoIter<Token>, order_clauses: &mut Vec<OrderByClause>) -> Result<(), Errors> {
    let Some(token) = tokens.next() else { return Ok(()) };
    match token {
        Token::Identifier(identifier)  => {
            order_clauses.push(OrderByClause::new(identifier));
            order(tokens, order_clauses)?;
            column(tokens, order_clauses)
        },
        _ => Err(Errors::SyntaxError(String::from("Unexpected token in order by clause"))),
    }
}

fn order(tokens: &mut IntoIter<Token>, order_clauses: &mut Vec<OrderByClause>) -> Result<(), Errors> {
    let Some(token) = tokens.next() else { return Ok(()) };
    match token {
        Token::Reserved(res) if res == *ASC => Ok(()),
        Token::Reserved(res) if res == *DESC => {
            change_last_to_desc(order_clauses);
            Ok(())
        },
        Token::Identifier(identifier)  => {
            order_clauses.push(OrderByClause::new(identifier));
            order(tokens, order_clauses)
        }
        _ => Err(Errors::SyntaxError(String::from("Unexpected token in order by clause"))),
    }
}

fn change_last_to_desc(order_clauses: &mut Vec<OrderByClause>) {
    let Some(order_clause) = order_clauses.pop();
    order_clause.order = DESC.to_string();
    order_clauses.push(order_clause);
}

fn get_next_value(tokens: &mut IntoIter<Token>) -> Result<Token, Errors> {
    tokens.next().ok_or(Errors::SyntaxError(String::from("Query lacks parameters")))
}