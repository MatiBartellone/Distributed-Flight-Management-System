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
        column(&mut tokens.into_iter(), &mut order_clauses, false)?;
        Ok(order_clauses)
    }
}

fn column(tokens: &mut IntoIter<Token>, order_clauses: &mut Vec<OrderByClause>, modified: bool) -> Result<(), Errors> {
    let Some(token) = tokens.next() else { return Ok(()) };
    match token {
        Token::Identifier(identifier)  => {
            order_clauses.push(OrderByClause::new(identifier));
            column(tokens, order_clauses, false)
        },
        Token::Reserved(res) if res == *DESC || res == *ASC => {
            if modified { return Err(Errors::SyntaxError(String::from("")))}
            change_last_to_desc(order_clauses)?;
            column(tokens, order_clauses, true)
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

fn change_last_to_desc(order_clauses: &mut Vec<OrderByClause>, order: String) -> Result<(), Errors> {
    if order_clauses.len() == 0 { return Err(Errors::SyntaxError(format!("No colum provided for {} modifier", order))) ; }
    let Some(order_clause) = order_clauses.pop();
    order_clause.order = order;
    order_clauses.push(order_clause);
    Ok(())
}

fn get_next_value(tokens: &mut IntoIter<Token>) -> Result<Token, Errors> {
    tokens.next().ok_or(Errors::SyntaxError(String::from("Query lacks parameters")))
}