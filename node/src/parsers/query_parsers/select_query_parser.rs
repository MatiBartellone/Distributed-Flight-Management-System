use std::vec::IntoIter;
use crate::parsers::query_parsers::order_by_clause_parser::OrderByClauseParser;
use crate::parsers::tokens::token::Token;
use crate::queries::insert_query::InsertQuery;
use crate::queries::select_query::SelectQuery;
use crate::utils::errors::Errors;

const FROM: &str = "FROM";
const WHERE: &str = "WHERE";
const ORDER: &str = "ORDER";
const BY: &str = "BY";

pub struct SelectQueryParser;

impl SelectQueryParser {
    pub fn parse(tokens: Vec<Token>) -> Result<SelectQuery, Errors> {
        let mut select_query = SelectQuery::new();
        columns(&mut tokens.into_iter(), &mut select_query)?;
        Ok(select_query)
    }
}

fn columns(tokens: &mut IntoIter<Token>, query: &mut SelectQuery) -> Result<(), Errors> {
    match get_next_value(tokens)? {
        Token::TokensList(list) => {
            query.columns = get_columns(list)?;
            from(tokens, query)
        },
        _ => Err(Errors::SyntaxError(String::from(
            "INSERT not followed by INTO",
        ))),
    }
}

fn from(tokens: &mut IntoIter<Token>, query: &mut SelectQuery) -> Result<(), Errors> {
    match get_next_value(tokens)? {
        Token::Reserved(res) if res == *FROM => table(tokens, query),
        _ => Err(Errors::SyntaxError(String::from(
            "Columns not followed by FROM",
        ))),
    }
}

fn table(tokens: &mut IntoIter<Token>, query: &mut SelectQuery) -> Result<(), Errors> {
    match get_next_value(tokens)? {
        Token::Identifier(identifier) => {
            query.table = identifier;
            modifiers(tokens, query)
        }
        _ => Err(Errors::SyntaxError(String::from(
            "Unexpected token in table_name",
        ))),
    }
}
fn modifiers(tokens: &mut IntoIter<Token>, query: &mut SelectQuery) -> Result<(), Errors> {
    let Some(token) = tokens.next() else {
        return Ok(());
    };
    match token {
        Token::Reserved(res) if res == *WHERE => where_clause(tokens, query),
        Token::Reserved(res) if res == *ORDER => by(tokens, query),
        _ => Err(Errors::SyntaxError(String::from(
            "Unexpected token in query",
        ))),
    }
}

fn where_clause(tokens: &mut IntoIter<Token>, query: &mut SelectQuery) -> Result<(), Errors> {
    match get_next_value(tokens)? {
        Token::TokensList(list) => {
            //query.where_clause = WhereClauseParser::parse(list)?;
            order(tokens, query)
        }
        _ => Err(Errors::SyntaxError(String::from(
            "Unexpected token in where_clause",
        ))),
    }
}

fn order(tokens: &mut IntoIter<Token>, query: &mut SelectQuery) -> Result<(), Errors> {
    let Some(token) = tokens.next() else {
        return Ok(());
    };
    match token {
        Token::Reserved(res) if res == *ORDER => by(tokens, query),
        _ => Err(Errors::SyntaxError(String::from(
            "Unexpected token in query",
        ))),
    }
}
fn by(tokens: &mut IntoIter<Token>, query: &mut SelectQuery) -> Result<(), Errors> {
    match get_next_value(tokens)? {
        Token::Reserved(res) if res == *BY => order_clause(tokens, query),
        _ => Err(Errors::SyntaxError(String::from(
            "Order not followed by BY",
        ))),
    }
}

fn order_clause(tokens: &mut IntoIter<Token>, query: &mut SelectQuery) -> Result<(), Errors> {
    match get_next_value(tokens)? {
        Token::TokensList(list) => {
            query.order_clauses = OrderByClauseParser::parse(list)?;
            let None = tokens.next() else {
                return Err(Errors::SyntaxError(String::from(
                    "Nothing should follow a order_clause",
                )));
            };
            Ok(())
        }
        _ => Err(Errors::SyntaxError(String::from(
            "Unexpected token in order_clause",
        ))),
    }
}

fn get_columns(list: Vec<Token>) -> Result<Vec<String>, Errors> {
    let mut columns = Vec::new();
    for elem in list {
        match elem {
            Token::Identifier(column) => columns.push(column),
            _ => {
                return Err(Errors::SyntaxError(String::from(
                    "Unexpected token in columns",
                )))
            }
        }
    }
    Ok(columns)
}

fn get_next_value(tokens: &mut IntoIter<Token>) -> Result<Token, Errors> {
    tokens
        .next()
        .ok_or(Errors::SyntaxError(String::from("Query lacks parameters")))
}