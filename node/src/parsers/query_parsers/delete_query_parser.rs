use std::vec::IntoIter;
use crate::parsers::tokens::token::Token;
use crate::queries::delete_query::DeleteQuery;
use crate::queries::insert_query::InsertQuery;
use crate::utils::errors::Errors;

const  FROM : &str = "FROM";

pub struct DeleteQueryParser;

impl DeleteQueryParser {
    pub fn parse(tokens: Vec<Token>) -> Result<DeleteQuery, Errors> {

    }
}

fn from(tokens: &mut IntoIter<Token>, query: &mut DeleteQuery) -> Result<(), Errors> {
    match get_next_value(tokens)? {
        Token::Reserved(res) if res == *FROM => table(tokens, query),
        _ => Err(Errors::SyntaxError(String::from("DELETE not followed by FROM"))),
    }
}

fn table(tokens: &mut IntoIter<Token>, query: &mut DeleteQuery) -> Result<(), Errors> {
    match get_next_value(tokens)? {
        Token::Identifier(identifier)  => {
            query.table = identifier;
            where_clause(tokens, query)
        },
        _ => Err(Errors::SyntaxError(String::from("Unexpected token in table_name"))),
    }
}

fn where_clause(tokens: &mut IntoIter<Token>, query: &mut DeleteQuery) -> Result<(), Errors> {
    match get_next_value(tokens)? {
        Token::TokensList(list)  => {
            query.where_clause = WhereClauseParser::parse(list)?;
            let None = tokens.next() else { return Err(Errors::SyntaxError(String::from("Nothing should follow a where-clause")) ) };
            Ok(())
        },
        _ => Err(Errors::SyntaxError(String::from("Unexpected token in table_name"))),
    }
}



fn get_next_value(tokens: &mut IntoIter<Token>) -> Result<Token, Errors> {
    tokens.next().ok_or(Errors::SyntaxError(String::from("Query lacks parameters")))
}