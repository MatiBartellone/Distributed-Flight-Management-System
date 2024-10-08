use crate::parsers::query_parsers::where_clause::where_clause_parser::WhereClauseParser;
use crate::parsers::tokens::token::Token;
use crate::queries::delete_query::DeleteQuery;
use crate::queries::insert_query::InsertQuery;
use crate::utils::errors::Errors;
use std::vec::IntoIter;

const FROM: &str = "FROM";

pub struct DeleteQueryParser;

impl DeleteQueryParser {
    pub fn parse(tokens: Vec<Token>) -> Result<DeleteQuery, Errors> {
        let mut delete_query = DeleteQuery::new();
        from(&mut tokens.into_iter(), &mut delete_query)?;
        Ok(delete_query)
    }
}

fn from(tokens: &mut IntoIter<Token>, query: &mut DeleteQuery) -> Result<(), Errors> {
    match get_next_value(tokens)? {
        Token::Reserved(res) if res == *FROM => table(tokens, query),
        _ => Err(Errors::SyntaxError(String::from(
            "DELETE not followed by FROM",
        ))),
    }
}

fn table(tokens: &mut IntoIter<Token>, query: &mut DeleteQuery) -> Result<(), Errors> {
    match get_next_value(tokens)? {
        Token::Identifier(identifier) => {
            query.table = identifier;
            where_clause(tokens, query)
        }
        _ => Err(Errors::SyntaxError(String::from(
            "Unexpected token in table_name",
        ))),
    }
}

fn where_clause(tokens: &mut IntoIter<Token>, query: &mut DeleteQuery) -> Result<(), Errors> {
    let Some(token) = tokens.next() else {
        return Ok(());
    };
    match token {
        Token::TokensList(list) => {
            query.where_clause = WhereClauseParser::parse(list)?;
            let None = tokens.next() else {
                return Err(Errors::SyntaxError(String::from(
                    "Nothing should follow a where-clause",
                )));
            };
            Ok(())
        }
        _ => Err(Errors::SyntaxError(String::from(
            "Unexpected token in table_name",
        ))),
    }
}

fn get_next_value(tokens: &mut IntoIter<Token>) -> Result<Token, Errors> {
    tokens
        .next()
        .ok_or(Errors::SyntaxError(String::from("Query lacks parameters")))
}
