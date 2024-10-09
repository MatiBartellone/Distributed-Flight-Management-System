use std::vec::IntoIter;
use crate::parsers::tokens::token::Token;
use crate::queries::create_table_query::CreateTableQuery;
use crate::queries::delete_query::DeleteQuery;
use crate::utils::errors::Errors;

const TABLE: &str = "TABLE";

pub struct CreateTableQueryParser;

impl CreateTableQueryParser {
    pub fn parse(tokens: Vec<Token>) -> Result<CreateTableQuery, Errors> {
        let mut create_table_query = CreateTableQuery::new();
        table(&mut tokens.into_iter(), &mut create_table_query)?;
        Ok(create_table_query)
    }
}

fn table(tokens: &mut IntoIter<Token>, query: &mut CreateTableQuery) -> Result<(), Errors> {
    match get_next_value(tokens)? {
        Token::Reserved(res) if res == *TABLE => table_name(tokens, query),
        _ => Err(Errors::SyntaxError(String::from(
            "CREATE not followed by TABLE",
        ))),
    }
}
fn table_name(tokens: &mut IntoIter<Token>, query: &mut CreateTableQuery) -> Result<(), Errors> {
    match get_next_value(tokens)? {
        Token::Identifier(identifier) => {
            query.table_name = identifier;
            columns(tokens, query)
        },
        _ => Err(Errors::SyntaxError(String::from(
            "Unexpected token in table name",
        ))),
    }
}

fn columns(tokens: &mut IntoIter<Token>, query: &mut CreateTableQuery) -> Result<(), Errors> {
    match get_next_value(tokens)? {
        Token::TokensList(list) => {
            query.table_name = identifier;
        },
        _ => Err(Errors::SyntaxError(String::from(
            "Unexpected token in table name",
        ))),
    }
}

fn get_next_value(tokens: &mut IntoIter<Token>) -> Result<Token, Errors> {
    tokens
        .next()
        .ok_or(Errors::SyntaxError(String::from("Query lacks parameters")))
}