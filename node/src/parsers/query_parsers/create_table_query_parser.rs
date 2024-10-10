use crate::parsers::tokens::token::{DataType, Token};
use crate::queries::create_table_query::CreateTableQuery;
use crate::utils::errors::Errors;
use std::vec::IntoIter;

const TABLE: &str = "TABLE";
const PRIMARY: &str = "PRIMARY";
const KEY: &str = "KEY";

pub struct CreateTableQueryParser;

impl CreateTableQueryParser {
    pub fn parse(tokens: Vec<Token>) -> Result<CreateTableQuery, Errors> {
        let mut create_table_query = CreateTableQuery::new();
        table(&mut tokens.into_iter(), &mut create_table_query)?;
        check_primary_key(&mut create_table_query)?;
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
            column_list(tokens, query)
        }
        _ => Err(Errors::SyntaxError(String::from(
            "Unexpected token in table name",
        ))),
    }
}

fn column_list(tokens: &mut IntoIter<Token>, query: &mut CreateTableQuery) -> Result<(), Errors> {
    match get_next_value(tokens)? {
        Token::TokensList(list) => {
            column(&mut list.into_iter(), query)?;
            let None = tokens.next() else {
                return Err(Errors::SyntaxError(String::from(
                    "Nothing should follow the column list",
                )));
            };
            Ok(())
        },
        _ => Err(Errors::SyntaxError(String::from(
            "Unexpected token in column definition",
        ))),
    }
}

fn column(tokens: &mut IntoIter<Token>, query: &mut CreateTableQuery) -> Result<(), Errors> {
    let Some(token) = tokens.next() else {
        return Ok(());
    };
    match token {
        Token::Identifier(identifier) => {
            query.columns.insert(identifier.clone(), get_data_type(tokens)?);
            try_primary_key(tokens, query, identifier)
        }
        Token::Reserved(res) if res == *PRIMARY => key(tokens, query),
        _ => Err(Errors::SyntaxError(String::from(
            "Unexpected token in column definition",
        ))),
    }
}

fn try_primary_key(tokens: &mut IntoIter<Token>, query: &mut CreateTableQuery, primary_key: String) -> Result<(), Errors> {
    match get_next_value(tokens)? {
        Token::Identifier(i) if i == *"," => column(tokens, query),
        Token::Reserved(res) if res == *PRIMARY => {
            primary_key_def(tokens, query, primary_key)
        }
        _ => Err(Errors::SyntaxError(String::from(
            "Unexpected token in column definition",
        ))),
    }
}

fn primary_key_def(
    tokens: &mut IntoIter<Token>,
    query: &mut CreateTableQuery,
    primary_key: String,
) -> Result<(), Errors> {
    match get_next_value(tokens)? {
        Token::Reserved(res) if res == *KEY => {
            set_primary_key(query, primary_key)?;
            match get_next_value(tokens)? {
                Token::Identifier(i) if i == *"," => Ok(()),
                _ => Err(Errors::SyntaxError(String::from(
                    "Comma missing after PRIMARY KEY",
                ))),
            }
        }
        _ => Err(Errors::SyntaxError(String::from(
            "PRIMARY not followed by KEY",
        ))),
    }
}
fn key(tokens: &mut IntoIter<Token>, query: &mut CreateTableQuery) -> Result<(), Errors> {
    match get_next_value(tokens)? {
        Token::Reserved(res) if res == *KEY => primary_key_list(tokens, query),
        _ => Err(Errors::SyntaxError(String::from(
            "PRIMARY not followed by KEY",
        ))),
    }
}

fn primary_key_list(
    tokens: &mut IntoIter<Token>,
    query: &mut CreateTableQuery,
) -> Result<(), Errors> {
    match get_next_value(tokens)? {
        Token::TokensList(list) => {
            if list.len() != 1 {
                return Err(Errors::SyntaxError(String::from(
                    "Primary key between parenthesis must be 1",
                )));
            };
            match list.first() {
                Some(Token::Identifier(identifier)) => {
                    set_primary_key(query, identifier.to_string())?;
                    Ok(())
                }
                _ => Err(Errors::SyntaxError(String::from(
                    "Unexpected token in primary key list",
                ))),
            }
        }
        _ => Err(Errors::SyntaxError(String::from(
            "Unexpected token in primary key list",
        ))),
    }
}

fn get_data_type(tokens: &mut IntoIter<Token>) -> Result<DataType, Errors> {
    match tokens.next() {
        Ok(token) => match token {
            Token::DataType(_) => Ok(token),
        },
        _ => Err(Errors::SyntaxError(String::from("Missing data type"))),
    }
}

fn get_next_value(tokens: &mut IntoIter<Token>) -> Result<Token, Errors> {
    tokens
        .next()
        .ok_or(Errors::SyntaxError(String::from("Query lacks parameters")))
}

fn set_primary_key(query: &mut CreateTableQuery, primary_key: String) -> Result<(), Errors> {
    if query.primary_key.is_empty() {
        query.primary_key = primary_key;
        return Ok(())
    }
    Err(Errors::SyntaxError(String::from("Primary key must be defined once")))
}

fn check_primary_key(query: &mut CreateTableQuery) -> Result<(), Errors> {
    if query.columns.get(query.primary_key.as_str()).is_none() {
        return Err(Errors::SyntaxError(String::from("Primary key column not defined")))
    }
    if query.primary_key.is_empty() {
        return Err(Errors::SyntaxError(String::from("Primary not defined")))
    }
    Ok(())
}