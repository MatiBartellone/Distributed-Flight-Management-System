use crate::{parsers::tokens::token::Token, 
    queries::drop_table_query::DropTableQuery, 
    utils::{errors::Errors, token_conversor::get_next_value}};
use crate::utils::constants::*;
use std::{vec::IntoIter, iter::Peekable};

pub struct DropTableParser;

impl DropTableParser {
    pub fn parse(tokens: &mut Peekable<IntoIter<Token>>) -> Result<DropTableQuery, Errors> {
        let mut drop_query = DropTableQuery::new();
        identifier(tokens, &mut drop_query, false)?;
        Ok(drop_query)
    }

}

fn identifier(tokens: &mut Peekable<IntoIter<Token>>, query: &mut DropTableQuery, is_final: bool) -> Result<(), Errors> {
    match get_next_value(tokens)? {
        Token::Identifier(title) => {
            query.table = title;
            finish(tokens)
        }
        Token::Reserved(res) if res == *IF && !is_final => exists(tokens, query) ,
        _ => Err(Errors::SyntaxError(String::from(
            "Unexpected token in table_name",
        ))),
    }
}

fn finish(tokens: &mut Peekable<IntoIter<Token>>) -> Result<(), Errors> {
    if tokens.next().is_none(){
        return Ok(())
    }
    Err(Errors::SyntaxError(String::from(
        "DROP with left over paramameters",
    )))
}

fn exists(tokens: &mut Peekable<IntoIter<Token>>, query: &mut DropTableQuery) -> Result<(), Errors> {
    match get_next_value(tokens)? {
        Token::Reserved(res) if res == *EXISTS => {
            query.if_exist = Some(true);
            identifier(tokens, query, true)
        }
        _ => Err(Errors::SyntaxError(String::from(
            "Unexpected token in table_name",
        ))),
    }
}