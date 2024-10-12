use crate::{parsers::tokens::token::Token, 
    queries::drop_keyspace_query::DropKeySpaceQuery, 
    utils::{errors::Errors, token_conversor::get_next_value}};
use crate::utils::constants::*;
use std::{vec::IntoIter, iter::Peekable};


pub struct DropSpaceQueryParser;

impl DropSpaceQueryParser {
    pub fn parse(tokens: &mut Peekable<IntoIter<Token>>) -> Result<DropKeySpaceQuery, Errors> {
        let mut drop_query = DropKeySpaceQuery::new();
        identifier(tokens, &mut drop_query, false)?;
        Ok(drop_query)
    }
}

fn identifier(tokens: &mut Peekable<IntoIter<Token>>, query: &mut DropKeySpaceQuery, is_final: bool) -> Result<(), Errors> {
    let token = get_next_value(tokens)?;
    println!("{:?}", token);
    match token {
        Token::Identifier(title) => {
            query.keyspace = title;
            finish(tokens)
        }
        Token::Reserved(res) if res == *IF && !is_final => exists(tokens, query) ,
        _ => {
            println!("aa");
            Err(Errors::SyntaxError(String::from(
            "Unexpected token in table_name",
        )))},
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

fn exists(tokens: &mut Peekable<IntoIter<Token>>, query: &mut DropKeySpaceQuery) -> Result<(), Errors> {
    match get_next_value(tokens)? {
        Token::Reserved(res) if res == *EXISTS => {
            query.if_exist = Some(true);
            identifier(tokens, query, true)
        }
        _ => Err(Errors::SyntaxError(String::from(
            "Unexpected token after IF",
        ))),
    }
}
 

