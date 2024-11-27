use crate::{
    parsers::tokens::token::Token,
    queries::query::Query,
    utils::{errors::Errors, types::token_conversor::get_next_value},
};
use std::{iter::Peekable, vec::IntoIter};

use Token::*;
use crate::utils::parser_constants::{KEYSPACE, TABLE};
use super::{
    drop_keyspace_parser::DropKeySpaceQueryParser, drop_table_parser::DropTableQueryParser,
};

pub struct DropQueryParser;

impl DropQueryParser {
    pub fn parse(tokens: Vec<Token>) -> Result<Box<dyn Query>, Errors> {
        let drop_query = reserv(&mut tokens.into_iter().peekable())?;
        Ok(drop_query)
    }
}

fn reserv(tokens: &mut Peekable<IntoIter<Token>>) -> Result<Box<dyn Query>, Errors> {
    match get_next_value(tokens)? {
        Reserved(title) if title == KEYSPACE => {
            let query = DropKeySpaceQueryParser::parse(tokens)?;
            Ok(Box::new(query))
        }
        Reserved(title) if title == TABLE => {
            let query = DropTableQueryParser::parse(tokens)?;
            Ok(Box::new(query))
        }
        _ => Err(Errors::SyntaxError(
            "Invalid Syntaxis in DROP, missing title".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use crate::parsers::{query_parsers::drop_query_parser::DropQueryParser, tokens::token::Token};

    #[test]
    fn test_parse_drop_keyspace_query() {
        let tokens = vec![
            Token::Reserved("KEYSPACE".to_string()),
            Token::Identifier("my_keyspace".to_string()),
        ];
        assert!(DropQueryParser::parse(tokens).is_ok());
    }

    #[test]
    fn test_parse_drop_keyspace_if_exists() {
        let tokens = vec![
            Token::Reserved("KEYSPACE".to_string()),
            Token::Reserved("IF".to_string()),
            Token::Reserved("EXISTS".to_string()),
            Token::Identifier("my_keyspace".to_string()),
        ];
        assert!(DropQueryParser::parse(tokens).is_ok());
    }

    #[test]
    fn test_invalid_token_in_drop_keyspace() {
        let tokens = vec![
            Token::Reserved("KEYSPACE".to_string()),
            Token::Reserved("INVALID".to_string()), // Token inválido
        ];
        assert!(DropQueryParser::parse(tokens).is_err());
    }

    #[test]
    fn test_drop_keyspace_with_extra_tokens() {
        let tokens = vec![
            Token::Reserved("KEYSPACE".to_string()),
            Token::Identifier("my_keyspace".to_string()),
            Token::Identifier("extra_token".to_string()), // Token extra
        ];

        assert!(DropQueryParser::parse(tokens).is_err());
    }
}
