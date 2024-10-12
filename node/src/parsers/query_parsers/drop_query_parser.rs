use crate::{parsers::tokens::token::Token, 
    queries::drop_query::DropQuery, 
    utils::{errors::Errors, token_conversor::get_next_value}};
use crate::utils::constants::*;
use std::{vec::IntoIter, iter::Peekable};


use Token::*;

use super::{drop_keyspace_parser::DropSpaceQueryParser, drop_table_parser::DropTableParser};

pub struct DropQueryParser;

impl DropQueryParser {
    pub fn parse(tokens: Vec<Token>) -> Result<DropQuery, Errors> {
        let mut drop_query = DropQuery::new();
        reserv(&mut tokens.into_iter().peekable(), &mut drop_query)?;
        Ok(drop_query)
    }
}


fn reserv(tokens: &mut Peekable<IntoIter<Token>>, drop_query: &mut DropQuery) -> Result<(), Errors>{
    match get_next_value(tokens)? {
        Reserved(title) if title == KEYSPACE => {
            let parsed_query = DropSpaceQueryParser::parse(tokens)?;
            drop_query.set_keyspace_query(parsed_query);
            Ok(())
        }
        Reserved(title) if title == TABLE => {
            let parsed_query = DropTableParser::parse(tokens)?;
            drop_query.set_table_query(parsed_query);
            Ok(())
        }
        _=> Err(Errors::SyntaxError(
            "Invalid Syntaxis in DROP, missing title".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parsers::tokens::token::Token;

    #[test]
    fn test_parse_drop_keyspace_query() {
        let tokens = vec![
            Token::Reserved("KEYSPACE".to_string()),
            Token::Identifier("my_keyspace".to_string()),
        ];
        let parsed_query = DropQueryParser::parse(tokens)
            .expect("Failed to parse DROP KEYSPACE query");
        let sub_parsed_query = parsed_query.keyspace;
        assert!(sub_parsed_query.is_some()); 
        let keyspace_query = sub_parsed_query.unwrap();
        assert_eq!(keyspace_query.keyspace, "my_keyspace");
        assert_eq!(keyspace_query.if_exist, None); 
    }

    #[test]
    fn test_parse_drop_keyspace_if_exists() {
        let tokens = vec![
            Token::Reserved("KEYSPACE".to_string()),
            Token::Reserved("IF".to_string()),
            Token::Reserved("EXISTS".to_string()),
            Token::Identifier("my_keyspace".to_string()),
        ];
        let parsed_query = DropQueryParser::parse(tokens)
            .expect("Failed to parse DROP KEYSPACE query");

        let sub_parsed_query = parsed_query.keyspace;
        assert!(sub_parsed_query.is_some()); 
        let keyspace_query = sub_parsed_query.unwrap();
        assert_eq!(keyspace_query.keyspace, "my_keyspace");
        assert_eq!(keyspace_query.if_exist, Some(true)); 
    }

    #[test]
    fn test_invalid_token_in_drop_keyspace() {
        let tokens = vec![
            Token::Reserved("KEYSPACE".to_string()),
            Token::Reserved("INVALID".to_string()), // Token inválido
        ];
        let result = DropQueryParser::parse(tokens);
        assert!(result.is_err());
        if let Err(Errors::SyntaxError(err_msg)) = result {
            assert_eq!(err_msg, "Unexpected token in table_name");
        } 
    }

    #[test]
    fn test_drop_keyspace_with_extra_tokens() {
        // Simula una lista de tokens que incluye un token extra no esperado
        let tokens = vec![
            Token::Reserved("KEYSPACE".to_string()),
            Token::Identifier("my_keyspace".to_string()),
            Token::Identifier("extra_token".to_string()), // Token extra
        ];

        let result = DropQueryParser::parse(tokens);
        assert!(result.is_err());
        if let Err(Errors::SyntaxError(err_msg)) = result {
            assert_eq!(err_msg, "DROP with left over paramameters");
        }
    }

}
