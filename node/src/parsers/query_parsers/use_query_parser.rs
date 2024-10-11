use std::{iter::Peekable, vec::IntoIter};

use crate::{parsers::tokens::token::Token, queries::use_query::UseQuery, utils::errors::Errors};

const MISSING_KEYSPACE: &str = "Missing keyspace name";
const UNEXPECTED_TOKEN: &str = "Unexpected token in keyspace_name";

pub struct UseQueryParser;

impl UseQueryParser {
    pub fn parse(&self, tokens: Vec<Token>) -> Result<UseQuery, Errors> {
        let mut use_query = UseQuery::new();
        self.keyspace_name(&mut tokens.into_iter().peekable(), &mut use_query)?;
        Ok(use_query)
    }

    fn keyspace_name(
        &self,
        tokens: &mut Peekable<IntoIter<Token>>,
        query: &mut UseQuery,
    ) -> Result<(), Errors> {
        match self.get_next_token(tokens)? {
            Token::Identifier(identifier) => {
                query.keyspace_name = identifier;
                Ok(())
            }
            _ => Err(Errors::SyntaxError(String::from(UNEXPECTED_TOKEN))),
        }
    }

    fn get_next_token(&self, tokens: &mut Peekable<IntoIter<Token>>) -> Result<Token, Errors> {
        tokens
            .next()
            .ok_or(Errors::SyntaxError(String::from(MISSING_KEYSPACE)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parsers::tokens::token::Token;

    const KEYSPACE_NAME: &str = "keyspace_name";
    const UNEXPECTED: &str = "UNEXPECTED";

    #[test]
    fn test_01_use_keyspace_is_valid() {
        let tokens = vec![Token::Identifier(String::from(KEYSPACE_NAME))];
        let use_query_parser = UseQueryParser;
        let use_query = use_query_parser.parse(tokens).unwrap();
        assert_eq!(use_query.keyspace_name, KEYSPACE_NAME);
    }
    #[test]
    fn test_02_use_keyspace_with_missing_keyspace_should_error() {
        let tokens = vec![];
        let use_query_parser = UseQueryParser;
        let result = use_query_parser.parse(tokens);

        assert!(result.is_err());

        if let Err(Errors::SyntaxError(msg)) = result {
            assert_eq!(msg, MISSING_KEYSPACE);
        } else {
            panic!("El error no es del tipo Errors::SyntaxError");
        }
    }
    #[test]
    fn test_03_use_keyspace_with_any_other_token_should_error() {
        let tokens = vec![Token::Reserved(String::from(UNEXPECTED))];
        let use_query_parser = UseQueryParser;
        let result = use_query_parser.parse(tokens);

        assert!(result.is_err());

        if let Err(Errors::SyntaxError(msg)) = result {
            assert_eq!(msg, UNEXPECTED_TOKEN);
        } else {
            panic!("El error no es del tipo Errors::SyntaxError");
        }
    }
}
