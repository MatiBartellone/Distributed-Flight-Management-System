use crate::parsers::tokens::token::Term;
use crate::{
    parsers::tokens::token::Token, queries::create_keyspace_query::CreateKeyspaceQuery,
    utils::errors::Errors,
};
use std::iter::Peekable;
use std::{collections::HashMap, vec::IntoIter};

const INVALID_PARAMETERS: &str = "Query lacks parameters";
const KEYSPACE: &str = "KEYSPACE";
const WITH: &str = "WITH";
const INVALID_CREATE: &str = "CREATE not followed by KEYSPACE";
const UNEXPECTED_TOKEN: &str = "Unexpected token in table_name";
const COMMA: &str = ",";

pub struct CreateKeyspaceParser;

impl CreateKeyspaceParser {
    pub fn parse(&self, tokens: Vec<Token>) -> Result<CreateKeyspaceQuery, Errors> {
        let mut create_keyspace_query = CreateKeyspaceQuery::new();
        self.keyspace_keyword(
            &mut tokens.into_iter().peekable(),
            &mut create_keyspace_query,
        )?;
        Ok(create_keyspace_query)
    }

    fn keyspace_keyword(
        &self,
        tokens: &mut Peekable<IntoIter<Token>>,
        query: &mut CreateKeyspaceQuery,
    ) -> Result<(), Errors> {
        match self.get_next_value(tokens)? {
            Token::Reserved(keyspace) if keyspace == *KEYSPACE => self.keyspace_name(tokens, query),
            _ => Err(Errors::SyntaxError(String::from(INVALID_CREATE))),
        }
    }

    fn keyspace_name(
        &self,
        tokens: &mut Peekable<IntoIter<Token>>,
        query: &mut CreateKeyspaceQuery,
    ) -> Result<(), Errors> {
        match self.get_next_value(tokens)? {
            Token::Identifier(identifier) => {
                query.keyspace = identifier;
                self.with(tokens, query)
            }
            _ => Err(Errors::SyntaxError(String::from(UNEXPECTED_TOKEN))),
        }
    }

    fn with(
        &self,
        tokens: &mut Peekable<IntoIter<Token>>,
        query: &mut CreateKeyspaceQuery,
    ) -> Result<(), Errors> {
        match self.get_next_value(tokens)? {
            Token::Reserved(with) if with == *WITH => self.replication(tokens, query),
            _ => Err(Errors::SyntaxError(String::from(INVALID_PARAMETERS))),
        }
    }

    fn replication(
        &self,
        tokens: &mut Peekable<IntoIter<Token>>,
        query: &mut CreateKeyspaceQuery,
    ) -> Result<(), Errors> {
        match self.get_next_value(tokens)? {
            Token::TokensList(list) => {
                let mut token_list = list.into_iter().peekable();
                query.replication = self.build_replication_map(&mut token_list)?;
                Ok(())
            }
            _ => Err(Errors::SyntaxError(String::from(INVALID_PARAMETERS))),
        }
    }

    fn build_replication_map(
        &self,
        tokens_list: &mut Peekable<IntoIter<Token>>,
    ) -> Result<HashMap<String, String>, Errors> {
        let mut replication = HashMap::<String, String>::new();
        match self.get_next_value(tokens_list)? {
            Token::Term(Term::Literal(literal)) => self.check_comma(tokens_list, &mut replication),
            _ => Err(Errors::SyntaxError(String::from(INVALID_PARAMETERS))),
        };

        Ok(replication)
    }

    fn check_comma(
        &self,
        tokens: &mut Peekable<IntoIter<Token>>,
        replication: &mut HashMap<String, String>,
    ) -> Result<HashMap<String, String>, Errors> {
        match self.get_next_value(tokens)? {
            Ok(Symbol(s)) if s == *COMMA && tokens.peek().is_some() => {
                self.build_replication_map(tokens)
            }
            _ => Ok(replication),
        }
    }

    fn get_next_value(&self, tokens: &mut Peekable<IntoIter<Token>>) -> Result<Token, Errors> {
        tokens
            .next()
            .ok_or(Errors::SyntaxError(String::from(INVALID_PARAMETERS)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parsers::tokens::token::{create_literal, DataType, Token};

    #[test]
    fn test_01_create_keyspace_is_valid() {
        // TODO: Refactor into smaller funcs while adding more tests
        let simple_strategy = create_literal("SimpleStrategy", DataType::Text);
        let one = create_literal("1", DataType::Integer);
        let tokens = vec![
            Token::Reserved(String::from(KEYSPACE)),
            Token::Identifier(String::from("keyspace_name")),
            Token::Reserved(String::from(WITH)),
            Token::TokensList(vec![
                Token::Reserved(String::from("class")),
                Token::Term(Term::Literal(simple_strategy)),
                Token::Reserved(String::from("replication_factor")),
                Token::Term(Term::Literal(one)),
            ]),
        ];
        let parser = CreateKeyspaceParser;
        let result = parser.parse(tokens);
        assert!(result.is_ok());
        let query = result.unwrap();
        assert_eq!(query.keyspace, "keyspace_name".to_string());
        assert_eq!(query.replication.get("class").unwrap(), "SimpleStrategy");
        assert_eq!(query.replication.get("replication_factor").unwrap(), "1");
    }
}
