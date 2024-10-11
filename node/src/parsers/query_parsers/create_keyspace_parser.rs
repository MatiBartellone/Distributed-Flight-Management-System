use crate::parsers::tokens::terms::Term;
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
const MISSING_COLON: &str = "Missing colon for separating parameters in replication";
const MISSING_KEY: &str = "Missing key for replication";
const MISSING_VALUE: &str = "Missing value for replication";
const COMMA: &str = ",";
const COLON: &str = ":";

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
        let token = self.get_next_value(tokens)?;
        match token {
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
            Token::BraceList(list) => {
                let mut token_list = list.into_iter().peekable();
                let replication_map = HashMap::<String, String>::new();
                query.replication = replication_map;
                self.build_replication_map(&mut token_list, &mut query.replication)?;
                Ok(())
            }
            _ => Err(Errors::SyntaxError(String::from(INVALID_PARAMETERS))),
        }
    }

    fn build_replication_map(
        &self,
        tokens_list: &mut Peekable<IntoIter<Token>>,
        replication: &mut HashMap<String, String>,
    ) -> Result<(), Errors> {
        self.check_key_literal(tokens_list, replication)
    }

    fn check_key_literal(
        &self,
        tokens_list: &mut Peekable<IntoIter<Token>>,
        replication: &mut HashMap<String, String>,
    ) -> Result<(), Errors> {
        match self.get_next_value(tokens_list)? {
            Token::Reserved(key) => self.check_colon(tokens_list, key, replication),
            _ => Err(Errors::SyntaxError(String::from(MISSING_KEY))),
        }
    }

    fn check_colon(
        &self,
        tokens_list: &mut Peekable<IntoIter<Token>>,
        key: String,
        replication: &mut HashMap<String, String>,
    ) -> Result<(), Errors> {
        match self.get_next_value(tokens_list)? {
            Token::Symbol(s) if s == COLON => {
                self.check_value_literal(tokens_list, key, replication)
            }
            _ => Err(Errors::SyntaxError(String::from(MISSING_COLON))),
        }
    }

    fn check_value_literal(
        &self,
        tokens: &mut Peekable<IntoIter<Token>>,
        key: String,
        replication: &mut HashMap<String, String>,
    ) -> Result<(), Errors> {
        match self.get_next_value(tokens)? {
            Token::Term(Term::Literal(value)) => {
                replication.insert(key, value.value);

                if tokens.peek().is_some() {
                    self.check_comma(tokens, replication)
                } else {
                    Ok(())
                }
            }
            _ => Err(Errors::SyntaxError(String::from(MISSING_VALUE))),
        }
    }

    fn check_comma(
        &self,
        tokens: &mut Peekable<IntoIter<Token>>,
        replication: &mut HashMap<String, String>,
    ) -> Result<(), Errors> {
        match self.get_next_value(tokens)? {
            Token::Symbol(s) if s == *COMMA && tokens.peek().is_some() => {
                self.check_key_literal(tokens, replication)
            }
            _ => Ok(()),
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
    use crate::parsers::tokens::{data_type::DataType, literal::Literal};

    use super::*;

    #[test]
    fn test_01_create_keyspace_is_valid() {
        let vec = vec![
            Token::Reserved("KEYSPACE".to_string()),
            Token::Identifier("keyspace_name".to_string()),
            Token::Reserved("WITH".to_string()),
            Token::BraceList(vec![
                Token::Reserved("class".to_string()),
                Token::Symbol(":".to_string()),
                Token::Term(Term::Literal(Literal::new(
                    "SimpleStrategy".to_string(),
                    DataType::Text,
                ))),
                Token::Symbol(",".to_string()),
                Token::Reserved("replication_factor".to_string()),
                Token::Symbol(":".to_string()),
                Token::Term(Term::Literal(Literal::new("1".to_string(), DataType::Int))),
            ]),
        ];
        let tokens = vec;

        let parser = CreateKeyspaceParser;
        let result = parser.parse(tokens);

        let query = result.unwrap();
        //        assert!(result.is_ok());

        assert_eq!(query.keyspace, "keyspace_name".to_string());
        assert_eq!(query.replication.get("class").unwrap(), "SimpleStrategy");
        assert_eq!(query.replication.get("replication_factor").unwrap(), "1");
    }
}
