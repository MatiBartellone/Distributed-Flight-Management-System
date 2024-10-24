use crate::parsers::tokens::terms::Term;
use crate::utils::constants::{REPLICATION, STRATEGY};
use crate::{
    parsers::tokens::token::Token, queries::create_keyspace_query::CreateKeyspaceQuery,
    utils::errors::Errors,
};
use std::iter::Peekable;
use std::{collections::HashMap, vec::IntoIter};

const INVALID_PARAMETERS: &str = "Query lacks parameters";
const WITH: &str = "WITH";
const UNEXPECTED_TOKEN: &str = "Unexpected token in table_name";
const MISSING_COLON: &str = "Missing colon for separating parameters in replication";
const MISSING_WITH: &str = "Missing WITH keyword";
const MISSING_KEY: &str = "Missing key for replication";
const INVALID_KEY: &str = "Invalid key for replication";
const MISSING_VALUE: &str = "Missing value for replication";
const COMMA: &str = ",";
const COLON: &str = ":";

pub struct CreateKeyspaceParser;

impl CreateKeyspaceParser {
    pub fn parse(&self, tokens: Vec<Token>) -> Result<CreateKeyspaceQuery, Errors> {
        let mut create_keyspace_query = CreateKeyspaceQuery::new();
        self.keyspace_name(
            &mut tokens.into_iter().peekable(),
            &mut create_keyspace_query,
        )?;
        Ok(create_keyspace_query)
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
            _ => Err(Errors::SyntaxError(String::from(MISSING_WITH))),
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
            Token::Term(Term::Literal(literal)) => {
                let key = literal.value;
                if key == STRATEGY || key == REPLICATION {
                    self.check_colon(tokens_list, key, replication)
                } else {
                    Err(Errors::SyntaxError(String::from(MISSING_KEY)))
                }
                
            }
            _ => Err(Errors::SyntaxError(String::from(MISSING_KEY))),
        }
    }

    fn check_colon(
        &self,
        tokens_list: &mut Peekable<IntoIter<Token>>,
        key: String,
        replication: &mut HashMap<String, String>,
    ) -> Result<(), Errors> {
        let tok = self.get_next_value(tokens_list)?;
        match tok {
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
    const KEYSPACE_NAME: &str = "keyspace_name";
    const CLASS: &str = "class";
    const SIMPLE_STRATEGY: &str = "SimpleStrategy";
    const REPLICATION_FACTOR_KEWORD: &str = "replication_factor";
    const REPLICATION_FACTOR_VALUE: &str = "1";

    fn create_tokens() -> Vec<Token> {
        vec![
            Token::Identifier(KEYSPACE_NAME.to_string()),
            Token::Reserved(WITH.to_string()),
        ]
    }

    fn create_brace_list() -> Vec<Token> {
        vec![Token::BraceList(vec![
            Token::Term(Term::Literal(Literal::new(
                "class".to_string(),
                DataType::Text,
            ))),
            Token::Symbol(COLON.to_string()),
            Token::Term(Term::Literal(Literal::new(
                SIMPLE_STRATEGY.to_string(),
                DataType::Text,
            ))),
            Token::Symbol(COMMA.to_string()),
            Token::Term(Term::Literal(Literal::new(
                "replication_factor".to_string(),
                DataType::Text,
            ))),
            Token::Symbol(COLON.to_string()),
            Token::Term(Term::Literal(Literal::new(
                REPLICATION_FACTOR_VALUE.to_string(),
                DataType::Int,
            ))),
        ])]
    }

    #[test]
    fn test_01_create_keyspace_is_valid() {
        let mut tokens = create_tokens();
        let barace_list = create_brace_list();

        tokens.extend(barace_list);

        let parser = CreateKeyspaceParser;
        let result = parser.parse(tokens);

        assert!(result.is_ok());

        let query = result.unwrap();
        assert_eq!(query.keyspace, KEYSPACE_NAME.to_string());
        assert_eq!(query.replication.get(CLASS).unwrap(), SIMPLE_STRATEGY);
        assert_eq!(
            query.replication.get(REPLICATION_FACTOR_KEWORD).unwrap(),
            REPLICATION_FACTOR_VALUE
        );
    }

    #[test]
    fn test_02_create_keyspace_missing_with_keyword_should_error() {
        let mut tokens = vec![Token::Identifier(KEYSPACE_NAME.to_string())];

        let barace_list = create_brace_list();

        tokens.extend(barace_list);
        let parser = CreateKeyspaceParser;
        let result = parser.parse(tokens);

        assert!(result.is_err());
        if let Err(Errors::SyntaxError(msg)) = result {
            assert_eq!(msg, MISSING_WITH);
        } else {
            panic!("El error no es del tipo Errors::SyntaxError");
        }
    }
    #[test]
    fn test_03_create_keyspace_missing_brace_list_should_error() {
        let tokens = create_tokens();
        let parser = CreateKeyspaceParser;
        let result = parser.parse(tokens);

        assert!(result.is_err());
        if let Err(Errors::SyntaxError(msg)) = result {
            assert_eq!(msg, INVALID_PARAMETERS);
        } else {
            panic!("El error no es del tipo Errors::SyntaxError");
        }
    }
    #[test]
    fn test_04_create_keyspace_missing_key_in_brace_list_should_error() {
        let mut tokens = create_tokens();
        let barace_list = vec![Token::BraceList(vec![
            Token::Symbol(COLON.to_string()),
            Token::Term(Term::Literal(Literal::new(
                SIMPLE_STRATEGY.to_string(),
                DataType::Text,
            ))),
        ])];

        tokens.extend(barace_list);
        let parser = CreateKeyspaceParser;
        let result = parser.parse(tokens);

        assert!(result.is_err());
        if let Err(Errors::SyntaxError(msg)) = result {
            assert_eq!(msg, MISSING_KEY);
        } else {
            panic!("El error no es del tipo Errors::SyntaxError");
        }
    }
    #[test]
    fn test_05_create_keyspace_missing_value_in_brace_list_should_error() {
        let mut tokens = create_tokens();
        let barace_list = vec![Token::BraceList(vec![
            Token::Term(Term::Literal(Literal::new(
                "class".to_string(),
                DataType::Text,
            ))),
            Token::Symbol(COLON.to_string()),
            Token::Symbol(COMMA.to_string()),
        ])];

        tokens.extend(barace_list);
        let parser = CreateKeyspaceParser;
        let result = parser.parse(tokens);

        assert!(result.is_err());
        if let Err(Errors::SyntaxError(msg)) = result {
            assert_eq!(msg, MISSING_VALUE);
        } else {
            panic!("El error no es del tipo Errors::SyntaxError");
        }
    }

    #[test]
    fn test_06_create_keyspace_missing_colon_in_brace_list_should_error() {
        let mut tokens = create_tokens();
        let barace_list = vec![Token::BraceList(vec![
            Token::Term(Term::Literal(Literal::new(
                "class".to_string(),
                DataType::Text,
            ))),
            Token::Term(Term::Literal(Literal::new(
                SIMPLE_STRATEGY.to_string(),
                DataType::Text,
            ))),
            Token::Symbol(COMMA.to_string()),
        ])];

        tokens.extend(barace_list);
        let parser = CreateKeyspaceParser;
        let result = parser.parse(tokens);

        assert!(result.is_err());
        if let Err(Errors::SyntaxError(msg)) = result {
            assert_eq!(msg, MISSING_COLON);
        } else {
            panic!("El error no es del tipo Errors::SyntaxError");
        }
    }
}
