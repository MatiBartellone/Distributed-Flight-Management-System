use crate::{
    parsers::tokens::token::Token,
    queries::query::Query,
    utils::{
        errors::Errors,
        types::token_conversor::get_next_value,
    },
};
use Token::*;
use crate::utils::parser_constants::{KEYSPACE, TABLE};
use super::{
    create_keyspace_parser::CreateKeyspaceParser, create_table_query_parser::CreateTableQueryParser,
};
pub struct CreateQueryParser;

impl CreateQueryParser {
    pub fn parse(tokens: Vec<Token>) -> Result<Box<dyn Query>, Errors> {
        let mut tokens_iter = tokens.into_iter().peekable();

        match get_next_value(&mut tokens_iter) {
            Ok(Reserved(res)) => {
                let tokens: Vec<Token> = tokens_iter.collect();
                match res.as_str() {
                    KEYSPACE => Ok(Box::new(CreateKeyspaceParser.parse(tokens)?)),
                    TABLE => Ok(Box::new(CreateTableQueryParser::parse(tokens)?)),
                    _ => Err(Errors::SyntaxError(format!("Unknown CREATE type: {}", res))),
                }
            }
            _ => Err(Errors::SyntaxError(
                "Invalid Syntaxis in CREATE".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::parsers::tokens::terms::{BooleanOperations, ComparisonOperators};
    use crate::{
        parsers::{
            query_parsers::create_query_parser::CreateQueryParser,
            tokens::{data_type::DataType, literal::Literal, terms::Term, token::Token},
        },
        utils::{
            errors::Errors,
            types::token_conversor::{
                create_identifier_token, create_paren_list_token, create_reserved_token,
                create_symbol_token, create_token_literal,
            },
        },
    };
    use crate::utils::parser_constants::COMMA;

    #[test]
    fn test_create_keyspace() {
        // KEYSPACE
        let tokens = vec![
            create_reserved_token("KEYSPACE"),
            create_identifier_token("KEYSPACE_NAME"),
            create_reserved_token("WITH"),
            create_reserved_token("REPLICATION"),
            Token::Term(Term::BooleanOperations(BooleanOperations::Comparison(
                ComparisonOperators::Equal,
            ))),
            Token::BraceList(vec![
                Token::Term(Term::Literal(Literal::new(
                    "class".to_string(),
                    DataType::Text,
                ))),
                Token::Symbol(":".to_string()),
                Token::Term(Term::Literal(Literal::new(
                    "SimpleStrategy".to_string(),
                    DataType::Text,
                ))),
                Token::Symbol(COMMA.to_string()),
                Token::Term(Term::Literal(Literal::new(
                    "replication_factor".to_string(),
                    DataType::Text,
                ))),
                Token::Symbol(":".to_string()),
                Token::Term(Term::Literal(Literal::new(1.to_string(), DataType::Int))),
            ]),
        ];
        let result = CreateQueryParser::parse(tokens);
        assert!(result.is_ok(), "Expected Ok for CREATE KEYSPACE");
    }

    #[test]
    fn test_create_table() {
        let tokens = vec![
            create_reserved_token("TABLE"),
            create_identifier_token("table_name"),
            create_paren_list_token(vec![
                create_identifier_token("id"),
                Token::DataType(DataType::Int),
                create_reserved_token("PRIMARY"),
                create_reserved_token("KEY"),
                create_symbol_token(COMMA),
                create_identifier_token("name"),
                Token::DataType(DataType::Text),
                create_symbol_token(COMMA),
            ]),
        ];

        let result = CreateQueryParser::parse(tokens);
        assert!(result.is_ok(), "Expected Ok for CREATE TABLE");
    }

    #[test]
    fn test_unknown_create_type() {
        // UNKNOWN CREATE
        let tokens = vec![create_reserved_token("UNKNOWN_CREATE")];

        let result = CreateQueryParser::parse(tokens);
        assert!(result.is_err(), "Expected Err for unknown CREATE type");
        if let Err(Errors::SyntaxError(msg)) = result {
            assert!(
                msg.contains("Unknown CREATE type"),
                "Error message does not match"
            );
        } else {
            panic!("Expected a SyntaxError");
        }
    }

    #[test]
    fn test_invalid_syntax() {
        // UNKNOWN TYPE
        let tokens = vec![create_token_literal("UNKNOWN_TYPE", DataType::Text)];

        let result = CreateQueryParser::parse(tokens);
        assert!(result.is_err(), "Expected Err for invalid syntax");
        if let Err(Errors::SyntaxError(msg)) = result {
            assert!(
                msg.contains("Invalid Syntaxis in CREATE"),
                "Error message does not match"
            );
        } else {
            panic!("Expected a SyntaxError");
        }
    }
}
