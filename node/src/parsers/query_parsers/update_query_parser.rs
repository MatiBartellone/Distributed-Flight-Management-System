use Token::*;
use std::{iter::Peekable, vec::IntoIter};

use crate::{parsers::tokens::token::Token, queries::update_query::UpdateQuery, utils::{errors::Errors, token_conversor::{get_next_value, get_sublist}}};

use super::{if_clause_parser::IfClauseParser, set_clause_parser::SetClauseParser, where_clause_parser::WhereClauseParser};

const SET: &str = "SET";
const WHERE: &str = "WHERE";
const IF: &str = "IF";

pub struct UpdateQueryParser;

impl UpdateQueryParser {
    pub fn parse(tokens: Vec<Token>) -> Result<UpdateQuery, Errors> {
        let mut update_query = UpdateQuery::new();
        table(&mut tokens.into_iter().peekable(), &mut update_query)?;
        Ok(update_query)
    }
}

fn table(tokens: &mut Peekable<IntoIter<Token>>, query: &mut UpdateQuery) -> Result<(), Errors> {
    match get_next_value(tokens)? {
        Identifier(table_name) => {
            query.table = table_name;
            set(tokens, query)
        }
        _ => Err(Errors::SyntaxError(String::from(
            "UPDATE not followed by a table name",
        ))),
    }
}

fn set(tokens: &mut Peekable<IntoIter<Token>>, query: &mut UpdateQuery) -> Result<(), Errors> {
    match get_next_value(tokens)? {
        Reserved(res) if res == *SET => {
            let sublist = get_sublist(tokens)?;
            query.changes = SetClauseParser::parse(sublist)?;
            where_clause(tokens, query)
        }
        _ => Err(Errors::SyntaxError("SET not found".to_string())),
    }
}

fn where_clause(tokens: &mut Peekable<IntoIter<Token>>, query: &mut UpdateQuery) -> Result<(), Errors> {
    match tokens.peek() {
        Some(Reserved(res)) if res == WHERE => tokens.next(),
        _ => return if_clause(tokens, query),
    };
    match get_next_value(tokens)? {
        TokensList(sub_list) => {
            query.where_clause = Some(WhereClauseParser::parse(sub_list)?);
            if_clause(tokens, query)
        }
        _ => Err(Errors::SyntaxError("Unexpected token in where_clause".to_string()))
    }
}

fn if_clause(tokens: &mut Peekable<IntoIter<Token>>, query: &mut UpdateQuery) -> Result<(), Errors> {
    match tokens.next() {
        Some(Reserved(res)) if res == IF => {},
        _ => return Ok(())
    };
    match get_next_value(tokens)? {
        TokensList(sub_list) => {
            query.if_clause = Some(IfClauseParser::parse(sub_list)?);
            if tokens.next().is_some() {
                return Err(Errors::SyntaxError(String::from("Nothing should follow a if-clause")));
            }
            Ok(())
        }
        _ => Err(Errors::SyntaxError("Unexpected token in if-clause".to_string()))
    }
}

#[cfg(test)]
mod tests {
    use crate::{parsers::{query_parsers::update_query_parser::UpdateQueryParser, tokens::token::{create_literal, ComparisonOperators, DataType, Token}}, queries::{if_clause::comparison_if, set_logic::assigmente_value::AssignmentValue, update_query::UpdateQuery, where_logic::where_clause::comparison_where}, utils::{errors::Errors, token_conversor::{create_comparison_operation_token, create_identifier_token, create_list_token, create_reserved_token, create_token_literal}}};
    use ComparisonOperators::*;
    use DataType::*;
    
    fn test_successful_update_parser_case(tokens: Vec<Token>, expected_query: UpdateQuery) {
        let result = UpdateQueryParser::parse(tokens);
        match result {
            Ok(query) => {
                assert_eq!(
                    query, expected_query,
                    "El parser devolvió una consulta inesperada.\nEsperado: {:#?}\nObtenido: {:#?}",
                    expected_query, query
                );
            }
            Err(e) => {
                panic!("El parser devolvió un error inesperado: {}.\n", e);
            }
        }
    }

    fn test_failed_update_parser_case(tokens: Vec<Token>, expected_error: Errors) {
        let result = UpdateQueryParser::parse(tokens);
        assert!(result.is_err(), "El parser no falló cuando debía.");
        let error = result.unwrap_err();
        assert_eq!(error, expected_error, "El error recibido no coincide con el esperado.");
    }

    #[test]
    fn test_update_query_simple_assignment() {
        // table SET age = 30 WHERE id = 5;
        let tokens = vec![
            create_identifier_token("table"),
            create_reserved_token("SET"),
            create_list_token(vec![
                create_identifier_token("age"), 
                create_comparison_operation_token(Equal),
                create_token_literal("30", Integer)
            ]),
            create_reserved_token("WHERE"),
            create_list_token(vec![
                create_identifier_token("id"),
                create_comparison_operation_token(Equal),
                create_token_literal("5", Integer),
            ]),
        ];

        let mut expected_query = UpdateQuery::new();
        expected_query.table = "table".to_string();
        expected_query.changes.insert("age".to_string(), AssignmentValue::Simple(create_literal("30", Integer)));
        expected_query.where_clause = Some(comparison_where("id", ComparisonOperators::Equal, create_literal("5", Integer)));

        test_successful_update_parser_case(tokens, expected_query);
    }

    #[test]
    fn test_update_query_multiple_assignments() {
        // table SET age = 30, name = 'John' WHERE id = 5;
        let tokens = vec![
            create_identifier_token("table"),
            create_reserved_token("SET"),
            create_list_token(vec![
                create_identifier_token("age"),
                create_comparison_operation_token(Equal),
                create_token_literal("30", Integer),
                create_identifier_token("name"),
                create_comparison_operation_token(Equal),
                create_token_literal("John", Text),
            ]),
            create_reserved_token("WHERE"),
            create_list_token(vec![
                create_identifier_token("id"),
                create_comparison_operation_token(Equal),
                create_token_literal("5", Integer),
            ]),
        ];
    
        let mut expected_query = UpdateQuery::new();
        expected_query.table = "table".to_string();
        expected_query.changes.insert("age".to_string(), AssignmentValue::Simple(create_literal("30", Integer)));
        expected_query.changes.insert("name".to_string(), AssignmentValue::Simple(create_literal("John", Text)));
        expected_query.where_clause = Some(comparison_where("id", ComparisonOperators::Equal, create_literal("5", Integer)));
    
        test_successful_update_parser_case(tokens, expected_query);
    }

    #[test]
    fn test_update_query_with_if_clause() {
        // table SET age = 30 IF id = 5;
        let tokens = vec![
            create_identifier_token("table"),
            create_reserved_token("SET"),
            create_list_token(vec![
                create_identifier_token("age"),
                create_comparison_operation_token(Equal),
                create_token_literal("30", Integer),
            ]),
            create_reserved_token("IF"),
            create_list_token(vec![
                create_identifier_token("id"),
                create_comparison_operation_token(Equal),
                create_token_literal("5", Integer),
            ]),
        ];
    
        let mut expected_query = UpdateQuery::new();
        expected_query.table = "table".to_string();
        expected_query.changes.insert("age".to_string(), AssignmentValue::Simple(create_literal("30", Integer)));
        expected_query.if_clause = Some(comparison_if("id", ComparisonOperators::Equal, create_literal("5", Integer)));
    
        test_successful_update_parser_case(tokens, expected_query);
    }

    #[test]
    fn test_update_query_missing_set() {
        // table id = 5;
        let tokens = vec![
            create_identifier_token("table"),
            create_identifier_token("id"),
            create_comparison_operation_token(Equal),
            create_token_literal("5", Integer),
        ];

        test_failed_update_parser_case(tokens, Errors::SyntaxError("SET not found".to_string()));
    }

    #[test]
    fn test_update_query_invalid_assignment() {
        // table SET age 30 WHERE id = 5;
        let tokens = vec![
            create_identifier_token("table"),
            create_reserved_token("SET"),
            create_list_token(vec![
                create_identifier_token("age"),
                create_token_literal("30", Integer),
            ]),
            create_reserved_token("WHERE"),
            create_identifier_token("id"),
            create_comparison_operation_token(Equal),
            create_token_literal("5", Integer),
        ];

        test_failed_update_parser_case(tokens, Errors::SyntaxError("= should follow a SET assignment".to_string()));
    }
}