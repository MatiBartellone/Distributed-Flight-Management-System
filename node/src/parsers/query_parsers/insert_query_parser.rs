use crate::parsers::tokens::literal::Literal;
use crate::parsers::tokens::terms::Term;
use crate::parsers::tokens::token::Token;
use crate::queries::insert_query::InsertQuery;
use crate::utils::errors::Errors;
use crate::utils::parser_constants::{COMMA, EXISTS, IF, INTO, VALUES};
use crate::utils::types::token_conversor::get_next_value;
use std::iter::Peekable;
use std::vec::IntoIter;
use crate::parsers::tokens::terms::BooleanOperations::Logical;
use crate::parsers::tokens::terms::LogicalOperators::*;

pub struct InsertQueryParser;

impl InsertQueryParser {
    pub fn parse(tokens_list: Vec<Token>) -> Result<InsertQuery, Errors> {
        let mut insert_query = InsertQuery::default();
        into(&mut tokens_list.into_iter().peekable(), &mut insert_query)?;
        Ok(insert_query)
    }
}

fn into(tokens: &mut Peekable<IntoIter<Token>>, query: &mut InsertQuery) -> Result<(), Errors> {
    match get_next_value(tokens)? {
        Token::Reserved(res) if res == *INTO => table(tokens, query),
        _ => Err(Errors::SyntaxError(String::from(
            "INSERT not followed by INTO",
        ))),
    }
}

fn table(tokens: &mut Peekable<IntoIter<Token>>, query: &mut InsertQuery) -> Result<(), Errors> {
    match get_next_value(tokens)? {
        Token::Identifier(identifier) => {
            query.table_name = identifier;
            headers(tokens, query)
        }
        _ => Err(Errors::SyntaxError(String::from(
            "Unexpected token in table_name",
        ))),
    }
}
fn headers(tokens: &mut Peekable<IntoIter<Token>>, query: &mut InsertQuery) -> Result<(), Errors> {
    match get_next_value(tokens)? {
        Token::ParenList(list) => {
            query.headers = get_headers(list)?;
            values(tokens, query)
        }
        _ => Err(Errors::SyntaxError(String::from(
            "Unexpected token in headers",
        ))),
    }
}

fn values(tokens: &mut Peekable<IntoIter<Token>>, query: &mut InsertQuery) -> Result<(), Errors> {
    match get_next_value(tokens)? {
        Token::Reserved(res) if res == *VALUES => values_list(tokens, query),
        _ => Err(Errors::SyntaxError(String::from(
            "headers not followed by VALUES",
        ))),
    }
}

fn values_list(
    tokens: &mut Peekable<IntoIter<Token>>,
    query: &mut InsertQuery,
) -> Result<(), Errors> {
    match get_next_value(tokens)? {
        Token::ParenList(list) => {
            query.values = get_values(list)?;
            if_clause(tokens, query)
        }
        _ => Err(Errors::SyntaxError(String::from(
            "No values where provided",
        ))),
    }
}

fn if_clause(
    tokens: &mut Peekable<IntoIter<Token>>,
    query: &mut InsertQuery,
) -> Result<(), Errors> {
    match tokens.next() {
        Some(Token::Reserved(res)) if res == IF => {
            let mut tokens = match tokens.next() {
                Some(Token::IterateToken(list)) => list.into_iter().peekable(),
                _ => return Err(Errors::SyntaxError("Unexpected token in if-clause".to_string())),
            };
            query.if_exists = Some(exists(&mut tokens)?);
            check_following_token(&mut tokens)
        }
        Some(_) => Err(Errors::SyntaxError("Unexpected token".to_string())),
        None => Ok(()),
    }
}

fn check_following_token(tokens: &mut Peekable<IntoIter<Token>>) -> Result<(), Errors> {
    if tokens.next().is_some() {
        return Err(Errors::SyntaxError(String::from("Nothing should follow a if-clause")));
    }
    Ok(())
}

fn handle_exists(tokens: &mut Peekable<IntoIter<Token>>) -> Result<bool, Errors> {
    check_following_token(tokens)?;
    Ok(true)
}

fn handle_not_exists(tokens: &mut Peekable<IntoIter<Token>>) -> Result<bool, Errors> {
    check_following_token(tokens)?;
    Ok(false)
}

fn exists(tokens: &mut Peekable<IntoIter<Token>>) -> Result<bool, Errors> {
    match tokens.next() {
        Some(Token::Reserved(res)) if res == EXISTS => handle_exists(tokens),
        Some(Token::Term(Term::BooleanOperations(Logical(Not)))) => match tokens.next() {
            Some(Token::Reserved(res)) if res == EXISTS => handle_not_exists(tokens),
            _ => Err(Errors::SyntaxError("Unexpected token".to_string())),
        },
        _ => Err(Errors::SyntaxError("Unexpected token in if-clause".to_string())),
    }
}

fn get_headers(list: Vec<Token>) -> Result<Vec<String>, Errors> {
    let mut headers = Vec::new();
    for (index, elem) in list.iter().enumerate() {
        if index % 2 == 0 {
            match elem {
                Token::Identifier(header) => headers.push(header.to_string()),
                _ => {
                    return Err(Errors::SyntaxError(String::from(
                        "Unexpected token in headers",
                    )))
                }
            }
        } else {
            match elem {
                Token::Symbol(symbol) if symbol == COMMA => continue,
                _ => {
                    return Err(Errors::SyntaxError(String::from(
                        "Column names must be separated by comma",
                    )))
                }
            }
        }
    }
    Ok(headers)
}
fn get_values(list: Vec<Token>) -> Result<Vec<Literal>, Errors> {
    let mut values = Vec::new();
    for (index, elem) in list.iter().enumerate() {
        if index % 2 == 0 {
            match elem {
                Token::Term(Term::Literal(literal)) => values.push(literal.to_owned()),
                _ => {
                    return Err(Errors::SyntaxError(String::from(
                        "Unexpected token in values",
                    )))
                }
            }
        } else {
            match elem {
                Token::Symbol(symbol) if symbol == COMMA => continue,
                _ => {
                    return Err(Errors::SyntaxError(String::from(
                        "Values must be separated by comma",
                    )))
                }
            }
        }
    }
    Ok(values)
}

#[cfg(test)]
mod tests {
    use std::vec;

    use crate::parsers::tokens::{
        data_type::DataType, literal::Literal, terms::Term, token::Token,
    };
    use crate::parsers::tokens::terms::BooleanOperations::Logical;
    use super::*;

    fn assert_error(result: Result<InsertQuery, Errors>, expected: &str) {
        assert!(result.is_err());
        if let Err(Errors::SyntaxError(err)) = result {
            assert_eq!(err, expected);
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn get_insert_tokens(
        into: &str,
        table: &str,
        hd1: &str,
        hd2: &str,
        values: &str,
        col1: &str,
        col2: &str,
        if_clause: Option<&str>,
        exists: Option<&str>,
    ) -> Vec<Token> {
        let mut tokens = vec![
            Token::Reserved(String::from(into)),
            Token::Identifier(String::from(table)),
            Token::ParenList(vec![
                Token::Identifier(String::from(hd1)),
                Token::Symbol(String::from(COMMA)),
                Token::Identifier(String::from(hd2)),
            ]),
            Token::Reserved(String::from(values)),
            Token::ParenList(vec![
                Token::Term(Term::Literal(Literal::new(col1.to_string(), DataType::Int))),
                Token::Symbol(String::from(COMMA)),
                Token::Term(Term::Literal(Literal::new(
                    col2.to_string(),
                    DataType::Text,
                ))),
            ]),
        ];

        if let Some(if_clause) = if_clause {
            tokens.push(Token::Reserved(if_clause.to_string()));
        }
        if let Some(exists) = exists {
            let list = match exists {
                "NOT EXISTS" => vec![Token::Term(Term::BooleanOperations(Logical(Not))), Token::Reserved(EXISTS.to_string())],
                "EXISTS" => vec![Token::Reserved(EXISTS.to_string())],
                _ => vec![Token::Reserved(exists.to_string())],
            };
            tokens.push(Token::IterateToken(list));
        }
        tokens
    }

    fn get_insert_query(table: &str, hd1: &str, hd2: &str, col1: &str, col2: &str, if_exists: Option<bool>) -> InsertQuery {
        InsertQuery {
            table_name: table.to_string(),
            headers: vec![String::from(hd1), String::from(hd2)],
            values: vec![
                Literal::new(col1.to_string(), DataType::Int),
                Literal::new(col2.to_string(), DataType::Text),
            ],
            if_exists,
        }
    }

    #[test]
    fn test_insert_query_parser_valid() {
        let tokens = get_insert_tokens(INTO, "kp.table_name", "id", "name", VALUES, "3", "Thiago", None, None);
        let expected = get_insert_query("kp.table_name", "id", "name", "3", "Thiago", None);
        assert_eq!(expected, InsertQueryParser::parse(tokens).unwrap());
    }

    #[test]
    fn test_insert_query_parser_missing_into() {
        let tokens = vec![Token::Identifier(String::from("table_name"))];
        let result = InsertQueryParser::parse(tokens);
        assert_error(result, "INSERT not followed by INTO");
    }

    #[test]
    fn test_insert_query_parser_unexpected_table_name() {
        let tokens = vec![
            Token::Reserved(String::from(INTO)),
            Token::ParenList(vec![
                Token::Identifier(String::from("id")),
                Token::Identifier(String::from("name")),
            ]),
        ];
        let result = InsertQueryParser::parse(tokens);
        assert_error(result, "Unexpected token in table_name");
    }

    #[test]
    fn test_insert_query_parser_unexpected_headers() {
        let tokens = vec![
            Token::Reserved(String::from(INTO)),
            Token::Identifier(String::from("kp.table_name")),
            Token::Reserved(String::from(VALUES)),
        ];
        let result = InsertQueryParser::parse(tokens);
        assert_error(result, "Unexpected token in headers");
    }

    #[test]
    fn test_insert_query_parser_headers_are_not_identifiers() {
        let tokens = vec![
            Token::Reserved(String::from(INTO)),
            Token::Identifier(String::from("kp.table_name")),
            Token::ParenList(vec![Token::Reserved(String::from("NOT AN IDENTIFIER"))]),
        ];
        let result = InsertQueryParser::parse(tokens);
        assert_error(result, "Unexpected token in headers");
    }

    #[test]
    fn test_insert_query_parser_missing_values() {
        let tokens = get_insert_tokens(INTO, "kp.table_name", "id", "name", "NOT VALUES", "", "", None, None);
        let result = InsertQueryParser::parse(tokens);
        assert_error(result, "headers not followed by VALUES");
    }

    #[test]
    fn test_insert_query_parser_values_are_not_literals() {
        let tokens = vec![
            Token::Reserved(String::from(INTO)),
            Token::Identifier(String::from("kp.table_name")),
            Token::ParenList(vec![
                Token::Identifier(String::from("id")),
                Token::Symbol(String::from(COMMA)),
                Token::Identifier(String::from("name")),
            ]),
            Token::Reserved(String::from(VALUES)),
            Token::ParenList(vec![Token::Reserved(String::from("NOT A LITERAL"))]),
        ];
        let result = InsertQueryParser::parse(tokens);
        assert_error(result, "Unexpected token in values");
    }

    #[test]
    fn test_insert_query_parser_exists_clause_valid_exists() {
        let tokens = get_insert_tokens(INTO, "kp.table_name", "id", "name", VALUES, "3", "Thiago", Some("IF"), Some("EXISTS"));
        let expected = get_insert_query("kp.table_name", "id", "name", "3", "Thiago", Some(true));
        let result = InsertQueryParser::parse(tokens);
        assert_eq!(expected, result.unwrap());
    }

    #[test]
    fn test_insert_query_parser_exists_clause_valid_not_exists() {
        let tokens = get_insert_tokens(INTO, "kp.table_name", "id", "name", VALUES, "3", "Thiago", Some("IF"), Some("NOT EXISTS"));
        let expected = get_insert_query("kp.table_name", "id", "name", "3", "Thiago", Some(false));
        let result = InsertQueryParser::parse(tokens);
        assert_eq!(expected, result.unwrap());
    }

    #[test]
    fn test_insert_query_parser_exists_clause_invalid_token() {
        let tokens = get_insert_tokens(INTO, "kp.table_name", "id", "name", VALUES, "3", "Thiago", Some("IF"), Some("INVALID"));
        let result = InsertQueryParser::parse(tokens);
        assert_error(result, "Unexpected token in if-clause");
    }
}
