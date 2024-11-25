use crate::parsers::tokens::literal::Literal;
use crate::parsers::tokens::terms::Term;
use crate::parsers::tokens::token::Token;
use crate::queries::insert_query::InsertQuery;
use crate::utils::constants::*;
use crate::utils::errors::Errors;
use crate::utils::token_conversor::get_next_value;
use std::iter::Peekable;
use std::vec::IntoIter;
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

fn values_list(tokens: &mut Peekable<IntoIter<Token>>, query: &mut InsertQuery) -> Result<(), Errors> {
    let Some(token) = tokens.next() else {
        return Ok(());
    };
    match token {
        Token::ParenList(list) => {
            query.values_list.push(get_values(list)?);
            values_list(tokens, query)
        }
        _ if query.values_list.iter().len() == 0 => Err(Errors::SyntaxError(String::from(
            "No values where provided",
        ))),
        _ => if_clause(tokens, query),
    }
}

fn if_clause(
    tokens: &mut Peekable<IntoIter<Token>>,
    query: &mut InsertQuery,
) -> Result<(), Errors> {
    match tokens.next() {
        Some(Token::Reserved(res)) if res == IF => {}
        Some(_) => return Err(Errors::SyntaxError("Unexpected token".to_string())),
        None => return Ok(()),
    };
    match get_next_value(tokens)? {
        Token::IterateToken(sub_list) => {
            query.if_exists = Some(exists(&mut sub_list.into_iter().peekable())?);
            if tokens.next().is_some() {
                return Err(Errors::SyntaxError(String::from(
                    "Nothing should follow a if-clause",
                )));
            }
            Ok(())
        }
        _ => Err(Errors::SyntaxError(
            "Unexpected token in if-clause".to_string(),
        )),
    }
}

fn exists(tokens: &mut Peekable<IntoIter<Token>>) -> Result<bool, Errors> {
    match tokens.next() {
        Some(Token::Reserved(res)) if res == EXISTS => {
            if tokens.next().is_some() {
                return Err(Errors::SyntaxError(String::from(
                    "Nothing should follow a if-clause",
                )));
            }
            Ok(true)
        }
        Some(Token::Reserved(res)) if res == NOT => {
            match tokens.next() {
                Some(Token::Reserved(res)) if res == EXISTS => { 
                    if tokens.next().is_some() {
                        return Err(Errors::SyntaxError(String::from(
                            "Nothing should follow a if-clause",
                        )));
                    }
                    Ok(false)
                }
                _ => Err(Errors::SyntaxError("Unexpected token".to_string())),
            }
        }
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
    use crate::parsers::tokens::{
        data_type::DataType, literal::Literal, terms::Term, token::Token,
    };

    use super::*;

    fn assert_error(result: Result<InsertQuery, Errors>, expected: &str) {
        assert!(result.is_err());
        if let Err(Errors::SyntaxError(err)) = result {
            assert_eq!(err, expected);
        }
    }

    fn get_insert_tokens(
        into: &str,
        table: &str,
        hd1: &str,
        hd2: &str,
        values: &str,
        col1: &str,
        col2: &str,
    ) -> Vec<Token> {
        vec![
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
        ]
    }
    fn get_insert_query(table: &str, hd1: &str, hd2: &str, col1: &str, col2: &str) -> InsertQuery {
        InsertQuery {
            table_name: table.to_string(),
            headers: vec![String::from(hd1), String::from(hd2)],
            values_list: vec![vec![
                Literal::new(col1.to_string(), DataType::Int),
                Literal::new(col2.to_string(), DataType::Text),
            ]],
            if_exists: None
        }
    }

    #[test]
    fn test_insert_query_parser_valid() {
        let tokens = get_insert_tokens(INTO, "kp.table_name", "id", "name", VALUES, "3", "Thiago");
        let expected = get_insert_query("kp.table_name", "id", "name", "3", "Thiago");
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
        let tokens = get_insert_tokens(INTO, "kp.table_name", "id", "name", "NOT VALUES", "", "");
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
}
