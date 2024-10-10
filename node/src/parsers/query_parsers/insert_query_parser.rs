use crate::parsers::tokens::token::{Literal, Term, Token};
use crate::queries::insert_query::InsertQuery;
use crate::utils::errors::Errors;
use std::vec::IntoIter;

const INTO: &str = "INTO";
const VALUES: &str = "VALUES";

pub struct InsertQueryParser;

impl InsertQueryParser {
    pub fn parse(tokens_list: Vec<Token>) -> Result<InsertQuery, Errors> {
        let mut insert_query = InsertQuery::new();
        into(&mut tokens_list.into_iter(), &mut insert_query)?;
        Ok(insert_query)
    }
}

fn into(tokens: &mut IntoIter<Token>, query: &mut InsertQuery) -> Result<(), Errors> {
    match get_next_value(tokens)? {
        Token::Reserved(res) if res == *INTO => table(tokens, query),
        _ => Err(Errors::SyntaxError(String::from(
            "INSERT not followed by INTO",
        ))),
    }
}

fn table(tokens: &mut IntoIter<Token>, query: &mut InsertQuery) -> Result<(), Errors> {
    match get_next_value(tokens)? {
        Token::Identifier(identifier) => {
            query.table = identifier;
            headers(tokens, query)
        }
        _ => Err(Errors::SyntaxError(String::from(
            "Unexpected token in table_name",
        ))),
    }
}
fn headers(tokens: &mut IntoIter<Token>, query: &mut InsertQuery) -> Result<(), Errors> {
    match get_next_value(tokens)? {
        Token::TokensList(list) => {
            query.headers = get_headers(list)?;
            values(tokens, query)
        }
        _ => Err(Errors::SyntaxError(String::from(
            "Unexpected token in headers",
        ))),
    }
}

fn values(tokens: &mut IntoIter<Token>, query: &mut InsertQuery) -> Result<(), Errors> {
    match get_next_value(tokens)? {
        Token::Reserved(res) if res == *VALUES => values_list(tokens, query),
        _ => Err(Errors::SyntaxError(String::from(
            "headers not followed by VALUES",
        ))),
    }
}

fn values_list(tokens: &mut IntoIter<Token>, query: &mut InsertQuery) -> Result<(), Errors> {
    let Some(token) = tokens.next() else {
        return Ok(());
    };
    match token {
        Token::TokensList(list) => {
            query.values_list.push(get_values(list)?);
            values_list(tokens, query)
        }
        _ if query.values_list.iter().len() == 0 => Err(Errors::SyntaxError(String::from(
            "No values where provided",
        ))),
        _ => Ok(()),
    }
}

fn get_headers(list: Vec<Token>) -> Result<Vec<String>, Errors> {
    let mut headers = Vec::new();
    for elem in list {
        match elem {
            Token::Identifier(header) => headers.push(header),
            _ => {
                return Err(Errors::SyntaxError(String::from(
                    "Unexpected token in headers",
                )))
            }
        }
    }
    Ok(headers)
}
fn get_values(list: Vec<Token>) -> Result<Vec<Literal>, Errors> {
    let mut values = Vec::new();
    for elem in list {
        match elem {
            Token::Term(Term::Literal(literal)) => values.push(literal),
            _ => {
                return Err(Errors::SyntaxError(String::from(
                    "Unexpected token in values",
                )))
            }
        }
    }
    Ok(values)
}

fn get_next_value(tokens: &mut IntoIter<Token>) -> Result<Token, Errors> {
    tokens
        .next()
        .ok_or(Errors::SyntaxError(String::from("Query lacks parameters")))
}

#[cfg(test)]
mod tests {
    use crate::parsers::tokens::token::{DataType, Token};

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
            Token::TokensList(vec![
                Token::Identifier(String::from(hd1)),
                Token::Identifier(String::from(hd2)),
            ]),
            Token::Reserved(String::from(values)),
            Token::TokensList(vec![
                Token::Term(Term::Literal(Literal {
                    valor: col1.to_string(),
                    tipo: DataType::Integer,
                })),
                Token::Term(Term::Literal(Literal {
                    valor: col2.to_string(),
                    tipo: DataType::Text,
                })),
            ]),
        ]
    }
    fn get_insert_query(table: &str, hd1: &str, hd2: &str, col1: &str, col2: &str) -> InsertQuery {
        InsertQuery {
            table: table.to_string(),
            headers: vec![String::from(hd1), String::from(hd2)],
            values_list: vec![vec![
                Literal {
                    valor: col1.to_string(),
                    tipo: DataType::Integer,
                },
                Literal {
                    valor: col2.to_string(),
                    tipo: DataType::Text,
                },
            ]],
        }
    }

    #[test]
    fn test_insert_query_parser_valid() {
        let tokens = get_insert_tokens(INTO, "table_name", "id", "name", VALUES, "3", "Thiago");
        let expected = get_insert_query("table_name", "id", "name", "3", "Thiago");
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
            Token::TokensList(vec![
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
            Token::Identifier(String::from("table_name")),
            Token::Reserved(String::from(VALUES)),
        ];
        let result = InsertQueryParser::parse(tokens);
        assert_error(result, "Unexpected token in headers");
    }
    #[test]
    fn test_insert_query_parser_headers_are_not_identifiers() {
        let tokens = vec![
            Token::Reserved(String::from(INTO)),
            Token::Identifier(String::from("table_name")),
            Token::TokensList(vec![Token::Reserved(String::from("NOT AN IDENTIFIER"))]),
        ];
        let result = InsertQueryParser::parse(tokens);
        assert_error(result, "Unexpected token in headers");
    }

    #[test]
    fn test_insert_query_parser_missing_values() {
        let tokens = get_insert_tokens(INTO, "table_name", "id", "name", "NOT VALUES", "", "");
        let result = InsertQueryParser::parse(tokens);
        assert_error(result, "headers not followed by VALUES");
    }

    #[test]
    fn test_insert_query_parser_values_are_not_literals() {
        let tokens = vec![
            Token::Reserved(String::from(INTO)),
            Token::Identifier(String::from("table_name")),
            Token::TokensList(vec![
                Token::Identifier(String::from("id")),
                Token::Identifier(String::from("name")),
            ]),
            Token::Reserved(String::from(VALUES)),
            Token::TokensList(vec![Token::Reserved(String::from("NOT A LITERAL"))]),
        ];
        let result = InsertQueryParser::parse(tokens);
        assert_error(result, "Unexpected token in values");
    }
}
