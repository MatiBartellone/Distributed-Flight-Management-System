use std::vec::IntoIter;
use crate::parsers::tokens::token::{Token, Term, Literal};
use crate::queries::insert_query::InsertQuery;
use crate::utils::errors::Errors;

const  INTO : &str = "INTO";
const VALUES : &str = "VALUES";

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
        _ => Err(Errors::SyntaxError(String::from("INSERT not followed by INTO"))),
    }
}

fn table(tokens: &mut IntoIter<Token>, query: &mut InsertQuery) -> Result<(), Errors> {
    match get_next_value(tokens)? {
        Token::Identifier(identifier)  => {
            query.table = identifier;
            headers(tokens, query)
        },
        _ => Err(Errors::SyntaxError(String::from("Unexpected token in table_name"))),
    }
}
fn headers(tokens: &mut IntoIter<Token>, query: &mut InsertQuery) -> Result<(), Errors> {
    match get_next_value(tokens)? {
        Token::TokensList(list)  => {
            query.headers = get_headers(list)?;
            values(tokens, query)
        },
        _ => Err(Errors::SyntaxError(String::from("Unexpected token in headers"))),
    }
}

fn values(tokens: &mut IntoIter<Token>, query: &mut InsertQuery) -> Result<(), Errors> {
    match get_next_value(tokens)? {
        Token::Reserved(res) if res == *VALUES => values_list(tokens, query),
        _ => Err(Errors::SyntaxError(String::from("headers not followed by VALUES"))),
    }
}

fn values_list(tokens: &mut IntoIter<Token>, query: &mut InsertQuery) -> Result<(), Errors> {
    let Some(token) = tokens.next() else { return Ok(()) };
    match token {
        Token::TokensList(list)  => {
            query.values_list.push(get_values(list)?);
            values_list(tokens, query)
        },
        _ => Ok(()),
    }
}

fn get_headers(list: Vec<Token>) -> Result<Vec<String>, Errors> {
    let mut headers = Vec::new();
    for elem in list {
        match elem {
            Token::Identifier(header) => headers.push(header),
            _ => return Err(Errors::SyntaxError(String::from("Unexpected token in headers"))),
        }
    }
    Ok(headers)
}
fn get_values(list: Vec<Token>) -> Result<Vec<Literal>, Errors> {
    let mut values = Vec::new();
    for elem in list {
        match elem {
            Token::Term(Term::Literal(literal)) => values.push(literal),
            _ => return Err(Errors::SyntaxError(String::from("Unexpected token in values"))),
        }
    }
    Ok(values)
}

fn get_next_value(tokens: &mut IntoIter<Token>) -> Result<Token, Errors> {
    tokens.next().ok_or(Errors::SyntaxError(String::from("Query lacks parameters")))
}

#[cfg(test)]
mod tests {
    use crate::parsers::tokens::token::{DataType, Token};

    use super::*;

    #[test]
    fn test_insert_query_parser_valid() {
        let tokens = vec![
            Token::Reserved(String::from(INTO)),
            Token::Identifier(String::from("table_name")),
            Token::TokensList(vec![Token::Identifier(String::from("id")), Token::Identifier(String::from("name"))]),
            Token::Reserved(String::from(VALUES)),
            Token::TokensList(vec![
                Token::Term(Term::Literal(Literal{ valor: "3".to_string(), tipo: DataType::Bigint })),
                Token::Term(Term::Literal(Literal{ valor: "Thiago".to_string(), tipo: DataType::Text }))
            ]),
        ];

        let expected = InsertQuery {
            table: "table_name".to_string(),
            headers: vec![String::from("id"), String::from("name")],
            values_list: vec![vec![
                Literal { valor: "3".to_string(), tipo: DataType::Bigint },
                Literal { valor: "Thiago".to_string(), tipo: DataType::Text },
            ]],
        };

        assert_eq!(expected, InsertQueryParser::parse(tokens).unwrap());
    }

    #[test]
    fn test_insert_query_parser_missing_into() {
        let tokens = vec![Token::Identifier(String::from("table_name")), ];
        let result = InsertQueryParser::parse(tokens);
        assert!(result.is_err());
        if let Err(Errors::SyntaxError(err)) = result {
            assert_eq!(err, "INSERT not followed by INTO");
        }
    }
    #[test]
    fn test_insert_query_parser_unexpected_table_name() {
        let tokens = vec![
            Token::Reserved(String::from(INTO)),
            Token::TokensList(vec![Token::Identifier(String::from("id")), Token::Identifier(String::from("name"))]),
        ];
        let result = InsertQueryParser::parse(tokens);
        assert!(result.is_err());
        if let Err(Errors::SyntaxError(err)) = result {
            assert_eq!(err, "Unexpected token in table_name");
        }
    }
    #[test]
    fn test_insert_query_parser_unexpected_headers() {
        let tokens = vec![
            Token::Reserved(String::from(INTO)),
            Token::Identifier(String::from("table_name")),
            Token::Reserved(String::from(VALUES)),
        ];
        let result = InsertQueryParser::parse(tokens);
        assert!(result.is_err());
        if let Err(Errors::SyntaxError(err)) = result {
            assert_eq!(err, "Unexpected token in headers");
        }
    }
    #[test]
    fn test_insert_query_parser_headers_are_not_identifiers() {
        let tokens = vec![
            Token::Reserved(String::from(INTO)),
            Token::Identifier(String::from("table_name")),
            Token::TokensList(vec![Token::Reserved(String::from("NOT AN IDENTIFIER"))]),
        ];
        let result = InsertQueryParser::parse(tokens);
        assert!(result.is_err());
        if let Err(Errors::SyntaxError(err)) = result {
            assert_eq!(err, "Unexpected token in headers");
        }
    }

    #[test]
    fn test_insert_query_parser_missing_values() {
        let tokens = vec![
            Token::Reserved(String::from(INTO)),
            Token::Identifier(String::from("table_name")),
            Token::TokensList(vec![Token::Identifier(String::from("id")), Token::Identifier(String::from("name"))]),
            Token::Reserved(String::from("not VALUES")),
        ];
        let result = InsertQueryParser::parse(tokens);
        assert!(result.is_err());
        if let Err(Errors::SyntaxError(err)) = result {
            assert_eq!(err, "headers not followed by VALUES");
        }
    }

    #[test]
    fn test_insert_query_parser_values_are_not_literals() {
        let tokens = vec![
            Token::Reserved(String::from(INTO)),
            Token::Identifier(String::from("table_name")),
            Token::TokensList(vec![Token::Identifier(String::from("id")), Token::Identifier(String::from("name"))]),
            Token::Reserved(String::from(VALUES)),
            Token::TokensList(vec![Token::Reserved(String::from("NOT A LITERAL"))]),
        ];
        let result = InsertQueryParser::parse(tokens);
        assert!(result.is_err());
        if let Err(Errors::SyntaxError(err)) = result {
            assert_eq!(err, "Unexpected token in values");
        }
    }
}
