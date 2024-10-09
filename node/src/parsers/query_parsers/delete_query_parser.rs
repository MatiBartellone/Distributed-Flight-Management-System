use crate::parsers::query_parsers::where_clause::where_clause_parser::WhereClauseParser;
use crate::parsers::tokens::token::Token;
use crate::queries::delete_query::DeleteQuery;
use crate::utils::errors::Errors;
use std::vec::IntoIter;
const FROM: &str = "FROM";
const WHERE: &str = "WHERE";

pub struct DeleteQueryParser;

impl DeleteQueryParser {
    pub fn parse(tokens: Vec<Token>) -> Result<DeleteQuery, Errors> {
        let mut delete_query = DeleteQuery::new();
        from(&mut tokens.into_iter(), &mut delete_query)?;
        Ok(delete_query)
    }
}

fn from(tokens: &mut IntoIter<Token>, query: &mut DeleteQuery) -> Result<(), Errors> {
    match get_next_value(tokens)? {
        Token::Reserved(res) if res == *FROM => table(tokens, query),
        _ => Err(Errors::SyntaxError(String::from(
            "DELETE not followed by FROM",
        ))),
    }
}

fn table(tokens: &mut IntoIter<Token>, query: &mut DeleteQuery) -> Result<(), Errors> {
    match get_next_value(tokens)? {
        Token::Identifier(identifier) => {
            query.table = identifier;
            where_keyword(tokens, query)
        }
        _ => Err(Errors::SyntaxError(String::from(
            "Unexpected token in table_name",
        ))),
    }
}

fn where_keyword(tokens: &mut IntoIter<Token>, query: &mut DeleteQuery) -> Result<(), Errors> {
    let Some(token) = tokens.next() else {
        return Ok(());
    };
    match token {
        Token::Reserved(res) if res == *WHERE => where_clause(tokens, query),
        _ => Err(Errors::SyntaxError(String::from(
            "WHERE keyword not found",
        ))),
    }
}

fn where_clause(tokens: &mut IntoIter<Token>, query: &mut DeleteQuery) -> Result<(), Errors> {
    let Some(token) = tokens.next() else {
        return Ok(());
    };
    match token {
        Token::TokensList(list) => {
            query.where_clause = WhereClauseParser::parse(list)?;
            let None = tokens.next() else {
                return Err(Errors::SyntaxError(String::from(
                    "Nothing should follow a where-clause",
                )));
            };
            Ok(())
        }
        _ => Err(Errors::SyntaxError(String::from(
            "Unexpected token in where_clause",
        ))),
    }
}

fn get_next_value(tokens: &mut IntoIter<Token>) -> Result<Token, Errors> {
    tokens
        .next()
        .ok_or(Errors::SyntaxError(String::from("Query lacks parameters")))
}


#[cfg(test)]
mod tests {
    use crate::parsers::tokens::token::{Token};
    use super::*;

    fn assert_error(result: Result<DeleteQuery, Errors>, expected: &str) {
        assert!(result.is_err());
        if let Err(Errors::SyntaxError(err)) = result {
            assert_eq!(err, expected);
        }
    }
    #[test]
    fn test_insert_query_parser_valid_no_where() {
        let tokens = vec![
            Token::Reserved(String::from(FROM)),
            Token::Identifier(String::from("table_name")),
        ];
        let expected = DeleteQuery {
            table: "table_name".to_string(),
            where_clause: None
        };
        assert_eq!(expected, DeleteQueryParser::parse(tokens).unwrap());
    }

    #[test]
    fn test_insert_query_parser_missing_from() {
        let tokens = vec![Token::Identifier(String::from("table_name"))];
        let result = DeleteQueryParser::parse(tokens);
        assert_error(result, "DELETE not followed by FROM");
    }
    #[test]
    fn test_insert_query_parser_unexpected_table_name() {
        let tokens = vec![
            Token::Reserved(String::from(FROM)),
            Token::Reserved(String::from("UNEXPECTED")),
        ];
        let result = DeleteQueryParser::parse(tokens);
        assert_error(result, "Unexpected token in table_name");
    }

    #[test]
    fn test_insert_query_parser_unexpected_in_where() {
        let tokens = vec![
            Token::Reserved(String::from(FROM)),
            Token::Identifier(String::from("table_name")),
            Token::Reserved(String::from("NOT WHERE")),
        ];
        let result = DeleteQueryParser::parse(tokens);
        assert_error(result, "WHERE keyword not found");
    }

    #[test]
    fn test_insert_query_parser_unexpected_in_where_clause() {
        let tokens = vec![
            Token::Reserved(String::from(FROM)),
            Token::Identifier(String::from("table_name")),
            Token::Reserved(String::from(WHERE)),
            Token::Reserved(String::from("UNEXPECTED")),
        ];
        let result = DeleteQueryParser::parse(tokens);
        assert_error(result, "Unexpected token in where_clause");
    }
}
