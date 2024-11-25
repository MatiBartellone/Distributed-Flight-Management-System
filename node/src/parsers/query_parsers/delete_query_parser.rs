use crate::parsers::tokens::token::Token;
use crate::queries::delete_query::DeleteQuery;
use crate::utils::errors::Errors;
use crate::utils::token_conversor::get_next_value;
use std::iter::Peekable;
use std::vec::IntoIter;

use super::if_clause_parser::IfClauseParser;
use super::where_clause_parser::WhereClauseParser;
use crate::utils::constants::*;
pub struct DeleteQueryParser;

impl DeleteQueryParser {
    pub fn parse(tokens: Vec<Token>) -> Result<DeleteQuery, Errors> {
        let mut delete_query = DeleteQuery::default();
        from(&mut tokens.into_iter().peekable(), &mut delete_query)?;
        Ok(delete_query)
    }
}

fn from(tokens: &mut Peekable<IntoIter<Token>>, query: &mut DeleteQuery) -> Result<(), Errors> {
    match get_next_value(tokens)? {
        Token::Reserved(res) if res == *FROM => table(tokens, query),
        _ => Err(Errors::SyntaxError(String::from(
            "DELETE not followed by FROM",
        ))),
    }
}

fn table(tokens: &mut Peekable<IntoIter<Token>>, query: &mut DeleteQuery) -> Result<(), Errors> {
    match get_next_value(tokens)? {
        Token::Identifier(identifier) => {
            query.table_name = identifier;
            where_clause(tokens, query)
        }
        _ => Err(Errors::SyntaxError(String::from(
            "Unexpected token in table_name",
        ))),
    }
}

fn where_clause(
    tokens: &mut Peekable<IntoIter<Token>>,
    query: &mut DeleteQuery,
) -> Result<(), Errors> {
    match tokens.peek() {
        Some(Token::Reserved(res)) if res == WHERE => tokens.next(),
        _ => return if_clause(tokens, query),
    };
    match get_next_value(tokens)? {
        Token::IterateToken(sub_list) => {
            query.where_clause = Some(WhereClauseParser::parse(sub_list)?);
            if_clause(tokens, query)
        }
        _ => Err(Errors::SyntaxError(
            "Unexpected token in where_clause".to_string(),
        )),
    }
}

fn if_clause(
    tokens: &mut Peekable<IntoIter<Token>>,
    query: &mut DeleteQuery,
) -> Result<(), Errors> {
    match tokens.next() {
        Some(Token::Reserved(res)) if res == IF => {}
        Some(_) => return Err(Errors::SyntaxError("Unexpected token".to_string())),
        None => return Ok(()),
    };
    match get_next_value(tokens)? {
        Token::IterateToken(sub_list) => {
            query.if_clause = Some(IfClauseParser::parse(sub_list)?);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parsers::tokens::token::Token;

    fn assert_error(result: Result<DeleteQuery, Errors>, expected: &str) {
        assert!(result.is_err());
        if let Err(Errors::SyntaxError(err)) = result {
            assert_eq!(err, expected);
        }
    }
    #[test]
    fn test_delete_query_parser_valid_no_where_and_if() {
        let tokens = vec![
            Token::Reserved(String::from(FROM)),
            Token::Identifier(String::from("kp.table_name")),
        ];
        let expected = DeleteQuery {
            table_name: "kp.table_name".to_string(),
            where_clause: None,
            if_clause: None,
        };
        assert_eq!(expected, DeleteQueryParser::parse(tokens).unwrap());
    }

    #[test]
    fn test_delete_query_parser_missing_from() {
        let tokens = vec![Token::Identifier(String::from("table_name"))];
        let result = DeleteQueryParser::parse(tokens);
        assert_error(result, "DELETE not followed by FROM");
    }

    #[test]
    fn test_delete_query_parser_unexpected_table_name() {
        let tokens = vec![
            Token::Reserved(String::from(FROM)),
            Token::Reserved(String::from("UNEXPECTED")),
        ];
        let result = DeleteQueryParser::parse(tokens);
        assert_error(result, "Unexpected token in table_name");
    }

    #[test]
    fn test_delete_query_parser_unexpected_in_where() {
        let tokens = vec![
            Token::Reserved(String::from(FROM)),
            Token::Identifier(String::from("table_name")),
            Token::Reserved(String::from("NOT WHERE")),
        ];
        let result = DeleteQueryParser::parse(tokens);
        assert_error(result, "Unexpected token");
    }

    #[test]
    fn test_delete_query_parser_unexpected_in_where_clause() {
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
