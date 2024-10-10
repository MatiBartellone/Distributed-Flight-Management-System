use crate::parsers::query_parsers::order_by_clause_parser::OrderByClauseParser;
use crate::parsers::query_parsers::where_clause_::where_clause_parser::WhereClauseParser;
use crate::parsers::tokens::token::Token;
use crate::queries::select_query::SelectQuery;
use crate::utils::errors::Errors;
use std::vec::IntoIter;

const FROM: &str = "FROM";
const WHERE: &str = "WHERE";
const ORDER: &str = "ORDER";
const BY: &str = "BY";

pub struct SelectQueryParser;

impl SelectQueryParser {
    pub fn parse(tokens: Vec<Token>) -> Result<SelectQuery, Errors> {
        let mut select_query = SelectQuery::new();
        columns(&mut tokens.into_iter(), &mut select_query)?;
        Ok(select_query)
    }
}

fn columns(tokens: &mut IntoIter<Token>, query: &mut SelectQuery) -> Result<(), Errors> {
    match get_next_value(tokens)? {
        Token::ParenList(list) => {
            query.columns = get_columns(list)?;
            from(tokens, query)
        }
        _ => Err(Errors::SyntaxError(String::from(
            "Unexpected token in columns",
        ))),
    }
}

fn from(tokens: &mut IntoIter<Token>, query: &mut SelectQuery) -> Result<(), Errors> {
    match get_next_value(tokens)? {
        Token::Reserved(res) if res == *FROM => table(tokens, query),
        _ => Err(Errors::SyntaxError(String::from(
            "Columns not followed by FROM",
        ))),
    }
}

fn table(tokens: &mut IntoIter<Token>, query: &mut SelectQuery) -> Result<(), Errors> {
    match get_next_value(tokens)? {
        Token::Identifier(identifier) => {
            query.table = identifier;
            modifiers(tokens, query)
        }
        _ => Err(Errors::SyntaxError(String::from(
            "Unexpected token in table_name",
        ))),
    }
}
fn modifiers(tokens: &mut IntoIter<Token>, query: &mut SelectQuery) -> Result<(), Errors> {
    let Some(token) = tokens.next() else {
        return Ok(());
    };
    match token {
        Token::Reserved(res) if res == *WHERE => where_clause(tokens, query),
        Token::Reserved(res) if res == *ORDER => by(tokens, query),
        _ => Err(Errors::SyntaxError(String::from(
            "Unexpected token in query",
        ))),
    }
}

fn where_clause(tokens: &mut IntoIter<Token>, query: &mut SelectQuery) -> Result<(), Errors> {
    match get_next_value(tokens)? {
        Token::IterateToken(list) => {
            query.where_clause = WhereClauseParser::parse(list)?;
            order(tokens, query)
        }
        _ => Err(Errors::SyntaxError(String::from(
            "Unexpected token in where_clause",
        ))),
    }
}

fn order(tokens: &mut IntoIter<Token>, query: &mut SelectQuery) -> Result<(), Errors> {
    let Some(token) = tokens.next() else {
        return Ok(());
    };
    match token {
        Token::Reserved(res) if res == *ORDER => by(tokens, query),
        _ => Err(Errors::SyntaxError(String::from(
            "Unexpected token in query",
        ))),
    }
}
fn by(tokens: &mut IntoIter<Token>, query: &mut SelectQuery) -> Result<(), Errors> {
    match get_next_value(tokens)? {
        Token::Reserved(res) if res == *BY => order_clause(tokens, query),
        _ => Err(Errors::SyntaxError(String::from(
            "ORDER not followed by BY",
        ))),
    }
}

fn order_clause(tokens: &mut IntoIter<Token>, query: &mut SelectQuery) -> Result<(), Errors> {
    match get_next_value(tokens)? {
        Token::IterateToken(list) => {
            query.order_clauses = Some(OrderByClauseParser::parse(list)?);
            let None = tokens.next() else {
                return Err(Errors::SyntaxError(String::from(
                    "Nothing should follow a order_clause",
                )));
            };
            Ok(())
        }
        _ => Err(Errors::SyntaxError(String::from(
            "Unexpected token in order_clause",
        ))),
    }
}

fn get_columns(list: Vec<Token>) -> Result<Vec<String>, Errors> {
    let mut columns = Vec::new();
    for elem in list {
        match elem {
            Token::Identifier(column) => columns.push(column),
            _ => {
                return Err(Errors::SyntaxError(String::from(
                    "Unexpected token in columns",
                )))
            }
        }
    }
    Ok(columns)
}

fn get_next_value(tokens: &mut IntoIter<Token>) -> Result<Token, Errors> {
    tokens
        .next()
        .ok_or(Errors::SyntaxError(String::from("Query lacks parameters")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parsers::tokens::token::Token;

    fn assert_error(result: Result<SelectQuery, Errors>, expected: &str) {
        assert!(result.is_err());
        if let Err(Errors::SyntaxError(err)) = result {
            assert_eq!(err, expected);
        }
    }
    #[test]
    fn test_select_query_parser_valid_no_where_no_order() {
        let tokens = vec![
            Token::ParenList(vec![Token::Identifier(String::from("id"))]),
            Token::Reserved(String::from(FROM)),
            Token::Identifier(String::from("table_name")),
        ];
        let expected = SelectQuery {
            columns: vec![String::from("id")],
            table: "table_name".to_string(),
            where_clause: None,
            order_clauses: None,
        };
        assert_eq!(expected, SelectQueryParser::parse(tokens).unwrap());
    }

    #[test]
    fn test_select_query_parser_unexpected_columns() {
        let tokens = vec![Token::Reserved(String::from(FROM))];
        let result = SelectQueryParser::parse(tokens);
        assert_error(result, "Unexpected token in columns");
    }
    #[test]
    fn test_select_query_parser_missing_from() {
        let tokens = vec![
            Token::ParenList(vec![Token::Identifier(String::from("id"))]),
            Token::Reserved(String::from("NOT FROM")),
        ];
        let result = SelectQueryParser::parse(tokens);
        assert_error(result, "Columns not followed by FROM");
    }
    #[test]
    fn test_select_query_parser_unexpected_table_name() {
        let tokens = vec![
            Token::ParenList(vec![Token::Identifier(String::from("id"))]),
            Token::Reserved(String::from(FROM)),
            Token::Reserved(String::from("UNEXPECTEDS")),
        ];
        let result = SelectQueryParser::parse(tokens);
        assert_error(result, "Unexpected token in table_name");
    }
    #[test]
    fn test_select_query_parser_unexpected_modifiers() {
        let tokens = vec![
            Token::ParenList(vec![Token::Identifier(String::from("id"))]),
            Token::Reserved(String::from(FROM)),
            Token::Identifier(String::from("table_name")),
            Token::Reserved(String::from("Unexpected")),
        ];
        let result = SelectQueryParser::parse(tokens);
        assert_error(result, "Unexpected token in query");
    }
    #[test]
    fn test_select_query_parser_unexpected_token_in_where_clause() {
        let tokens = vec![
            Token::ParenList(vec![Token::Identifier(String::from("id"))]),
            Token::Reserved(String::from(FROM)),
            Token::Identifier(String::from("table_name")),
            Token::Reserved(String::from(WHERE)),
            Token::Reserved(String::from("UNEXPECTED")),
        ];
        let result = SelectQueryParser::parse(tokens);
        assert_error(result, "Unexpected token in where_clause");
    }

    #[test]
    fn test_select_query_parser_missing_by_after_order() {
        let tokens = vec![
            Token::ParenList(vec![Token::Identifier(String::from("id"))]),
            Token::Reserved(String::from(FROM)),
            Token::Identifier(String::from("table_name")),
            Token::Reserved(String::from(ORDER)),
            Token::Reserved(String::from("UNEXPECTED")),
        ];
        let result = SelectQueryParser::parse(tokens);
        assert_error(result, "ORDER not followed by BY");
    }
    #[test]
    fn test_select_query_parser_unexpected_token_in_order_clause() {
        let tokens = vec![
            Token::ParenList(vec![Token::Identifier(String::from("id"))]),
            Token::Reserved(String::from(FROM)),
            Token::Identifier(String::from("table_name")),
            Token::Reserved(String::from(ORDER)),
            Token::Reserved(String::from(BY)),
            Token::Reserved(String::from("UNEXPECTED")),
        ];
        let result = SelectQueryParser::parse(tokens);
        assert_error(result, "Unexpected token in order_clause");
    }
}
