use std::vec::IntoIter;
use crate::parsers::tokens::token::Token;
use crate::queries::order_by_clause::OrderByClause;
use crate::utils::errors::Errors;

const ASC : &str = "ASC";
const DESC  : &str = "DESC";
pub struct OrderByClauseParser;

impl OrderByClauseParser {
    pub fn parse(tokens: Vec<Token>) -> Result<Vec<OrderByClause>, Errors> {
        let mut order_clauses = Vec::<OrderByClause>::new();
        column(&mut tokens.into_iter(), &mut order_clauses, false)?;
        Ok(order_clauses)
    }
}

fn column(tokens: &mut IntoIter<Token>, order_clauses: &mut Vec<OrderByClause>, modified: bool) -> Result<(), Errors> {
    let Some(token) = tokens.next() else { return Ok(()) };
    match token {
        Token::Identifier(identifier)  => {
            order_clauses.push(OrderByClause::new(identifier));
            column(tokens, order_clauses, false)
        },
        Token::Reserved(res) if res == *ASC => {
            if modified { return Err(Errors::SyntaxError(String::from("Cannot use two types of order together, a column is missing")))};
            let Some(order_clause) = order_clauses.pop() else { return Err(Errors::SyntaxError(format!("No column provided for ASC modifier"))) };
            column(tokens, order_clauses, true)
        },
        Token::Reserved(res) if res == *DESC => {
            if modified { return Err(Errors::SyntaxError(String::from("Cannot use two types of order together, a column is missing")))}
            change_last_to_desc(order_clauses)?;
            column(tokens, order_clauses, true)
        },
        _ => Err(Errors::SyntaxError(String::from("Unexpected token in order by clause"))),
    }
}
fn change_last_to_desc(order_clauses: &mut Vec<OrderByClause>) -> Result<(), Errors> {
    let Some(order_clause) = order_clauses.pop() else { return Err(Errors::SyntaxError(format!("No column provided for DESC modifier"))) };
    order_clauses.push(OrderByClause::new_with_order(order_clause.column, DESC.to_string()));
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::parsers::tokens::token::Token;
    use crate::utils::errors::Errors;

    #[test]
    fn test_parse_single_column_asc() {
        let tokens = vec![
            Token::Identifier(String::from("column")),
            Token::Reserved(String::from(ASC)),
        ];
        let result = OrderByClauseParser::parse(tokens);
        assert!(result.is_ok());

        let order_clauses = result.unwrap();
        assert_eq!(order_clauses.len(), 1);
        assert_eq!(order_clauses[0].column, "column");
        assert_eq!(order_clauses[0].order, ASC);
    }

    #[test]
    fn test_parse_single_column_asc_default() {
        let tokens = vec![
            Token::Identifier(String::from("column")),
        ];
        let result = OrderByClauseParser::parse(tokens);
        assert!(result.is_ok());

        let order_clauses = result.unwrap();
        assert_eq!(order_clauses.len(), 1);
        assert_eq!(order_clauses[0].column, "column");
        assert_eq!(order_clauses[0].order, ASC);
    }

    #[test]
    fn test_parse_single_column_desc() {
        let tokens = vec![
            Token::Identifier(String::from("column")),
            Token::Reserved(String::from(DESC)),
        ];
        let result = OrderByClauseParser::parse(tokens);
        assert!(result.is_ok());

        let order_clauses = result.unwrap();
        assert_eq!(order_clauses.len(), 1);
        assert_eq!(order_clauses[0].column, "column");
        assert_eq!(order_clauses[0].order, DESC);
    }

    #[test]
    fn test_parse_multiple_columns() {
        let tokens = vec![
            Token::Identifier(String::from("column1")),
            Token::Reserved(String::from(ASC)),
            Token::Identifier(String::from("column2")),
            Token::Identifier(String::from("column3")),
            Token::Reserved(String::from(DESC)),
        ];
        let result = OrderByClauseParser::parse(tokens);
        assert!(result.is_ok());

        let order_clauses = result.unwrap();
        assert_eq!(order_clauses.len(), 3);
        assert_eq!(order_clauses[0].column, "column1");
        assert_eq!(order_clauses[0].order, ASC);
        assert_eq!(order_clauses[1].column, "column2");
        assert_eq!(order_clauses[1].order, ASC);
        assert_eq!(order_clauses[2].column, "column3");
        assert_eq!(order_clauses[2].order, DESC);
    }

    #[test]
    fn test_parse_no_column() {
        let tokens = vec![
            Token::Reserved(String::from(ASC)),
        ];
        let result = OrderByClauseParser::parse(tokens);
        assert!(result.is_err());

        let error = result.unwrap_err();
        if let Errors::SyntaxError(msg) = error {
            assert_eq!(msg, "No column provided for ASC modifier");
        }
    }

    #[test]
    fn test_parse_unexpected_token() {
        let tokens = vec![
            Token::Reserved(String::from("INVALID_TOKEN")),
        ];
        let result = OrderByClauseParser::parse(tokens);
        assert!(result.is_err());

        let error = result.unwrap_err();
        if let Errors::SyntaxError(msg) = error {
            assert_eq!(msg, "Unexpected token in order by clause");
        }
    }

    #[test]
    fn test_parse_two_modifiers_for_column() {
        let tokens = vec![
            Token::Identifier(String::from("column")),
            Token::Reserved(String::from(ASC)),
            Token::Reserved(String::from(DESC)),
        ];
        let result = OrderByClauseParser::parse(tokens);
        assert!(result.is_err());

        let error = result.unwrap_err();
        if let Errors::SyntaxError(msg) = error {
            assert_eq!(msg, "Cannot use two types of order together, a column is missing");
        }
    }
}
