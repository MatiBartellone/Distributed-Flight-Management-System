use crate::parsers::tokens::token::Token;
use crate::queries::order_by_clause::OrderByClause;
use crate::utils::errors::Errors;
use std::vec::IntoIter;

const ASC: &str = "ASC";
const DESC: &str = "DESC";
pub struct OrderByClauseParser;

impl OrderByClauseParser {
    pub fn parse(tokens: Vec<Token>) -> Result<Vec<OrderByClause>, Errors> {
        let mut order_clauses = Vec::<OrderByClause>::new();
        column(&mut tokens.into_iter(), &mut order_clauses, false)?;
        Ok(order_clauses)
    }
}

fn column(
    tokens: &mut IntoIter<Token>,
    order_clauses: &mut Vec<OrderByClause>,
    modified: bool,
) -> Result<(), Errors> {
    let Some(token) = tokens.next() else {
        return Ok(());
    };
    match token {
        Token::Identifier(identifier) => {
            order_clauses.push(OrderByClause::new(identifier));
            column(tokens, order_clauses, false)
        }
        Token::Reserved(res) if res == *ASC => {
            if modified {
                return Err(Errors::SyntaxError(String::from(
                    "Cannot use two types of order together, a column is missing",
                )));
            };
            if order_clauses.is_empty() {
                return Err(Errors::SyntaxError(String::from(
                    "No column provided for ASC modifier",
                )));
            };
            column(tokens, order_clauses, true)
        }
        Token::Reserved(res) if res == *DESC => {
            if modified {
                return Err(Errors::SyntaxError(String::from(
                    "Cannot use two types of order together, a column is missing",
                )));
            }
            change_last_to_desc(order_clauses)?;
            column(tokens, order_clauses, true)
        }
        _ => Err(Errors::SyntaxError(String::from(
            "Unexpected token in order by clause",
        ))),
    }
}
fn change_last_to_desc(order_clauses: &mut Vec<OrderByClause>) -> Result<(), Errors> {
    let Some(order_clause) = order_clauses.pop() else {
        return Err(Errors::SyntaxError(String::from(
            "No column provided for DESC modifier",
        )));
    };
    order_clauses.push(OrderByClause::new_with_order(
        order_clause.column,
        DESC.to_string(),
    ));
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::parsers::tokens::token::Token;
    use crate::utils::errors::Errors;

    fn assert_column(n: usize, column: &str, order: &str, order_clauses: &[OrderByClause]) {
        assert_eq!(order_clauses[n].column, column);
        assert_eq!(order_clauses[n].order, order);
    }

    fn assert_one_non_error_column(
        result: Result<Vec<OrderByClause>, Errors>,
        column: &str,
        order: &str,
    ) {
        assert!(result.is_ok());

        let order_clauses = result.unwrap();
        assert_eq!(order_clauses.len(), 1);
        assert_column(0, column, order, &order_clauses);
    }

    fn assert_error(result: Result<Vec<OrderByClause>, Errors>, expected: &str) {
        assert!(result.is_err());

        let error = result.unwrap_err();
        if let Errors::SyntaxError(msg) = error {
            assert_eq!(msg, expected);
        }
    }

    #[test]
    fn test_parse_single_column_asc() {
        let tokens = vec![
            Token::Identifier(String::from("column")),
            Token::Reserved(String::from(ASC)),
        ];
        let result = OrderByClauseParser::parse(tokens);
        assert_one_non_error_column(result, "column", ASC);
    }

    #[test]
    fn test_parse_single_column_asc_default() {
        let tokens = vec![Token::Identifier(String::from("column"))];
        let result = OrderByClauseParser::parse(tokens);
        assert_one_non_error_column(result, "column", ASC);
    }

    #[test]
    fn test_parse_single_column_desc() {
        let tokens = vec![
            Token::Identifier(String::from("column")),
            Token::Reserved(String::from(DESC)),
        ];
        let result = OrderByClauseParser::parse(tokens);
        assert_one_non_error_column(result, "column", DESC);
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
        assert_column(0, "column1", ASC, &order_clauses);
        assert_column(1, "column2", ASC, &order_clauses);
        assert_column(2, "column3", DESC, &order_clauses);
    }

    #[test]
    fn test_parse_no_column() {
        let tokens = vec![Token::Reserved(String::from(ASC))];
        let result = OrderByClauseParser::parse(tokens);
        assert_error(result, "No column provided for ASC modifier");
    }

    #[test]
    fn test_parse_unexpected_token() {
        let tokens = vec![Token::Reserved(String::from("INVALID_TOKEN"))];
        let result = OrderByClauseParser::parse(tokens);
        assert_error(result, "Unexpected token in order by clause");
    }

    #[test]
    fn test_parse_two_modifiers_for_column() {
        let tokens = vec![
            Token::Identifier(String::from("column")),
            Token::Reserved(String::from(ASC)),
            Token::Reserved(String::from(DESC)),
        ];
        let result = OrderByClauseParser::parse(tokens);
        assert_error(
            result,
            "Cannot use two types of order together, a column is missing",
        );
    }
}
