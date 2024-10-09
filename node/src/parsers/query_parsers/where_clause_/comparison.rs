use std::collections::HashMap;

use crate::{
    parsers::tokens::token::{ComparisonOperators, Literal},
    utils::errors::Errors,
};

use super::evaluate::Evaluate;
use ComparisonOperators::*;

#[derive(Debug, PartialEq)]
pub struct ComparisonExpr {
    column_name: String,
    operator: ComparisonOperators,
    literal: Literal,
}

impl ComparisonExpr {
    pub fn new(column_name: String, operator: &ComparisonOperators, literal: Literal) -> Self {
        let operator = match operator {
            Less => Less,
            Equal => Equal,
            NotEqual => NotEqual,
            Greater => Greater,
            GreaterOrEqual => GreaterOrEqual,
            LessOrEqual => LessOrEqual,
        };
        ComparisonExpr {
            column_name,
            operator,
            literal,
        }
    }
}

fn get_column_value<'a>(
    column_name: &String,
    row: &'a HashMap<String, Literal>,
) -> Result<&'a Literal, Errors> {
    match row.get(column_name) {
        Some(literal) => Ok(literal),
        None => Err(Errors::Invalid(format!("Column {} not found", column_name))),
    }
}

impl Evaluate for ComparisonExpr {
    fn evaluate(&self, row: &HashMap<String, Literal>) -> Result<bool, Errors> {
        let column_literal = get_column_value(&self.column_name, row)?;
        match self.operator {
            Equal => Ok(column_literal == &self.literal),
            NotEqual => Ok(column_literal != &self.literal),
            Less => Ok(column_literal < &self.literal),
            Greater => Ok(column_literal > &self.literal),
            LessOrEqual => Ok(column_literal <= &self.literal),
            GreaterOrEqual => Ok(column_literal >= &self.literal),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::parsers::{
        query_parsers::where_clause_::{comparison::ComparisonExpr, evaluate::Evaluate},
        tokens::token::{create_literal, ComparisonOperators, DataType, Literal},
    };
    use std::collections::HashMap;
    use ComparisonOperators::*;
    use DataType::*;

    fn create_row_integer() -> HashMap<String, Literal> {
        let mut row: HashMap<String, Literal> = HashMap::new();
        row.insert("age".to_string(), create_literal("30", DataType::Integer));
        row.insert(
            "score".to_string(),
            create_literal("100", DataType::Integer),
        );
        row
    }

    fn create_row_boolean() -> HashMap<String, Literal> {
        let mut row: HashMap<String, Literal> = HashMap::new();
        row.insert(
            "is_active".to_string(),
            create_literal("true", DataType::Boolean),
        );
        row.insert(
            "has_access".to_string(),
            create_literal("false", DataType::Boolean),
        );
        row
    }

    fn create_row_decimal() -> HashMap<String, Literal> {
        let mut row: HashMap<String, Literal> = HashMap::new();
        row.insert(
            "price".to_string(),
            create_literal("199.99", DataType::Decimal),
        );
        row.insert(
            "discount".to_string(),
            create_literal("10.50", DataType::Decimal),
        );
        row
    }

    fn create_row_text() -> HashMap<String, Literal> {
        let mut row: HashMap<String, Literal> = HashMap::new();
        row.insert("name".to_string(), create_literal("Alice", DataType::Text));
        row.insert(
            "city".to_string(),
            create_literal("New York", DataType::Text),
        );
        row
    }

    fn assert_multiple_expr_evaluations(
        exprs: Vec<ComparisonExpr>,
        row: &HashMap<String, Literal>,
        expected_results: Vec<bool>,
    ) {
        for i in 0..exprs.len() {
            let result = &exprs.get(i).unwrap().evaluate(row).unwrap();
            let expected = expected_results.get(i).unwrap();
            assert_eq!(
                result, expected,
                "Test mismatch for expression: {:?}",
                result
            );
        }
    }

    #[test]
    fn test_integer_comparison() {
        let row = create_row_integer();

        let exprs = vec![
            ComparisonExpr::new("age".to_string(), &Less, create_literal("35", Integer)),
            ComparisonExpr::new(
                "score".to_string(),
                &GreaterOrEqual,
                create_literal("95", Integer),
            ),
            ComparisonExpr::new("age".to_string(), &Greater, create_literal("35", Integer)),
            ComparisonExpr::new(
                "score".to_string(),
                &LessOrEqual,
                create_literal("50", Integer),
            ),
        ];

        let expected_results = vec![true, true, false, false];

        assert_multiple_expr_evaluations(exprs, &row, expected_results);
    }

    #[test]
    fn test_boolean_comparison() {
        let row = create_row_boolean();

        let exprs = vec![
            ComparisonExpr::new(
                "is_active".to_string(),
                &Equal,
                create_literal("true", Boolean),
            ),
            ComparisonExpr::new(
                "has_access".to_string(),
                &NotEqual,
                create_literal("true", Boolean),
            ),
            ComparisonExpr::new(
                "is_active".to_string(),
                &Equal,
                create_literal("false", Boolean),
            ),
            ComparisonExpr::new(
                "has_access".to_string(),
                &NotEqual,
                create_literal("false", Boolean),
            ),
        ];

        let expected_results = vec![true, true, false, false];

        assert_multiple_expr_evaluations(exprs, &row, expected_results);
    }

    #[test]
    fn test_decimal_comparison() {
        let row = create_row_decimal();

        let exprs = vec![
            ComparisonExpr::new(
                "price".to_string(),
                &Greater,
                create_literal("150.00", Decimal),
            ),
            ComparisonExpr::new(
                "discount".to_string(),
                &LessOrEqual,
                create_literal("15.00", Decimal),
            ),
            ComparisonExpr::new(
                "price".to_string(),
                &Less,
                create_literal("150.00", Decimal),
            ),
            ComparisonExpr::new(
                "discount".to_string(),
                &Greater,
                create_literal("15.00", Decimal),
            ),
        ];

        let expected_results = vec![true, true, false, false];

        assert_multiple_expr_evaluations(exprs, &row, expected_results);
    }

    #[test]
    fn test_text_comparison() {
        let row = create_row_text();

        let exprs = vec![
            ComparisonExpr::new("name".to_string(), &Equal, create_literal("Alice", Text)),
            ComparisonExpr::new(
                "city".to_string(),
                &NotEqual,
                create_literal("Los Angeles", Text),
            ),
            ComparisonExpr::new("name".to_string(), &NotEqual, create_literal("Alice", Text)),
            ComparisonExpr::new("city".to_string(), &Equal, create_literal("New York", Text)),
        ];

        let expected_results = vec![true, true, false, true];

        assert_multiple_expr_evaluations(exprs, &row, expected_results);
    }
}
