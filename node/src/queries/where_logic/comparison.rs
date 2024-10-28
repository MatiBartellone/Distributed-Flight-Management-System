use crate::{
    parsers::tokens::{literal::Literal, terms::ComparisonOperators},
    queries::evaluate::Evaluate,
    utils::errors::Errors,
};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

use ComparisonOperators::*;

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
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
            GreaterEqual => GreaterEqual,
            LesserEqual => LesserEqual,
        };
        ComparisonExpr {
            column_name,
            operator,
            literal,
        }
    }

    pub fn get_primary_key(
        &self,
        pk: &mut Vec<String>,
        table_pk: &HashSet<String>,
    ) -> Result<(), Errors> {
        if !table_pk.contains(&self.column_name) {
            return Ok(());
        }
        match self.operator {
            Equal => {
                pk.push(self.literal.value.to_string());
                Ok(())
            }
            _ => Err(Errors::SyntaxError(String::from(
                "Partition keys in where_clause only allowed with '=' operator",
            ))),
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
            LesserEqual => Ok(column_literal <= &self.literal),
            GreaterEqual => Ok(column_literal >= &self.literal),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::parsers::tokens::data_type::DataType;
    use crate::parsers::tokens::literal::Literal;
    use crate::parsers::tokens::terms::ComparisonOperators;
    use crate::queries::evaluate::Evaluate;
    use std::collections::HashMap;
    use ComparisonOperators::*;
    use DataType::*;

    use super::ComparisonExpr;

    fn create_row_int() -> HashMap<String, Literal> {
        let mut row: HashMap<String, Literal> = HashMap::new();
        row.insert(
            "age".to_string(),
            Literal::new("30".to_string(), DataType::Int),
        );
        row.insert(
            "score".to_string(),
            Literal::new("100".to_string(), DataType::Int),
        );
        row
    }

    fn create_row_boolean() -> HashMap<String, Literal> {
        let mut row: HashMap<String, Literal> = HashMap::new();
        row.insert(
            "is_active".to_string(),
            Literal::new("true".to_string(), DataType::Boolean),
        );
        row.insert(
            "has_access".to_string(),
            Literal::new("false".to_string(), DataType::Boolean),
        );
        row
    }

    fn create_row_decimal() -> HashMap<String, Literal> {
        let mut row: HashMap<String, Literal> = HashMap::new();
        row.insert(
            "price".to_string(),
            Literal::new("199.99".to_string(), DataType::Decimal),
        );
        row.insert(
            "discount".to_string(),
            Literal::new("10.50".to_string(), DataType::Decimal),
        );
        row
    }

    fn create_row_text() -> HashMap<String, Literal> {
        let mut row: HashMap<String, Literal> = HashMap::new();
        row.insert(
            "name".to_string(),
            Literal::new("Alice".to_string(), DataType::Text),
        );
        row.insert(
            "city".to_string(),
            Literal::new("New York".to_string(), DataType::Text),
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
    fn test_int_comparison() {
        let row = create_row_int();

        let exprs = vec![
            ComparisonExpr::new(
                "age".to_string(),
                &Less,
                Literal::new("35".to_string(), Int),
            ),
            ComparisonExpr::new(
                "score".to_string(),
                &GreaterEqual,
                Literal::new("95".to_string(), Int),
            ),
            ComparisonExpr::new(
                "age".to_string(),
                &Greater,
                Literal::new("35".to_string(), Int),
            ),
            ComparisonExpr::new(
                "score".to_string(),
                &LesserEqual,
                Literal::new("50".to_string(), Int),
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
                Literal::new("true".to_string(), Boolean),
            ),
            ComparisonExpr::new(
                "has_access".to_string(),
                &NotEqual,
                Literal::new("true".to_string(), Boolean),
            ),
            ComparisonExpr::new(
                "is_active".to_string(),
                &Equal,
                Literal::new("false".to_string(), Boolean),
            ),
            ComparisonExpr::new(
                "has_access".to_string(),
                &NotEqual,
                Literal::new("false".to_string(), Boolean),
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
                Literal::new("150.00".to_string(), Decimal),
            ),
            ComparisonExpr::new(
                "discount".to_string(),
                &LesserEqual,
                Literal::new("15.00".to_string(), Decimal),
            ),
            ComparisonExpr::new(
                "price".to_string(),
                &Less,
                Literal::new("150.00".to_string(), Decimal),
            ),
            ComparisonExpr::new(
                "discount".to_string(),
                &Greater,
                Literal::new("15.00".to_string(), Decimal),
            ),
        ];

        let expected_results = vec![true, true, false, false];

        assert_multiple_expr_evaluations(exprs, &row, expected_results);
    }

    #[test]
    fn test_text_comparison() {
        let row = create_row_text();

        let exprs = vec![
            ComparisonExpr::new(
                "name".to_string(),
                &Equal,
                Literal::new("Alice".to_string(), Text),
            ),
            ComparisonExpr::new(
                "city".to_string(),
                &NotEqual,
                Literal::new("Los Angeles".to_string(), Text),
            ),
            ComparisonExpr::new(
                "name".to_string(),
                &NotEqual,
                Literal::new("Alice".to_string(), Text),
            ),
            ComparisonExpr::new(
                "city".to_string(),
                &Equal,
                Literal::new("New York".to_string(), Text),
            ),
        ];

        let expected_results = vec![true, true, false, true];

        assert_multiple_expr_evaluations(exprs, &row, expected_results);
    }
}
