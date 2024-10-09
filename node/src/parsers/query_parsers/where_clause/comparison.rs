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
    literal: Literal
}

impl ComparisonExpr {
    pub fn new(column_name: String, operator: &ComparisonOperators, literal: Literal) -> Self {
        let operator = match operator {
            Less => Less,
            Equal => Equal,
            NotEqual => NotEqual,
            Greater => Greater,
            GreaterOrEqual => GreaterOrEqual,
            LessOrEqual => LessOrEqual
        };
        ComparisonExpr {
            column_name,
            operator,
            literal
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
    use std::collections::HashMap;

    use crate::parsers::{
        query_parsers::where_clause::{comparison::ComparisonExpr, evaluate::Evaluate},
        tokens::token::{ComparisonOperators, DataType, Literal},
    };

    #[test]
    fn test_integer_comparison() {
        let mut row: HashMap<String, Literal> = HashMap::new();
        row.insert(
            "age".to_string(),
            Literal {
                valor: "30".to_string(),
                tipo: DataType::Integer,
            },
        );
        row.insert(
            "score".to_string(),
            Literal {
                valor: "100".to_string(),
                tipo: DataType::Integer,
            },
        );

        let expr = ComparisonExpr::new(
            "age".to_string(),
            &ComparisonOperators::Less,
            Literal {
                valor: "35".to_string(),
                tipo: DataType::Integer,
            },
        );
        assert_eq!(expr.evaluate(&row).unwrap(), true);

        let expr = ComparisonExpr::new(
            "score".to_string(),
            &ComparisonOperators::GreaterOrEqual,
            Literal {
                valor: "95".to_string(),
                tipo: DataType::Integer,
            },
        );
        assert_eq!(expr.evaluate(&row).unwrap(), true);

        let expr = ComparisonExpr::new(
            "age".to_string(),
            &ComparisonOperators::Greater,
            Literal {
                valor: "35".to_string(),
                tipo: DataType::Integer,
            },
        );
        assert_eq!(expr.evaluate(&row).unwrap(), false);

        let expr = ComparisonExpr::new(
            "score".to_string(),
            &ComparisonOperators::LessOrEqual,
            Literal {
                valor: "50".to_string(),
                tipo: DataType::Integer,
            },
        );
        assert_eq!(expr.evaluate(&row).unwrap(), false);
    }

    #[test]
    fn test_boolean_comparison() {
        let mut row: HashMap<String, Literal> = HashMap::new();
        row.insert(
            "is_active".to_string(),
            Literal {
                valor: "true".to_string(),
                tipo: DataType::Boolean,
            },
        );
        row.insert(
            "has_access".to_string(),
            Literal {
                valor: "false".to_string(),
                tipo: DataType::Boolean,
            },
        );

        let expr = ComparisonExpr::new(
            "is_active".to_string(),
            &ComparisonOperators::Equal,
            Literal {
                valor: "true".to_string(),
                tipo: DataType::Boolean,
            },
        );
        assert_eq!(expr.evaluate(&row).unwrap(), true);

        let expr = ComparisonExpr::new(
            "has_access".to_string(),
            &ComparisonOperators::NotEqual,
            Literal {
                valor: "true".to_string(),
                tipo: DataType::Boolean,
            },
        );
        assert_eq!(expr.evaluate(&row).unwrap(), true);

        let expr = ComparisonExpr::new(
            "is_active".to_string(),
            &ComparisonOperators::Equal,
            Literal {
                valor: "false".to_string(),
                tipo: DataType::Boolean,
            },
        );
        assert_eq!(expr.evaluate(&row).unwrap(), false);

        let expr = ComparisonExpr::new(
            "has_access".to_string(),
            &ComparisonOperators::NotEqual,
            Literal {
                valor: "false".to_string(),
                tipo: DataType::Boolean,
            },
        );
        assert_eq!(expr.evaluate(&row).unwrap(), false);
    }

    #[test]
    fn test_decimal_comparison() {
        let mut row: HashMap<String, Literal> = HashMap::new();
        row.insert(
            "price".to_string(),
            Literal {
                valor: "199.99".to_string(),
                tipo: DataType::Decimal,
            },
        );
        row.insert(
            "discount".to_string(),
            Literal {
                valor: "10.50".to_string(),
                tipo: DataType::Decimal,
            },
        );

        let expr = ComparisonExpr::new(
            "price".to_string(),
            &ComparisonOperators::Greater,
            Literal {
                valor: "150.00".to_string(),
                tipo: DataType::Decimal,
            },
        );
        assert_eq!(expr.evaluate(&row).unwrap(), true);

        let expr = ComparisonExpr::new(
            "discount".to_string(),
            &ComparisonOperators::LessOrEqual,
            Literal {
                valor: "15.00".to_string(),
                tipo: DataType::Decimal,
            },
        );
        assert_eq!(expr.evaluate(&row).unwrap(), true);

        let expr = ComparisonExpr::new(
            "price".to_string(),
            &ComparisonOperators::Less,
            Literal {
                valor: "150.00".to_string(),
                tipo: DataType::Decimal,
            },
        );
        assert_eq!(expr.evaluate(&row).unwrap(), false);

        let expr = ComparisonExpr::new(
            "discount".to_string(),
            &ComparisonOperators::Greater,
            Literal {
                valor: "15.00".to_string(),
                tipo: DataType::Decimal,
            },
        );
        assert_eq!(expr.evaluate(&row).unwrap(), false);
    }

    #[test]
    fn test_text_comparison() {
        let mut row: HashMap<String, Literal> = HashMap::new();
        row.insert(
            "name".to_string(),
            Literal {
                valor: "Alice".to_string(),
                tipo: DataType::Text,
            },
        );
        row.insert(
            "city".to_string(),
            Literal {
                valor: "New York".to_string(),
                tipo: DataType::Text,
            },
        );

        let expr = ComparisonExpr::new(
            "name".to_string(),
            &ComparisonOperators::Equal,
            Literal {
                valor: "Alice".to_string(),
                tipo: DataType::Text,
            },
        );
        assert_eq!(expr.evaluate(&row).unwrap(), true);

        let expr = ComparisonExpr::new(
            "city".to_string(),
            &ComparisonOperators::NotEqual,
            Literal {
                valor: "Los Angeles".to_string(),
                tipo: DataType::Text,
            },
        );
        assert_eq!(expr.evaluate(&row).unwrap(), true);

        let expr = ComparisonExpr::new(
            "name".to_string(),
            &ComparisonOperators::NotEqual,
            Literal {
                valor: "Alice".to_string(),
                tipo: DataType::Text,
            },
        );
        assert_eq!(expr.evaluate(&row).unwrap(), false);

        let expr = ComparisonExpr::new(
            "city".to_string(),
            &ComparisonOperators::Equal,
            Literal {
                valor: "New York ".to_string(),
                tipo: DataType::Text,
            },
        );
        assert_eq!(expr.evaluate(&row).unwrap(), false);
    }
}
