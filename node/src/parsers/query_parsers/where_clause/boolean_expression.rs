use std::collections::HashMap;

use crate::{parsers::tokens::token::Literal, utils::errors::Errors};
use BooleanExpression::*;

use super::{comparison::ComparisonExpr, evaluate::Evaluate};

/// Enum para representar diferentes tipos de expresiones booleanas.
// #[derive(Debug, PartialEq)]
pub enum BooleanExpression {
    Comparation(ComparisonExpr),
    Tuple(Vec<ComparisonExpr>),
    And(Box<BooleanExpression>, Box<BooleanExpression>),
    Or(Box<BooleanExpression>, Box<BooleanExpression>),
    Not(Box<BooleanExpression>)
}

impl Evaluate for BooleanExpression {
    /// Evalúa una expresión booleana usando los valores Columna -> Valor de una fila
    fn evaluate(&self, row: &HashMap<String, Literal>) -> Result<bool, Errors> {
        match self {
            Comparation(comparacion) => comparacion.evaluate(row),
            Tuple(comparaciones) => {
                for comparacion in comparaciones {
                    match comparacion.evaluate(row) {
                        Ok(true) => {}, 
                        Ok(false) => return Ok(false),
                        Err(err) => return Err(err)
                    }
                }
                Ok(true)
            }
            And(expr1, expr2) => {
                Ok(expr1.evaluate(row)? && expr2.evaluate(row)?)
            },
            Or(expr1, expr2) => {
                Ok(expr1.evaluate(row)? || expr2.evaluate(row)?)
            },
            Not(expr) => {
                Ok(!expr.evaluate(row)?)
            },
        }
    }
}