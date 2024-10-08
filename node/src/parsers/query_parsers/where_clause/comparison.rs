use std::collections::HashMap;

use crate::{parsers::tokens::token::{compare_literals, ComparisonOperators, Literal}, utils::errors::Errors};

use ComparisonOperators::*;
use super::evaluate::Evaluate;

pub struct ComparisonExpr  {
    column_name: String,
    operator: ComparisonOperators,
    literal: Literal,
}

impl ComparisonExpr {
    pub fn new(column_name: String, operator: ComparisonOperators,  literal: Literal) -> Self {
    ComparisonExpr  {
           column_name,
           operator,
           literal
       }
   }
}


fn get_column_value<'a>(column_name: &String, row: &'a HashMap<String, Literal>) -> Result<&'a Literal, Errors> {
    match row.get(column_name) {
        Some(literal) => Ok(literal),
        None => Err(Errors::Invalid(format!("Column {} not found", column_name))),
    }
}

impl Evaluate for ComparisonExpr  {
    /// Evalúa una comapracion usando los valores Columna -> Valor de una fila
    fn evaluate(&self, row: &HashMap<String, Literal>) -> Result<bool, Errors> {
        let column_literal = get_column_value(&self.column_name, row)?;
        match self.operator {
            Equal => Ok(column_literal.valor == self.literal.valor),
            NotEqual => Ok(column_literal.valor != self.literal.valor),
            Less => compare_literals(&column_literal, &self.literal, |a, b| a < b),
            Greater => compare_literals(&column_literal, &self.literal, |a, b| a > b),
            LessOrEqual => compare_literals(&column_literal, &self.literal, |a, b| a <= b),
            GreaterOrEqual => compare_literals(&column_literal, &self.literal, |a, b| a >= b),
        }
    }
}