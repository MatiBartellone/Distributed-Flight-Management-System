use std::collections::HashMap;

use crate::{parsers::tokens::token::Literal, utils::errors::Errors};

/// Trait para evaluar expresiones booleanas en función de una fila de Literales.
pub trait Evaluate {
    fn evaluate(&self, fila: &HashMap<String, Literal>) -> Result<bool, Errors>;
}
