use std::collections::HashMap;

use crate::{utils::errors::Errors, parsers::tokens::literal::Literal};

/// Trait para evaluar expresiones booleanas en función de una fila de Literales.
pub trait Evaluate {
    fn evaluate(&self, fila: &HashMap<String, Literal>) -> Result<bool, Errors>;
}
