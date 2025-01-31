//! # Term Module
//!
//! Este módulo define las estructuras, enumeraciones y funciones públicas necesarias para manejar
//! términos relacionados con operaciones aritméticas, lógicas y literales.

use super::{
    literal::{to_literal, Literal},
    token::Token,
};
use serde::{Deserialize, Serialize};

/// Representa un término que puede ser aritmético, lógico o literal.
#[derive(Debug, PartialEq)]
pub enum Term {
    /// Un valor literal, como un número o una cadena.
    Literal(Literal),
    /// Una operación aritmética.
    ArithMath(ArithMath),
    /// Una operación booleana.
    BooleanOperations(BooleanOperations),
}

/// Representa una operación booleana: lógica o de comparación.
#[derive(Debug, PartialEq)]
pub enum BooleanOperations {
    /// Operadores lógicos como AND, OR y NOT.
    Logical(LogicalOperators),
    /// Operadores de comparación como `<`, `>`, `=` y similares.
    Comparison(ComparisonOperators),
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub enum ArithMath {
    Suma,
    Sub,
    Division,
    Rest,
    Multiplication,
}

#[derive(Debug, PartialEq)]
pub enum LogicalOperators {
    Or,
    And,
    Not,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum ComparisonOperators {
    Less,
    Equal,
    NotEqual,
    Greater,
    GreaterEqual,
    LesserEqual,
}

fn to_math(word: &str) -> Option<Token> {
    match word {
        "+" => Some(Token::Term(Term::ArithMath(ArithMath::Suma))),
        "-" => Some(Token::Term(Term::ArithMath(ArithMath::Sub))),
        "/" => Some(Token::Term(Term::ArithMath(ArithMath::Division))),
        "%" => Some(Token::Term(Term::ArithMath(ArithMath::Rest))),
        "*" => Some(Token::Term(Term::ArithMath(ArithMath::Multiplication))),
        _ => None,
    }
}

fn to_boolean(word: &str) -> Option<Token> {
    match word.to_ascii_uppercase().as_str() {
        "OR" => Some(Token::Term(Term::BooleanOperations(
            BooleanOperations::Logical(LogicalOperators::Or),
        ))),
        "AND" => Some(Token::Term(Term::BooleanOperations(
            BooleanOperations::Logical(LogicalOperators::And),
        ))),
        "NOT" => Some(Token::Term(Term::BooleanOperations(
            BooleanOperations::Logical(LogicalOperators::Not),
        ))),
        "<" => Some(Token::Term(Term::BooleanOperations(
            BooleanOperations::Comparison(ComparisonOperators::Less),
        ))),
        "=" => Some(Token::Term(Term::BooleanOperations(
            BooleanOperations::Comparison(ComparisonOperators::Equal),
        ))),
        "_DF_" => Some(Token::Term(Term::BooleanOperations(
            BooleanOperations::Comparison(ComparisonOperators::NotEqual),
        ))),
        ">" => Some(Token::Term(Term::BooleanOperations(
            BooleanOperations::Comparison(ComparisonOperators::Greater),
        ))),
        "_GE_" => Some(Token::Term(Term::BooleanOperations(
            BooleanOperations::Comparison(ComparisonOperators::GreaterEqual),
        ))),
        "_LE_" => Some(Token::Term(Term::BooleanOperations(
            BooleanOperations::Comparison(ComparisonOperators::LesserEqual),
        ))),
        _ => None,
    }
}

/// Convierte una cadena en un término (token aritmético, booleano o literal).
///
/// # Parámetros
/// - `word`: Cadena de texto que representa el término.
///
/// # Retorno
/// - `Some(Token)` si la cadena coincide con un término reconocido.
/// - `None` si no se reconoce.
///
/// # Ejemplo
/// ```ignore
/// use crate::string_to_term;
/// let token = string_to_term("+").unwrap();
/// assert_eq!(format!("{:?}", token), "Term::ArithMath(ArithMath::Suma)");
pub fn string_to_term(word: &str) -> Option<Token> {
    if let Some(token) = to_math(word) {
        return Some(token);
    }
    if let Some(token) = to_boolean(word) {
        return Some(token);
    }
    if let Some(token) = to_literal(word) {
        return Some(token);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_math() {
        assert_eq!(
            to_math("+"),
            Some(Token::Term(Term::ArithMath(ArithMath::Suma)))
        );
        assert_eq!(
            to_math("-"),
            Some(Token::Term(Term::ArithMath(ArithMath::Sub)))
        );
        assert_eq!(
            to_math("/"),
            Some(Token::Term(Term::ArithMath(ArithMath::Division)))
        );
        assert_eq!(
            to_math("%"),
            Some(Token::Term(Term::ArithMath(ArithMath::Rest)))
        );
        assert_eq!(
            to_math("*"),
            Some(Token::Term(Term::ArithMath(ArithMath::Multiplication)))
        );
        assert_eq!(to_math("^"), None); // Caso no soportado
    }

    #[test]
    fn test_to_boolean_logical() {
        assert_eq!(
            to_boolean("OR"),
            Some(Token::Term(Term::BooleanOperations(
                BooleanOperations::Logical(LogicalOperators::Or)
            )))
        );
        assert_eq!(
            to_boolean("AND"),
            Some(Token::Term(Term::BooleanOperations(
                BooleanOperations::Logical(LogicalOperators::And)
            )))
        );
        assert_eq!(
            to_boolean("NOT"),
            Some(Token::Term(Term::BooleanOperations(
                BooleanOperations::Logical(LogicalOperators::Not)
            )))
        );
    }

    #[test]
    fn test_to_boolean_comparison() {
        assert_eq!(
            to_boolean("<"),
            Some(Token::Term(Term::BooleanOperations(
                BooleanOperations::Comparison(ComparisonOperators::Less)
            )))
        );
        assert_eq!(
            to_boolean("="),
            Some(Token::Term(Term::BooleanOperations(
                BooleanOperations::Comparison(ComparisonOperators::Equal)
            )))
        );
        assert_eq!(
            to_boolean("_DF_"),
            Some(Token::Term(Term::BooleanOperations(
                BooleanOperations::Comparison(ComparisonOperators::NotEqual)
            )))
        );
        assert_eq!(
            to_boolean(">"),
            Some(Token::Term(Term::BooleanOperations(
                BooleanOperations::Comparison(ComparisonOperators::Greater)
            )))
        );
        assert_eq!(
            to_boolean("_GE_"),
            Some(Token::Term(Term::BooleanOperations(
                BooleanOperations::Comparison(ComparisonOperators::GreaterEqual)
            )))
        );
        assert_eq!(
            to_boolean("_LE_"),
            Some(Token::Term(Term::BooleanOperations(
                BooleanOperations::Comparison(ComparisonOperators::LesserEqual)
            )))
        );
        assert_eq!(to_boolean("UNKNOWN"), None); // Caso no soportado
    }

    #[test]
    fn test_string_to_term_math() {
        assert_eq!(
            string_to_term("+"),
            Some(Token::Term(Term::ArithMath(ArithMath::Suma)))
        );
        assert_eq!(
            string_to_term("/"),
            Some(Token::Term(Term::ArithMath(ArithMath::Division)))
        );
    }

    #[test]
    fn test_string_to_term_boolean() {
        assert_eq!(
            string_to_term("OR"),
            Some(Token::Term(Term::BooleanOperations(
                BooleanOperations::Logical(LogicalOperators::Or)
            )))
        );
        assert_eq!(
            string_to_term("_LE_"),
            Some(Token::Term(Term::BooleanOperations(
                BooleanOperations::Comparison(ComparisonOperators::LesserEqual)
            )))
        );
    }

    #[test]
    fn test_string_to_term_literal() {
        assert_eq!(string_to_term("unknown_literal"), None);
    }

    #[test]
    fn test_string_to_term_unsupported() {
        assert_eq!(string_to_term("UNKNOWN"), None); // Caso no soportado
    }
}
