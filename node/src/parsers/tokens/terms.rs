use super::{
    literal::{to_literal, Literal},
    token::Token,
};

#[derive(Debug, PartialEq)]
pub enum Term {
    Literal(Literal),
    AritmeticasMath(AritmeticasMath),
    BooleanOperations(BooleanOperations),
}

#[derive(Debug, PartialEq)]
pub enum BooleanOperations {
    Logical(LogicalOperators),
    Comparison(ComparisonOperators),
}

#[derive(Debug, PartialEq)]
pub enum AritmeticasMath {
    Suma,
    Resta,
    Division,
    Resto,
    Multiplication,
}

#[derive(Debug, PartialEq)]
pub enum LogicalOperators {
    Or,
    And,
    Not,
}

#[derive(Debug, PartialEq)]
pub enum ComparisonOperators {
    Less,
    Equal,
    NotEqual,
    Greater,
    GreaterOrEqual,
    LessOrEqual,
}

fn to_math(word: &str) -> Option<Token> {
    match word {
        "+" => Some(Token::Term(Term::AritmeticasMath(AritmeticasMath::Suma))),
        "-" => Some(Token::Term(Term::AritmeticasMath(AritmeticasMath::Resta))),
        "/" => Some(Token::Term(Term::AritmeticasMath(
            AritmeticasMath::Division,
        ))),
        "%" => Some(Token::Term(Term::AritmeticasMath(AritmeticasMath::Resto))),
        "*" => Some(Token::Term(Term::AritmeticasMath(
            AritmeticasMath::Multiplication,
        ))),
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
            BooleanOperations::Comparison(ComparisonOperators::GreaterOrEqual),
        ))),
        "_LE_" => Some(Token::Term(Term::BooleanOperations(
            BooleanOperations::Comparison(ComparisonOperators::LessOrEqual),
        ))),
        _ => None,
    }
}

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
