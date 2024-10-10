use std::{iter::Peekable, vec::IntoIter};

use crate::parsers::tokens::token::AritmeticasMath;
use crate::parsers::tokens::token::DataType;
use crate::parsers::tokens::token::{
    create_literal, BooleanOperations, ComparisonOperators, Literal, LogicalOperators, Term, Token,
};
use BooleanOperations::*;
use Term::*;
use Token::*;

use super::errors::Errors;

pub fn get_literal(tokens: &mut Peekable<IntoIter<Token>>) -> Result<Literal, Errors> {
    let token = get_next_value(tokens)?;
    match token {
        Term(Term::Literal(literal)) => Ok(literal),
        _ => Err(Errors::Invalid("Expected a literal".to_string())),
    }
}

pub fn get_sublist(
    tokens: &mut Peekable<IntoIter<Token>>,
) -> Result<Vec<Token>, Errors> {
    let token = get_next_value(tokens)?;
    match token {
        TokensList(list) => Ok(list),
        e => Err(Errors::Invalid(format!(
            "Expected Sub List but has {:?}",
            e
        ))),
    }
}

pub fn get_arithmetic_math(
    tokens: &mut Peekable<IntoIter<Token>>,
) -> Result<AritmeticasMath, Errors> {
    let token = get_next_value(tokens)?;
    match token {
        Term(AritmeticasMath(op)) => Ok(op),
        e => Err(Errors::Invalid(format!(
            "Expected comparision operator but has {:?}",
            e
        ))),
    }
}

pub fn get_comparision_operator(
    tokens: &mut Peekable<IntoIter<Token>>,
) -> Result<ComparisonOperators, Errors> {
    let token = get_next_value(tokens)?;
    match token {
        Term(AritmeticasBool(Comparison(op))) => Ok(op),
        e => Err(Errors::Invalid(format!(
            "Expected comparision operator but has {:?}",
            e
        ))),
    }
}

pub fn get_logical_operator(
    tokens: &mut Peekable<IntoIter<Token>>,
) -> Result<LogicalOperators, Errors> {
    let token = get_next_value(tokens)?;
    match token {
        Term(AritmeticasBool(Logical(op))) => Ok(op),
        _ => Err(Errors::Invalid(
            "Expected logical operator (AND, OR, NOT)".to_string(),
        )),
    }
}

pub fn get_reserved_string(tokens: &mut Peekable<IntoIter<Token>>) -> Result<String, Errors> {
    let token = get_next_value(tokens)?;
    match token {
        Reserved(string) => Ok(string),
        _ => Err(Errors::Invalid("Expected Reserved".to_string())),
    }
}

pub fn get_list(tokens: &mut Peekable<IntoIter<Token>>) -> Result<Vec<Token>, Errors> {
    let token = get_next_value(tokens)?;
    match token {
        TokensList(token_list) => Ok(token_list),
        _ => Err(Errors::Invalid("Expected List".to_string())),
    }
}

pub fn get_identifier_string(tokens: &mut Peekable<IntoIter<Token>>) -> Result<String, Errors> {
    let token = get_next_value(tokens)?;
    match token {
        Identifier(string) => Ok(string),
        _ => Err(Errors::Invalid("Expected Identifier".to_string())),
    }
}

pub fn get_next_value(tokens: &mut Peekable<IntoIter<Token>>) -> Result<Token, Errors> {
    tokens
        .next()
        .ok_or(Errors::SyntaxError(String::from("Query lacks parameters")))
}

pub fn peek_next_value(peekable_tokens: &mut Peekable<IntoIter<Token>>) -> Result<&Token, Errors> {
    peekable_tokens
        .peek()
        .ok_or(Errors::SyntaxError(String::from("Query lacks parameters")))
}

pub fn iter_is_empty(tokens: &mut Peekable<IntoIter<Token>>) -> bool {
    let mut peekable = tokens.peekable();
    peekable.peek().is_none()
}

// String to enum Token

// Identifier
pub fn create_identifier_token(value: &str) -> Token {
    Token::Identifier(value.to_string())
}

// Token list

pub fn create_list_token(list: Vec<Token>) -> Token {
    Token::TokensList(list)
}

// Term
pub fn create_token_literal(value: &str, data_type: DataType) -> Token {
    Token::Term(Literal(create_literal(value, data_type)))
}

pub fn create_logical_operation_token(operation: LogicalOperators) -> Token {
    Token::Term(AritmeticasBool(BooleanOperations::Logical(operation)))
}

pub fn create_comparison_operation_token(operation: ComparisonOperators) -> Token {
    Token::Term(AritmeticasBool(BooleanOperations::Comparison(operation)))
}

// Reserved
pub fn create_reserved_token(value: &str) -> Token {
    Token::Reserved(value.to_string())
}

pub fn create_data_type_token(data_type: DataType) -> Token {
    Token::DataType(data_type)
}

pub fn create_aritmeticas_math_token(operation: AritmeticasMath) -> Token {
    Token::Term(AritmeticasMath(operation))
}
