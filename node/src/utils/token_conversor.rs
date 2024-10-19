use std::{iter::Peekable, vec::IntoIter};
use crate::parsers::tokens::{
    data_type::DataType,
    literal::Literal,
    terms::{ArithMath, BooleanOperations, ComparisonOperators, LogicalOperators, Term},
    token::Token,
};
use Term::*;
use BooleanOperations::*;
use LogicalOperators::*;
use crate::parsers::tokens::token::Token::{IterateToken, ParenList};
use super::errors::Errors;

pub fn precedence(tokens: Vec<Token>) -> Vec<Token> {
    let mut result = Vec::new();
    let mut current_list = Vec::new();

    for token in tokens.into_iter() {
        match token {
            Token::Term(BooleanOperations(Logical(Or))) => {
                result.push(ParenList(current_list));
                current_list = Vec::new();
                result.push(Token::Term(BooleanOperations(Logical(Or))));
            }
            ParenList(list) => {
                let list = precedence(list);
                current_list.push(ParenList(list));
            }
            _ => {
                current_list.push(token);
            }
        }
    }
    if result.is_empty(){
        return current_list
    }
    result.push(ParenList(current_list));
    result
}

pub fn get_literal(tokens: &mut Peekable<IntoIter<Token>>) -> Result<Literal, Errors> {
    let token = get_next_value(tokens)?;
    match token {
        Token::Term(Term::Literal(literal)) => Ok(literal),
        _ => Err(Errors::Invalid("Expected a literal".to_string())),
    }
}

pub fn get_sublist(tokens: &mut Peekable<IntoIter<Token>>) -> Result<Vec<Token>, Errors> {
    let token = get_next_value(tokens)?;
    match token {
        IterateToken(list) => Ok(list),
        e => Err(Errors::Invalid(format!(
            "Expected Sub List but has {:?}",
            e
        ))),
    }
}

pub fn get_arithmetic_math(tokens: &mut Peekable<IntoIter<Token>>) -> Result<ArithMath, Errors> {
    let token = get_next_value(tokens)?;
    match token {
        Token::Term(ArithMath(op)) => Ok(op),
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
        Token::Term(Term::BooleanOperations(BooleanOperations::Comparison(op))) => Ok(op),
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
        Token::Term(Term::BooleanOperations(BooleanOperations::Logical(op))) => Ok(op),
        _ => Err(Errors::Invalid(
            "Expected logical operator (AND, OR, NOT)".to_string(),
        )),
    }
}

pub fn get_reserved_string(tokens: &mut Peekable<IntoIter<Token>>) -> Result<String, Errors> {
    let token = get_next_value(tokens)?;
    match token {
        Token::Reserved(string) => Ok(string),
        _ => Err(Errors::Invalid("Expected Reserved".to_string())),
    }
}

pub fn get_list(tokens: &mut Peekable<IntoIter<Token>>) -> Result<Vec<Token>, Errors> {
    let token = get_next_value(tokens)?;
    match token {
        Token::ParenList(token_list) => Ok(token_list),
        _ => Err(Errors::Invalid("Expected List".to_string())),
    }
}

pub fn get_identifier_string(tokens: &mut Peekable<IntoIter<Token>>) -> Result<String, Errors> {
    let token = get_next_value(tokens)?;
    match token {
        Token::Identifier(string) => Ok(string),
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

// Lists

pub fn create_iterate_list_token(list: Vec<Token>) -> Token {
    Token::IterateToken(list)
}

pub fn create_paren_list_token(list: Vec<Token>) -> Token {
    Token::ParenList(list)
}

pub fn create_brace_list_token(list: Vec<Token>) -> Token {
    Token::BraceList(list)
}

// Term
pub fn create_token_literal(value: &str, data_type: DataType) -> Token {
    Token::Term(Term::Literal(Literal::new(value.to_string(), data_type)))
}

pub fn create_logical_operation_token(operation: LogicalOperators) -> Token {
    Token::Term(Term::BooleanOperations(BooleanOperations::Logical(
        operation,
    )))
}

pub fn create_comparison_operation_token(operation: ComparisonOperators) -> Token {
    Token::Term(Term::BooleanOperations(BooleanOperations::Comparison(
        operation,
    )))
}

// Reserved
pub fn create_reserved_token(value: &str) -> Token {
    Token::Reserved(value.to_string())
}

pub fn create_data_type_token(data_type: DataType) -> Token {
    Token::DataType(data_type)
}

pub fn create_aritmeticas_math_token(operation: ArithMath) -> Token {
    Token::Term(Term::ArithMath(operation))
}

// Symbol

pub fn create_symbol_token(symbol: &str) -> Token {
    Token::Symbol(symbol.to_string())
}
