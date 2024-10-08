use super::boolean_expression::BooleanExpression;
use crate::parsers::tokens::token::Literal;
use crate::{
    parsers::{
        query_parsers::where_clause::comparison::ComparisonExpr,
        tokens::token::{BooleanOperations, ComparisonOperators, LogicalOperators, Term, Token},
    },
    utils::errors::Errors,
};

use BooleanOperations::*;
use LogicalOperators::*;
use Term::*;
use Token::*;

use std::{iter::Peekable, vec::IntoIter};

pub struct WhereClauseParser;

impl WhereClauseParser {
    pub fn parse(tokens: Vec<Token>) -> Result<Option<BooleanExpression>, Errors> {
        Ok(Some(where_clause(&mut tokens.into_iter())?))
    }
}

fn where_and_or(
    tokens: &mut IntoIter<Token>,
    left_expr: BooleanExpression,
) -> Result<BooleanExpression, Errors> {
    match get_next_value(tokens) {
        Ok(Term(AritmeticasBool(Logical(And)))) => {
            let right_expr = where_clause(tokens)?;
            Ok(BooleanExpression::And(
                Box::new(left_expr),
                Box::new(right_expr),
            ))
        }
        Ok(Term(AritmeticasBool(Logical(Or)))) => {
            let right_expr = where_clause(tokens)?;
            Ok(BooleanExpression::Or(
                Box::new(left_expr),
                Box::new(right_expr),
            ))
        }
        Err(_) => Ok(left_expr),
        _ => Err(Errors::Invalid(
            "Invalid Syntaxis in WHERE_CLAUSE".to_string(),
        )),
    }
}

fn where_comparision(
    tokens: &mut IntoIter<Token>,
    column_name: String,
) -> Result<BooleanExpression, Errors> {
    let operator = get_comparision_operator(tokens)?;
    let literal = get_literal(tokens)?;
    let expression =
        BooleanExpression::Comparation(ComparisonExpr::new(column_name, operator, literal));
    where_and_or(tokens, expression)
}

fn where_comparisions(
    tokens: &mut IntoIter<Token>,
    column_names: Vec<Token>,
) -> Result<BooleanExpression, Errors> {
    let operator = get_comparision_operator(tokens)?;
    let literals = get_list(tokens)?;
    if column_names.len() != literals.len() {
        return Err(Errors::Invalid("Invalid tuples len".to_string()));
    }
    let mut column_iter = column_names.into_iter();
    let mut literal_iter = literals.into_iter();

    let mut tuple = Vec::new();
    for _ in 0..column_names.len() {
        let column_name = get_reserved_string(&mut column_iter)?;
        let literal = get_literal(&mut literal_iter)?;

        let expression = ComparisonExpr::new(column_name, operator, literal);

        tuple.push(expression);
    }
    Ok(BooleanExpression::Tuple(tuple))
}

fn where_list(tokens: &mut IntoIter<Token>, list: Vec<Token>) -> Result<BooleanExpression, Errors> {
    let mut peekable_tokens = tokens.peekable();
    match peekable_tokens.peek() {
        // [tupla, comparison, tupla, ...]
        Some(Term(AritmeticasBool(Comparison(_)))) => where_comparisions(tokens, list),
        // [lista]
        None => where_clause(&mut list.into_iter()),
        // [lista, ...]
        _ => {
            let left_expr = where_clause(&mut list.into_iter())?;
            where_and_or(tokens, left_expr)
        }
    }
}

fn where_clause(tokens: &mut IntoIter<Token>) -> Result<BooleanExpression, Errors> {
    match get_next_value(tokens)? {
        // [column_name, comparasion, literal, ...]
        Identifier(column_name) => where_comparision(tokens, column_name),
        // [tupla, comparasion, tupla, ...] or [lista, ...]
        TokensList(token_list) => where_list(tokens, token_list),
        // [NOT, ...]
        Term(AritmeticasBool(Logical(Not))) => {
            let expression = where_clause(tokens)?;
            Ok(BooleanExpression::Not(Box::new(expression)))
        }
        _ => Err(Errors::Invalid(
            "Invalid Syntaxis in WHERE_CLAUSE".to_string(),
        )),
    }
}

fn get_literal(tokens: &mut IntoIter<Token>) -> Result<Literal, Errors> {
    let token = get_next_value(tokens)?;
    match token {
        Term(Term::Literal(literal)) => Ok(literal),
        _ => Err(Errors::Invalid("Expected a literal".to_string())),
    }
}

fn get_comparision_operator(tokens: &mut IntoIter<Token>) -> Result<ComparisonOperators, Errors> {
    let token = get_next_value(tokens)?;
    match token {
        Term(AritmeticasBool(Comparison(op))) => Ok(op),
        _ => Err(Errors::Invalid("Expected comparision operator".to_string())),
    }
}

fn get_logical_operator(tokens: &mut IntoIter<Token>) -> Result<LogicalOperators, Errors> {
    let token = get_next_value(tokens)?;
    match token {
        Term(AritmeticasBool(Logical(op))) => Ok(op),
        _ => Err(Errors::Invalid(
            "Expected logical operator (AND, OR, NOT)".to_string(),
        )),
    }
}

fn get_reserved_string(tokens: &mut IntoIter<Token>) -> Result<String, Errors> {
    let token = get_next_value(tokens)?;
    match token {
        Reserved(string) => Ok(string),
        _ => Err(Errors::Invalid("Expected tuple".to_string())),
    }
}

fn get_list(tokens: &mut IntoIter<Token>) -> Result<Vec<Token>, Errors> {
    let token = get_next_value(tokens)?;
    match token {
        TokensList(token_list) => Ok(token_list),
        _ => Err(Errors::Invalid("Expected tuple".to_string())),
    }
}

fn get_next_value(tokens: &mut IntoIter<Token>) -> Result<Token, Errors> {
    tokens
        .next()
        .ok_or(Errors::SyntaxError(String::from("Query lacks parameters")))
}

fn peek_next_value(peekable_tokens: &mut Peekable<IntoIter<Token>>) -> Result<&Token, Errors> {
    peekable_tokens
        .peek()
        .ok_or(Errors::SyntaxError(String::from("Query lacks parameters")))
}

fn iter_is_empty(tokens: &mut IntoIter<Token>) -> bool {
    let mut peekable = tokens.peekable();
    peekable.peek().is_none()
}
