use crate::{parsers::tokens::token::{ComparisonOperators, Term, Token}, queries::update_query::{AssigmentValue, UpdateQuery}, utils::{errors::Errors, token_conversor::{get_arithmetic_math, get_comparision_operator, get_next_value}}};
use Token::*;
use ComparisonOperators::*;
use Term::*;
use std::{iter::Peekable, vec::IntoIter};

use super::where_clause_::where_clause_parser::WhereClauseParser;

const SET: &str = "SET";
const WHERE: &str = "WHERE";

pub struct UpdateQueryParser;

impl UpdateQueryParser {
    pub fn parse(tokens: Vec<Token>) -> Result<UpdateQuery, Errors> {
        let mut delete_query = UpdateQuery::new();
        table(&mut tokens.into_iter().peekable(), &mut delete_query)?;
        Ok(delete_query)
    }
}

fn table(tokens: &mut Peekable<IntoIter<Token>>, query: &mut UpdateQuery) -> Result<(), Errors> {
    match get_next_value(tokens)? {
        Identifier(table)=> {
            query.table = table;
            set(tokens, query)
        }
        _ => Err(Errors::SyntaxError(String::from(
            "UPDATE not followed by a table name",
        ))),
    }
}

fn set(tokens: &mut Peekable<IntoIter<Token>>, query: &mut UpdateQuery) -> Result<(), Errors> {
    match get_next_value(tokens)? {
        Identifier(res) if res == *SET => {
            values(tokens, query)
        }
        _ => Err(Errors::SyntaxError("SET not found".to_string())),
    }
}

fn values(tokens: &mut Peekable<IntoIter<Token>>, query: &mut UpdateQuery) -> Result<(), Errors> {
    match get_next_value(tokens)?{
        Identifier(column_name) => assigment(tokens, query, column_name),
        _ => {
            if query.changes.is_empty(){
                return Err(Errors::SyntaxError("Invalid Sintax follow SET".to_string()))
            }
            where_clause(tokens, query)
        }
    }
}

fn assigment(tokens: &mut Peekable<IntoIter<Token>>, query: &mut UpdateQuery, column_name: String) -> Result<(), Errors> {
    let Ok(Equal) = get_comparision_operator(tokens) else {
        return Err(Errors::SyntaxError("= should follow a SET assigment".to_string()))
    };
    match get_next_value(tokens)? {
        // [column_name, = , literal]
        Term(Literal(value)) => {
            query.changes.insert(column_name, AssigmentValue::Simple(value));
            values(tokens, query)
        }
        // [column_name, = , other_column, ...]
        Identifier(other_column) => column_asssigment(tokens, query, column_name, other_column),
        _ => Err(Errors::SyntaxError("Invalid assigment".to_string())),
    }
}

fn column_asssigment(tokens: &mut Peekable<IntoIter<Token>>, query: &mut UpdateQuery, column_name: String, other_column: String) -> Result<(), Errors> {
    let token = tokens.peek();
    match token {
        // [column_name, = , other_column, +|-, literal]
        Some(Term(AritmeticasMath(_))) =>
            arithmetic_assigment(tokens, query, column_name, other_column),
        // [column_name, = , other_column]
        _ => {
            query.changes.insert(column_name, AssigmentValue::Column(other_column));
            values(tokens, query)
        }
    }
}

fn arithmetic_assigment(tokens: &mut Peekable<IntoIter<Token>>, query: &mut UpdateQuery, column_name: String, other_column: String) -> Result<(), Errors> {
    let op = get_arithmetic_math(tokens)?;
    match get_next_value(tokens)? {
        Term(Literal(literal)) => {
            query.changes.insert(column_name, AssigmentValue::Arithmetic(other_column, op, literal));
            values(tokens, query)
        }
        _ => Err(Errors::SyntaxError("Expected a numeric literal after the arithmetic operator".to_string())),
    }
}

fn where_clause(tokens: &mut Peekable<IntoIter<Token>>, query: &mut UpdateQuery) -> Result<(), Errors> {
    match tokens.peek() {
        Some(Reserved(res)) if res == WHERE => tokens.next(),
        _ => return if_clause(tokens, query),
    };
    match get_next_value(tokens)? {
        TokensList(list) => {
            query.where_clause = WhereClauseParser::parse(list)?;
            if_clause(tokens, query)
        }
        _ => Err(Errors::SyntaxError("Unexpected token in where_clause".to_string()))
    }
}

fn if_clause(_tokens: &mut Peekable<IntoIter<Token>>, _query: &mut UpdateQuery) -> Result<(), Errors> {
    todo!()
}