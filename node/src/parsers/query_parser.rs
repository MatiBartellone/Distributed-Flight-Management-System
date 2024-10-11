use crate::executables::executable::Executable;
use crate::executables::query_executable::QueryExecutable;
use crate::parsers::parser::Parser;
use crate::queries::cql_query::CQLQuery;
use crate::utils::bytes_cursor::BytesCursor;
use crate::utils::errors::Errors;
use crate::utils::token_conversor::get_next_value;

use super::query_parsers::delete_query_parser::DeleteQueryParser;
use super::query_parsers::insert_query_parser::InsertQueryParser;
use super::query_parsers::select_query_parser::SelectQueryParser;
use super::tokens::lexer::standardize;
use super::tokens::token::{tokenize, Token};
use Token::*;

pub struct QueryParser;

impl Parser for QueryParser {
    fn parse(&self, body: &[u8]) -> Result<Box<dyn Executable>, Errors> {
        let mut cursor = BytesCursor::new(body);
        let tokens = query_lexer(& mut cursor)?;
        let query = query_parser(tokens)?;
        let consistency = cursor.read_consistency()?;
        let executable = QueryExecutable::new(query, consistency); 
        Ok(Box::new(executable))
    }
}

fn query_lexer(cursor: &mut BytesCursor) -> Result<Vec<Token>, Errors> {
    let string = cursor.read_long_string()?;
    let message = standardize(&string);
    tokenize(message)
}

fn query_parser(tokens: Vec<Token>) -> Result<Box<dyn >, Errors> {
    let mut tokens_iter = tokens.into_iter().peekable();

    match get_next_value(&mut tokens_iter) {
        Ok(Reserved(res)) => {
            let tokens: Vec<Token> = tokens_iter.collect();
            match res.as_str() {
                "SELECT" => SelectQueryParser::parse(tokens),
                "INSERT" => InsertQueryParser::parse(tokens),
                //"UPDATE" => UpdateQueryParser::parse(tokens),
                "DELETE" => DeleteQueryParser::parse(tokens),
                //"DROP" => DropQueryParser::parse(tokens),
                _ => Err(Errors::SyntaxError(format!("Unknown query type: {}", res))),
            }
        },
        _ => Err(Errors::SyntaxError("Invalid CQL syntax".to_string())),
    }
}