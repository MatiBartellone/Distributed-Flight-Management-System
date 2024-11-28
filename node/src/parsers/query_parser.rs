use crate::executables::executable::Executable;
use crate::executables::query_executable::QueryExecutable;
use crate::parsers::parser::Parser;
use crate::queries::query::Query;
use crate::utils::errors::Errors;
use crate::utils::types::bytes_cursor::BytesCursor;
use crate::utils::types::token_conversor::get_next_value;

use super::query_parsers::alter_table_parser::AlterTableParser;
use super::query_parsers::delete_query_parser::DeleteQueryParser;
use super::query_parsers::drop_query_parser::DropQueryParser;
use super::query_parsers::insert_query_parser::InsertQueryParser;
use super::query_parsers::select_query_parser::SelectQueryParser;
use super::query_parsers::update_query_parser::UpdateQueryParser;
use super::query_parsers::use_query_parser::UseQueryParser;
use super::tokens::lexer::standardize;
use super::tokens::token::{tokenize, Token};
use crate::parsers::query_parsers::create_query_parser::CreateQueryParser;
use Token::*;

pub struct QueryParser;

impl Parser for QueryParser {
    fn parse(&self, body: &[u8]) -> Result<Box<dyn Executable>, Errors> {
        let mut cursor = BytesCursor::new(body);
        let string = cursor.read_long_string()?;
        let tokens = query_lexer(string)?;
        let query = query_parser(tokens)?;
        let consistency = cursor.read_short()?;
        let executable = QueryExecutable::new(query, consistency);
        Ok(Box::new(executable))
    }
}

/// given a raw string representing a query, returns the tokenized query using the lexer.
pub fn query_lexer(string: String) -> Result<Vec<Token>, Errors> {
    let message = standardize(&string);
    tokenize(message)
}


/// given a tokenized query, matching the query type, returns the matching Query trait implementation
pub fn query_parser(tokens: Vec<Token>) -> Result<Box<dyn Query>, Errors> {
    let mut tokens_iter = tokens.into_iter().peekable();

    match get_next_value(&mut tokens_iter) {
        Ok(Reserved(res)) => {
            let tokens: Vec<Token> = tokens_iter.collect();
            match res.as_str() {
                "SELECT" => Ok(Box::new(SelectQueryParser::parse(tokens)?)),
                "INSERT" => Ok(Box::new(InsertQueryParser::parse(tokens)?)),
                "UPDATE" => Ok(Box::new(UpdateQueryParser::parse(tokens)?)),
                "DELETE" => Ok(Box::new(DeleteQueryParser::parse(tokens)?)),
                "USE" => Ok(Box::new(UseQueryParser.parse(tokens)?)),
                "ALTER" => Ok(Box::new(AlterTableParser.parse(tokens)?)),
                "DROP" => DropQueryParser::parse(tokens),
                "CREATE" => CreateQueryParser::parse(tokens),
                _ => Err(Errors::SyntaxError(format!("Unknown query type: {}", res))),
            }
        }
        _ => Err(Errors::SyntaxError("Invalid CQL syntax".to_string())),
    }
}

#[cfg(test)]
mod tests {
    use crate::{parsers::parser::Parser, utils::errors::Errors};

    use super::QueryParser;

    #[test]
    fn test_parse_select_query_with_consistency() {
        let query = "SELECT id FROM kp.users".as_bytes();
        let len = (query.len() as i32).to_be_bytes();
        let consistency = 0x0001_i16.to_be_bytes();
        let mut body = Vec::new();
        body.extend_from_slice(&len);
        body.extend_from_slice(query);
        body.extend_from_slice(&consistency);

        let parser = QueryParser;
        let result = parser.parse(&body);
        if let Err(e) = &result {
            println!("Error al parsear: {:?}", e);
        }
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_select_query_no_consistency() {
        let query = "SELECT id FROM kp.users".as_bytes();
        let len = (query.len() as i32).to_be_bytes();
        let mut body = Vec::new();
        body.extend_from_slice(&len);
        body.extend_from_slice(query);

        let parser = QueryParser;
        let result = parser.parse(&body);

        assert!(result.is_err());

        if let Err(e) = result {
            match e {
                Errors::ProtocolError(msg) => assert_eq!(msg, "Could not read bytes"),
                _ => panic!("Se esperaba un error de consistencia"),
            }
        }
    }
}
