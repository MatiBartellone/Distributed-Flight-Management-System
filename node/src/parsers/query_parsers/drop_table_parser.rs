use crate::{parsers::tokens::token::Token, 
    queries::drop_table_query::DropTableQuery, 
    utils::{errors::Errors, token_conversor::get_next_value}};
use crate::utils::constants::*;
use std::{vec::IntoIter, iter::Peekable};

pub struct DropTableQueryParser;

impl DropTableQueryParser {
    pub fn parse(tokens: &mut Peekable<IntoIter<Token>>) -> Result<DropTableQuery, Errors> {
        let mut drop_query = DropTableQuery::new();
        frok(tokens, &mut drop_query)?;
        Ok(drop_query)
    }

}

fn frok(tokens: &mut Peekable<IntoIter<Token>>, query: &mut DropTableQuery) -> Result<(), Errors> {
    match tokens.peek() {
        Some(Token::Identifier(_)) => table(tokens, query),
        Some(Token::Reserved(_)) => ifa(tokens, query),
        _ => {
            Err(Errors::SyntaxError(String::from(
                "Unexpected token in table_name",
            )))
        }
    }
}

fn table(tokens: &mut Peekable<IntoIter<Token>>, query: &mut DropTableQuery) -> Result<(), Errors> {
    match get_next_value(tokens)?{
        Token::Identifier(title) => {
            query.table = title;
            finish(tokens)
        }
        _ => {
            Err(Errors::SyntaxError(String::from(
            "Unexpected token in table_name",
        )))},
    }
}

fn ifa(tokens: &mut Peekable<IntoIter<Token>>, query: &mut DropTableQuery) -> Result<(), Errors> {
    match get_next_value(tokens)?{
        Token::Reserved(res) if res == *IF => exists(tokens, query) ,
        _ => {
            Err(Errors::SyntaxError(String::from(
            "Unexpected token in table_name",
        )))},
    }
}


fn finish(tokens: &mut Peekable<IntoIter<Token>>) -> Result<(), Errors> {
    if tokens.next().is_none(){
        return Ok(())
    }
    Err(Errors::SyntaxError(String::from(
        "DROP with left over paramameters",
    )))
}

fn exists(tokens: &mut Peekable<IntoIter<Token>>, query: &mut DropTableQuery) -> Result<(), Errors> {
    match get_next_value(tokens)? {
        Token::Reserved(res) if res == *EXISTS => {
            query.if_exist = Some(true);
            table(tokens, query)
        }
        _ => Err(Errors::SyntaxError(String::from(
            "Unexpected token after IF",
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parsers::tokens::token::Token;
    
    use crate::utils::errors::Errors;

    #[test]
    fn test_parse_drop_table_simple() {
        let tokens = vec![
            Token::Identifier("my_table".to_string()),
        ];
        let mut token_iter = tokens.into_iter().peekable();
        let result = DropTableQueryParser::parse(&mut token_iter);
        assert!(result.is_ok());
        let query = result.unwrap();
        assert_eq!(query.table, "my_table");
        assert_eq!(query.if_exist, None); // No hay IF EXISTS
    }

    #[test]
    fn test_parse_drop_table_if_exists() {
        let tokens = vec![
            Token::Reserved("IF".to_string()),
            Token::Reserved("EXISTS".to_string()),
            Token::Identifier("my_table".to_string()),
        ];
        let mut token_iter = tokens.into_iter().peekable();
        let result = DropTableQueryParser::parse(&mut token_iter);
        assert!(result.is_ok());
        let query = result.unwrap();
        assert_eq!(query.table, "my_table");
        assert_eq!(query.if_exist, Some(true)); // Debe tener IF EXISTS
    }

    #[test]
    fn test_parse_drop_table_invalid_token() {
        let tokens = vec![
            Token::Reserved("INVALID".to_string()), // Token inválido
        ];
        let mut token_iter = tokens.into_iter().peekable();
        let result = DropTableQueryParser::parse(&mut token_iter);
        assert!(result.is_err());
        if let Err(Errors::SyntaxError(err_msg)) = result {
            assert_eq!(err_msg, "Unexpected token in table_name");
        } else {
            panic!("Expected a SyntaxError");
        }
    }

    #[test]
    fn test_parse_drop_table_with_extra_tokens() {
        // Caso de error: DROP table con tokens sobrantes
        let tokens = vec![
            Token::Identifier("my_table".to_string()),
            Token::Identifier("extra_token".to_string()), // Token extra no esperado
        ];
        let mut token_iter = tokens.into_iter().peekable();
        let result = DropTableQueryParser::parse(&mut token_iter);

        assert!(result.is_err());
        if let Err(Errors::SyntaxError(err_msg)) = result {
            assert_eq!(err_msg, "DROP with left over paramameters");
        } else {
            panic!("Expected a SyntaxError");
        }
    }

    #[test]
    fn test_parse_drop_table_if_exists_invalid_syntax() {
        let tokens = vec![
            Token::Reserved("IF".to_string()),
            Token::Reserved("INVALID".to_string()), // Token inválido después de IF
        ];
        let mut token_iter = tokens.into_iter().peekable();
        let result = DropTableQueryParser::parse(&mut token_iter);

        // Verificamos que devuelva un error de sintaxis en el IF
        assert!(result.is_err());
        if let Err(Errors::SyntaxError(err_msg)) = result {
            assert_eq!(err_msg, "Unexpected token after IF");
        } else {
            panic!("Expected a SyntaxError");
        }
    }
}