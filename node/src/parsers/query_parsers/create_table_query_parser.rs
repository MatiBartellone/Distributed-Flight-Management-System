use crate::parsers::tokens::data_type::DataType;
use crate::parsers::tokens::token::Token;
use crate::queries::create_table_query::CreateTableQuery;
use crate::utils::errors::Errors;
use std::vec::IntoIter;

const TABLE: &str = "TABLE";
const PRIMARY: &str = "PRIMARY";
const KEY: &str = "KEY";
const COMMA: &str = ",";

pub struct CreateTableQueryParser;

impl CreateTableQueryParser {
    pub fn parse(tokens: Vec<Token>) -> Result<CreateTableQuery, Errors> {
        let mut create_table_query = CreateTableQuery::new();
        table(&mut tokens.into_iter(), &mut create_table_query)?;
        check_primary_key(&mut create_table_query)?;
        Ok(create_table_query)
    }
}

fn table(tokens: &mut IntoIter<Token>, query: &mut CreateTableQuery) -> Result<(), Errors> {
    match get_next_value(tokens)? {
        Token::Reserved(res) if res == *TABLE => table_name(tokens, query),
        _ => Err(Errors::SyntaxError(String::from(
            "CREATE not followed by TABLE",
        ))),
    }
}
fn table_name(tokens: &mut IntoIter<Token>, query: &mut CreateTableQuery) -> Result<(), Errors> {
    match get_next_value(tokens)? {
        Token::Identifier(identifier) => {
            query.table_name = identifier;
            column_list(tokens, query)
        }
        _ => Err(Errors::SyntaxError(String::from(
            "Unexpected token in table name",
        ))),
    }
}

fn column_list(tokens: &mut IntoIter<Token>, query: &mut CreateTableQuery) -> Result<(), Errors> {
    match get_next_value(tokens)? {
        Token::ParenList(list) => {
            column(&mut list.into_iter(), query)?;
            let None = tokens.next() else {
                return Err(Errors::SyntaxError(String::from(
                    "Nothing should follow the column list",
                )));
            };
            Ok(())
        }
        _ => Err(Errors::SyntaxError(String::from(
            "Unexpected token in column definition",
        ))),
    }
}

fn column(tokens: &mut IntoIter<Token>, query: &mut CreateTableQuery) -> Result<(), Errors> {
    let Some(token) = tokens.next() else {
        return Ok(());
    };
    match token {
        Token::Identifier(identifier) => {
            query
                .columns
                .insert(identifier.clone(), get_data_type(tokens)?);
            try_primary_key(tokens, query, identifier)
        }
        Token::Reserved(res) if res == *PRIMARY => key(tokens, query),
        _ => Err(Errors::SyntaxError(String::from(
            "Unexpected token in column definition",
        ))),
    }
}

fn try_primary_key(
    tokens: &mut IntoIter<Token>,
    query: &mut CreateTableQuery,
    primary_key: String,
) -> Result<(), Errors> {
    let Some(token) = tokens.next() else {
        return Ok(());
    };
    match token {
        Token::Symbol(s) if s == *COMMA => column(tokens, query),
        Token::Reserved(res) if res == *PRIMARY => primary_key_def(tokens, query, primary_key),
        _ => Err(Errors::SyntaxError(String::from(
            "Unexpected token in column definition",
        ))),
    }
}

fn primary_key_def(
    tokens: &mut IntoIter<Token>,
    query: &mut CreateTableQuery,
    primary_key: String,
) -> Result<(), Errors> {
    match get_next_value(tokens)? {
        Token::Reserved(res) if res == *KEY => {
            set_primary_key(query, primary_key)?;
            match get_next_value(tokens)? {
                Token::Symbol(s) if s == *COMMA => column(tokens, query),
                _ => Err(Errors::SyntaxError(String::from(
                    "Comma missing after PRIMARY KEY",
                ))),
            }
        }
        _ => Err(Errors::SyntaxError(String::from(
            "PRIMARY not followed by KEY",
        ))),
    }
}
fn key(tokens: &mut IntoIter<Token>, query: &mut CreateTableQuery) -> Result<(), Errors> {
    match get_next_value(tokens)? {
        Token::Reserved(res) if res == *KEY => primary_key_list(tokens, query),
        _ => Err(Errors::SyntaxError(String::from(
            "PRIMARY not followed by KEY",
        ))),
    }
}

fn primary_key_list(
    tokens: &mut IntoIter<Token>,
    query: &mut CreateTableQuery,
) -> Result<(), Errors> {
    match get_next_value(tokens)? {
        Token::ParenList(list) => {
            if list.len() != 1 {
                return Err(Errors::SyntaxError(String::from(
                    "Primary key between parenthesis must be 1",
                )));
            };
            match list.first() {
                Some(Token::Identifier(identifier)) => {
                    set_primary_key(query, identifier.to_string())?;
                    Ok(())
                }
                _ => Err(Errors::SyntaxError(String::from(
                    "Unexpected token in primary key list",
                ))),
            }
        }
        _ => Err(Errors::SyntaxError(String::from(
            "Unexpected token in primary key list",
        ))),
    }
}

fn get_data_type(tokens: &mut IntoIter<Token>) -> Result<DataType, Errors> {
    match tokens.next() {
        Some(Token::DataType(data_type)) => Ok(data_type),
        _ => Err(Errors::SyntaxError(String::from("Missing data type"))),
    }
}

fn get_next_value(tokens: &mut IntoIter<Token>) -> Result<Token, Errors> {
    tokens
        .next()
        .ok_or(Errors::SyntaxError(String::from("Query lacks parameters")))
}

fn set_primary_key(query: &mut CreateTableQuery, primary_key: String) -> Result<(), Errors> {
    if query.primary_key.is_empty() {
        query.primary_key = primary_key;
        return Ok(());
    }
    Err(Errors::SyntaxError(String::from(
        "Primary key must be defined only once",
    )))
}

fn check_primary_key(query: &mut CreateTableQuery) -> Result<(), Errors> {
    if !query.columns.contains_key(query.primary_key.as_str()) {
        return Err(Errors::SyntaxError(String::from(
            "Primary key column not defined",
        )));
    }
    if query.primary_key.is_empty() {
        return Err(Errors::SyntaxError(String::from("Primary key not defined")));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parsers::tokens::token::Token;
    use std::collections::HashMap;

    fn assert_error(result: Result<CreateTableQuery, Errors>, expected: &str) {
        assert!(result.is_err());
        if let Err(Errors::SyntaxError(err)) = result {
            assert_eq!(err, expected);
        }
    }

    fn get_valid_tokens_1(col1: Token, type1: Token) -> Vec<Token> {
        vec![
            Token::Reserved(String::from(TABLE)),
            Token::Identifier(String::from("table_name")),
            Token::ParenList(vec![
                col1,
                type1,
                Token::Reserved(String::from(PRIMARY)),
                Token::Reserved(String::from(KEY)),
                Token::Symbol(String::from(COMMA)),
                Token::Identifier(String::from("name")),
                Token::DataType(DataType::Text),
                Token::Symbol(String::from(COMMA)),
            ]),
        ]
    }
    fn get_valid_tokens_2(col1: Token, type1: Token, primary_key: &str) -> Vec<Token> {
        vec![
            Token::Reserved(String::from(TABLE)),
            Token::Identifier(String::from("table_name")),
            Token::ParenList(vec![
                col1,
                type1,
                Token::Symbol(String::from(COMMA)),
                Token::Identifier(String::from("name")),
                Token::DataType(DataType::Text),
                Token::Symbol(String::from(COMMA)),
                Token::Reserved(String::from(PRIMARY)),
                Token::Reserved(String::from(KEY)),
                Token::ParenList(vec![Token::Identifier(String::from(primary_key)),])
            ]),
        ]
    }

    fn get_valid_query() -> CreateTableQuery {
        CreateTableQuery {
            table_name: String::from("table_name"),
            columns: HashMap::from([
                (String::from("id"), DataType::Int),
                (String::from("name"), DataType::Text),
            ]),
            primary_key: String::from("id"),
        }
    }

    #[test]
    fn test_create_table_valid_1() {
        let tokens = get_valid_tokens_1(Token::Identifier(String::from("id")), Token::DataType(DataType::Int));
        let expected = get_valid_query();
        assert_eq!(expected, CreateTableQueryParser::parse(tokens).unwrap());
    }

    #[test]
    fn test_create_table_valid_2() {
        let tokens = get_valid_tokens_2(Token::Identifier(String::from("id")), Token::DataType(DataType::Int), "id");
        let expected = get_valid_query();
        assert_eq!(expected, CreateTableQueryParser::parse(tokens).unwrap());
    }

    #[test]
    fn test_create_table_equal_primary_key_definitions() {
        let tokens_1 = get_valid_tokens_1(Token::Identifier(String::from("id")), Token::DataType(DataType::Int));
        let tokens_2 = get_valid_tokens_2(Token::Identifier(String::from("id")), Token::DataType(DataType::Int), "id");
        assert_eq!(CreateTableQueryParser::parse(tokens_1).unwrap(), CreateTableQueryParser::parse(tokens_2).unwrap());
    }

    #[test]
    fn test_create_table_missing_table_keyword() {
        let tokens = vec![Token::Identifier(String::from("NOT TABLE"))];
        let result = CreateTableQueryParser::parse(tokens);
        assert_error(result, "CREATE not followed by TABLE");
    }

    #[test]
    fn test_create_table_unexpected_table_name() {
        let tokens = vec![
            Token::Reserved(String::from(TABLE)),
            Token::Reserved(String::from("UNEXPECTED")),
        ];
        let result = CreateTableQueryParser::parse(tokens);
        assert_error(result, "Unexpected token in table name");
    }

    #[test]
    fn test_create_table_unexpected_column_definition() {
        let tokens = vec![
            Token::Reserved(String::from(TABLE)),
            Token::Identifier(String::from("table_name")),
            Token::Identifier(String::from("UNEXPECTED")),
        ];
        let result = CreateTableQueryParser::parse(tokens);
        assert_error(result, "Unexpected token in column definition");
    }

    #[test]
    fn test_create_table_unexpected_column_definition_not_an_identifier() {
        let tokens = get_valid_tokens_1(Token::Reserved(String::from("UNEXPECTED")), Token::DataType(DataType::Text));
        let result = CreateTableQueryParser::parse(tokens);
        assert_error(result, "Unexpected token in column definition");
    }

    #[test]
    fn test_create_table_missing_data_type() {
        let tokens = get_valid_tokens_1(Token::Identifier(String::from("id")), Token::Symbol(String::from(COMMA)));
        let result = CreateTableQueryParser::parse(tokens);
        assert_error(result, "Missing data type");
    }

    #[test]
    fn test_create_table_none_existant_primary_key() {
        let tokens = get_valid_tokens_2(Token::Identifier(String::from("id")), Token::DataType(DataType::Int), "NOT EXISTENT");
        let result = CreateTableQueryParser::parse(tokens);
        assert_error(result, "Primary key column not defined");
    }

    #[test]
    fn test_create_table_more_than_one_primary_key() {
        let tokens = vec![
            Token::Reserved(String::from(TABLE)),
            Token::Identifier(String::from("table_name")),
            Token::ParenList(vec![
                Token::Identifier(String::from("id")),
                Token::DataType(DataType::Int),
                Token::Reserved(String::from(PRIMARY)),
                Token::Reserved(String::from(KEY)),
                Token::Symbol(String::from(COMMA)),
                Token::Identifier(String::from("name")),
                Token::DataType(DataType::Text),
                Token::Reserved(String::from(PRIMARY)),
                Token::Reserved(String::from(KEY)),
            ]),
        ];
        let result = CreateTableQueryParser::parse(tokens);
        assert_error(result, "Primary key must be defined only once");
    }

    #[test]
    fn test_create_table_not_defined_primary_key() {
        let tokens = vec![
            Token::Reserved(String::from(TABLE)),
            Token::Identifier(String::from("table_name")),
            Token::ParenList(vec![
                Token::Identifier(String::from("id")),
                Token::DataType(DataType::Int),
            ]),
        ];
        let result = CreateTableQueryParser::parse(tokens);
        assert_error(result, "Primary key column not defined");
    }
}
