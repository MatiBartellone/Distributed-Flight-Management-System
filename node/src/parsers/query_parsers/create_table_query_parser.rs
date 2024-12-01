use crate::parsers::tokens::data_type::DataType;
use crate::parsers::tokens::token::Token;
use crate::queries::create_table_query::CreateTableQuery;
use crate::utils::errors::Errors;
use std::vec::IntoIter;

const PRIMARY: &str = "PRIMARY";
const KEY: &str = "KEY";
const COMMA: &str = ",";
const UNEXPECTED_TABLE_ERR: &str = "Unexpected token in table name";
const NOTHING_AFTER_CL_ERR: &str = "Nothing should follow the column list";
const UNEXPECTED_COLUMN_ERR: &str = "Unexpected token in column definition";
const COMMA_MISSING_PR_ERR: &str = "Comma missing after PRIMARY KEY";
const MISSING_KEY_ERR: &str = "PRIMARY not followed by KEY";
const UNEXPECTED_PK_ERR: &str = "Unexpected token in primary key list";
const MISSING_DT_ERR: &str = "Missing data type";
const SHORT_QUERY_ERR: &str = "Query lacks parameters";

pub struct CreateTableQueryParser;

impl CreateTableQueryParser {
    pub fn parse(tokens: Vec<Token>) -> Result<CreateTableQuery, Errors> {
        let mut create_table_query = CreateTableQuery::new();
        table_name(&mut tokens.into_iter(), &mut create_table_query)?;
        check_primary_key(&mut create_table_query)?;
        Ok(create_table_query)
    }
}

fn table_name(tokens: &mut IntoIter<Token>, query: &mut CreateTableQuery) -> Result<(), Errors> {
    match get_next_value(tokens)? {
        Token::Identifier(identifier) => {
            query.table_name = identifier;
            column_list(tokens, query)
        }
        _ => Err(Errors::SyntaxError(String::from(UNEXPECTED_TABLE_ERR))),
    }
}

fn column_list(tokens: &mut IntoIter<Token>, query: &mut CreateTableQuery) -> Result<(), Errors> {
    match get_next_value(tokens)? {
        Token::ParenList(list) => {
            column(&mut list.into_iter(), query)?;
            let None = tokens.next() else {
                return Err(Errors::SyntaxError(String::from(NOTHING_AFTER_CL_ERR)));
            };
            Ok(())
        }
        _ => Err(Errors::SyntaxError(String::from(UNEXPECTED_COLUMN_ERR))),
    }
}

fn column(tokens: &mut IntoIter<Token>, query: &mut CreateTableQuery) -> Result<(), Errors> {
    let Some(token) = tokens.next() else {
        return Ok(());
    };
    match token {
        Token::Identifier(identifier) => {
            if query.columns.contains_key(&identifier.to_string()) {
                return Err(Errors::Invalid(String::from("There is a repeated column")));
            }
            query
                .columns
                .insert(identifier.to_string(), get_data_type(tokens)?);
            try_partition_key(tokens, query, identifier)
        }
        Token::Reserved(res) if res == *PRIMARY => {
            if !query.primary_key.get_partition_key().is_empty() {
                return Err(Errors::SyntaxError(String::from(
                    "Partition key already defined",
                )));
            }
            key(tokens, query)
        }
        _ => Err(Errors::SyntaxError(String::from(UNEXPECTED_COLUMN_ERR))),
    }
}

fn try_partition_key(
    tokens: &mut IntoIter<Token>,
    query: &mut CreateTableQuery,
    partition_key: String,
) -> Result<(), Errors> {
    let Some(token) = tokens.next() else {
        return Ok(());
    };
    match token {
        Token::Symbol(s) if s == *COMMA => column(tokens, query),
        Token::Reserved(res) if res == *PRIMARY => partition_key_def(tokens, query, partition_key),
        _ => Err(Errors::SyntaxError(String::from(UNEXPECTED_COLUMN_ERR))),
    }
}

fn partition_key_def(
    tokens: &mut IntoIter<Token>,
    query: &mut CreateTableQuery,
    partition_key: String,
) -> Result<(), Errors> {
    match get_next_value(tokens)? {
        Token::Reserved(res) if res == *KEY => {
            set_partition_key(query, partition_key)?;
            match get_next_value(tokens)? {
                Token::Symbol(s) if s == *COMMA => column(tokens, query),
                _ => Err(Errors::SyntaxError(String::from(COMMA_MISSING_PR_ERR))),
            }
        }
        _ => Err(Errors::SyntaxError(String::from(MISSING_KEY_ERR))),
    }
}

fn set_partition_key(query: &mut CreateTableQuery, partition_key: String) -> Result<(), Errors> {
    if query.primary_key.get_partition_key().is_empty() {
        query.primary_key.add_partition_key(partition_key);
        return Ok(());
    }
    Err(Errors::SyntaxError(String::from(
        "Only one partition key is allowed using this method",
    )))
}
fn key(tokens: &mut IntoIter<Token>, query: &mut CreateTableQuery) -> Result<(), Errors> {
    match get_next_value(tokens)? {
        Token::Reserved(res) if res == *KEY => primary_key_list(tokens, query),
        _ => Err(Errors::SyntaxError(String::from(MISSING_KEY_ERR))),
    }
}

fn primary_key_list(
    tokens: &mut IntoIter<Token>,
    query: &mut CreateTableQuery,
) -> Result<(), Errors> {
    match get_next_value(tokens)? {
        Token::ParenList(list) => {
            let mut primary_key_tokens = list.into_iter();
            match get_next_value(&mut primary_key_tokens)? {
                Token::ParenList(partition_key_list) => {
                    set_partition_key_list(&mut partition_key_list.into_iter(), query)
                }
                Token::Identifier(identifier) => set_partition_key(query, identifier.to_string()),
                _ => Err(Errors::SyntaxError(String::from(UNEXPECTED_PK_ERR))),
            }?;
            clustering_columns(&mut primary_key_tokens, query)
        }
        _ => Err(Errors::SyntaxError(String::from(UNEXPECTED_PK_ERR))),
    }
}

fn clustering_columns(
    tokens: &mut IntoIter<Token>,
    query: &mut CreateTableQuery,
) -> Result<(), Errors> {
    let Some(token) = tokens.next() else {
        return Ok(());
    };
    match token {
        Token::Symbol(symbol) if symbol == COMMA => set_clustering_column(tokens, query),
        _ => Err(Errors::SyntaxError(String::from(UNEXPECTED_PK_ERR))),
    }
}

fn set_clustering_column(
    tokens: &mut IntoIter<Token>,
    query: &mut CreateTableQuery,
) -> Result<(), Errors> {
    match get_next_value(tokens)? {
        Token::Identifier(column) => {
            query.primary_key.add_clustering_column(column);
            clustering_columns(tokens, query)
        }
        _ => Err(Errors::SyntaxError(String::from(UNEXPECTED_PK_ERR))),
    }
}

fn set_partition_key_list(
    tokens: &mut IntoIter<Token>,
    query: &mut CreateTableQuery,
) -> Result<(), Errors> {
    let Some(token) = tokens.next() else {
        return Ok(());
    };
    match token {
        Token::Identifier(partition_key) => {
            query
                .primary_key
                .add_partition_key(partition_key.to_string());
            try_comma(tokens)?;
            set_partition_key_list(tokens, query)
        }
        _ => Err(Errors::SyntaxError(String::from(UNEXPECTED_PK_ERR))),
    }
}

fn try_comma(tokens: &mut IntoIter<Token>) -> Result<(), Errors> {
    let Some(token) = tokens.next() else {
        return Ok(());
    };
    match token {
        Token::Symbol(symbol) if symbol == COMMA => Ok(()),
        _ => Err(Errors::SyntaxError(String::from(UNEXPECTED_PK_ERR))),
    }
}

fn get_data_type(tokens: &mut IntoIter<Token>) -> Result<DataType, Errors> {
    match tokens.next() {
        Some(Token::DataType(data_type)) => Ok(data_type),
        _ => Err(Errors::SyntaxError(String::from(MISSING_DT_ERR))),
    }
}

fn get_next_value(tokens: &mut IntoIter<Token>) -> Result<Token, Errors> {
    tokens
        .next()
        .ok_or(Errors::SyntaxError(String::from(SHORT_QUERY_ERR)))
}

fn check_primary_key(query: &mut CreateTableQuery) -> Result<(), Errors> {
    if query.primary_key.get_partition_key().is_empty() {
        return Err(Errors::SyntaxError(String::from(
            "Partition key not defined",
        )));
    }
    for partition in query.primary_key.get_partition_key().iter() {
        if !query.columns.contains_key(partition) {
            return Err(Errors::SyntaxError(format!(
                "Partition key {} not found in columns",
                partition
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parsers::tokens::token::Token;
    use crate::utils::types::primary_key::PrimaryKey;
    use std::collections::HashMap;

    fn assert_error(result: Result<CreateTableQuery, Errors>, expected: &str) {
        assert!(result.is_err());
        if let Err(Errors::SyntaxError(err)) = result {
            assert_eq!(err, expected);
        }
    }

    fn get_valid_tokens_1(col1: Token, type1: Token) -> Vec<Token> {
        vec![
            Token::Identifier(String::from("kp.table_name")),
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
            Token::Identifier(String::from("kp.table_name")),
            Token::ParenList(vec![
                col1,
                type1,
                Token::Symbol(String::from(COMMA)),
                Token::Identifier(String::from("name")),
                Token::DataType(DataType::Text),
                Token::Symbol(String::from(COMMA)),
                Token::Reserved(String::from(PRIMARY)),
                Token::Reserved(String::from(KEY)),
                Token::ParenList(vec![Token::Identifier(String::from(primary_key))]),
            ]),
        ]
    }

    fn get_valid_query() -> CreateTableQuery {
        CreateTableQuery {
            table_name: String::from("kp.table_name"),
            columns: HashMap::from([
                (String::from("id"), DataType::Int),
                (String::from("name"), DataType::Text),
            ]),
            primary_key: PrimaryKey::new(vec![String::from("id")], None),
        }
    }

    #[test]
    fn test_create_table_valid_1() {
        let tokens = get_valid_tokens_1(
            Token::Identifier(String::from("id")),
            Token::DataType(DataType::Int),
        );
        let expected = get_valid_query();
        assert_eq!(expected, CreateTableQueryParser::parse(tokens).unwrap());
    }

    #[test]
    fn test_create_table_valid_2() {
        let tokens = get_valid_tokens_2(
            Token::Identifier(String::from("id")),
            Token::DataType(DataType::Int),
            "id",
        );
        let expected = get_valid_query();
        assert_eq!(expected, CreateTableQueryParser::parse(tokens).unwrap());
    }

    #[test]
    fn test_create_table_equal_primary_key_definitions() {
        let tokens_1 = get_valid_tokens_1(
            Token::Identifier(String::from("id")),
            Token::DataType(DataType::Int),
        );
        let tokens_2 = get_valid_tokens_2(
            Token::Identifier(String::from("id")),
            Token::DataType(DataType::Int),
            "id",
        );
        assert_eq!(
            CreateTableQueryParser::parse(tokens_1).unwrap(),
            CreateTableQueryParser::parse(tokens_2).unwrap()
        );
    }

    #[test]
    fn test_create_table_unexpected_table_name() {
        let tokens = vec![Token::Reserved(String::from("UNEXPECTED"))];
        let result = CreateTableQueryParser::parse(tokens);
        assert_error(result, UNEXPECTED_TABLE_ERR);
    }

    #[test]
    fn test_create_table_unexpected_column_definition() {
        let tokens = vec![
            Token::Identifier(String::from("table_name")),
            Token::Identifier(String::from("UNEXPECTED")),
        ];
        let result = CreateTableQueryParser::parse(tokens);
        assert_error(result, UNEXPECTED_COLUMN_ERR);
    }

    #[test]
    fn test_create_table_unexpected_column_definition_not_an_identifier() {
        let tokens = get_valid_tokens_1(
            Token::Reserved(String::from("UNEXPECTED")),
            Token::DataType(DataType::Text),
        );
        let result = CreateTableQueryParser::parse(tokens);
        assert_error(result, UNEXPECTED_COLUMN_ERR);
    }

    #[test]
    fn test_create_table_missing_data_type() {
        let tokens = get_valid_tokens_1(
            Token::Identifier(String::from("id")),
            Token::Symbol(String::from(COMMA)),
        );
        let result = CreateTableQueryParser::parse(tokens);
        assert_error(result, MISSING_DT_ERR);
    }

    #[test]
    fn test_create_table_none_existant_primary_key() {
        let tokens = get_valid_tokens_2(
            Token::Identifier(String::from("id")),
            Token::DataType(DataType::Int),
            "NOT EXISTENT",
        );
        let result = CreateTableQueryParser::parse(tokens);
        assert_error(result, "Partition key NOT EXISTENT not found in columns");
    }

    #[test]
    fn test_create_table_more_than_one_primary_key() {
        let tokens = vec![
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
        assert_error(
            result,
            "Only one partition key is allowed using this method",
        );
    }

    #[test]
    fn test_create_table_not_defined_primary_key() {
        let tokens = vec![
            Token::Identifier(String::from("table_name")),
            Token::ParenList(vec![
                Token::Identifier(String::from("id")),
                Token::DataType(DataType::Int),
            ]),
        ];
        let result = CreateTableQueryParser::parse(tokens);
        assert_error(result, "Partition key not defined");
    }

    #[test]
    fn test_primary_key_multiple_partitions_and_clustering_columns() {
        let tokens = vec![
            Token::Identifier(String::from("table_name")),
            Token::ParenList(vec![
                Token::Identifier(String::from("id")),
                Token::DataType(DataType::Int),
                Token::Symbol(String::from(COMMA)),
                Token::Identifier(String::from("name")),
                Token::DataType(DataType::Text),
                Token::Symbol(String::from(COMMA)),
                Token::Identifier(String::from("surname")),
                Token::DataType(DataType::Text),
                Token::Symbol(String::from(COMMA)),
                Token::Identifier(String::from("address")),
                Token::DataType(DataType::Text),
                Token::Symbol(String::from(COMMA)),
                Token::Reserved(String::from(PRIMARY)),
                Token::Reserved(String::from(KEY)),
                Token::ParenList(vec![
                    Token::ParenList(vec![
                        Token::Identifier(String::from("id")),
                        Token::Symbol(String::from(COMMA)),
                        Token::Identifier(String::from("name")),
                    ]),
                    Token::Symbol(String::from(COMMA)),
                    Token::Identifier(String::from("surname")),
                    Token::Symbol(String::from(COMMA)),
                    Token::Identifier(String::from("address")),
                ]),
            ]),
        ];
        let result = CreateTableQueryParser::parse(tokens).unwrap();
        let expected_pk = PrimaryKey::new(
            vec![String::from("id"), String::from("name")],
            Some(vec![String::from("surname"), String::from("address")]),
        );
        assert_eq!(result.primary_key, expected_pk);
    }

    #[test]
    fn test_primary_key_multiple_partitions_and_clustering_columns_2() {
        let tokens = vec![
            Token::Identifier(String::from("table_name")),
            Token::ParenList(vec![
                Token::Identifier(String::from("id")),
                Token::DataType(DataType::Int),
                Token::Symbol(String::from(COMMA)),
                Token::Identifier(String::from("name")),
                Token::DataType(DataType::Text),
                Token::Symbol(String::from(COMMA)),
                Token::Identifier(String::from("surname")),
                Token::DataType(DataType::Text),
                Token::Symbol(String::from(COMMA)),
                Token::Reserved(String::from(PRIMARY)),
                Token::Reserved(String::from(KEY)),
                Token::ParenList(vec![
                    Token::Identifier(String::from("id")),
                    Token::Symbol(String::from(COMMA)),
                    Token::Identifier(String::from("name")),
                    Token::Symbol(String::from(COMMA)),
                    Token::Identifier(String::from("surname")),
                ]),
            ]),
        ];
        let result = CreateTableQueryParser::parse(tokens).unwrap();
        let expected_pk = PrimaryKey::new(
            vec![String::from("id")],
            Some(vec![String::from("name"), String::from("surname")]),
        );
        assert_eq!(result.primary_key, expected_pk);
    }
}
