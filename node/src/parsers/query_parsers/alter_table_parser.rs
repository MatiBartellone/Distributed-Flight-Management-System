use std::{iter::Peekable, vec::IntoIter};

use crate::{
    parsers::tokens::token::Token,
    queries::alter_table_query::{AlterTableQuery, Operations},
    utils::errors::Errors,
};

use crate::utils::types::token_conversor::get_next_value;

const MISSING_KEYWORD: &str = "Missing keyword following table name";
const UNEXPECTED_TOKEN: &str = "Unexpected token";
const TO: &str = "TO";
const TYPE: &str = "TYPE";
const TABLE: &str = "TABLE";
const ADD: &str = "ADD";
const ALTER: &str = "ALTER";
const RENAME: &str = "RENAME";
const DROP: &str = "DROP";
pub struct AlterTableParser;

impl Default for AlterTableParser {
    fn default() -> Self {
        Self::new()
    }
}

impl AlterTableParser {
    pub fn new() -> AlterTableParser {
        AlterTableParser {}
    }

    pub fn parse(&self, tokens: Vec<Token>) -> Result<AlterTableQuery, Errors> {
        let mut alter_table_query = AlterTableQuery::new();
        self.table(&mut tokens.into_iter().peekable(), &mut alter_table_query)?;
        Ok(alter_table_query)
    }

    fn table(
        &self,
        tokens: &mut Peekable<IntoIter<Token>>,
        query: &mut AlterTableQuery,
    ) -> Result<(), Errors> {
        match get_next_value(tokens)? {
            Token::Reserved(table) if table == TABLE => self.table_name(tokens, query),
            _ => Err(Errors::SyntaxError(UNEXPECTED_TOKEN.to_string())),
        }
    }

    fn table_name(
        &self,
        tokens: &mut Peekable<IntoIter<Token>>,
        query: &mut AlterTableQuery,
    ) -> Result<(), Errors> {
        match get_next_value(tokens)? {
            Token::Identifier(table_name) => {
                query.table_name = table_name;
                self.keyword(tokens, query)
            }
            _ => Err(Errors::SyntaxError(UNEXPECTED_TOKEN.to_string())),
        }
    }

    fn keyword(
        &self,
        tokens: &mut Peekable<IntoIter<Token>>,
        query: &mut AlterTableQuery,
    ) -> Result<(), Errors> {
        let add = ADD.to_string();
        let alter = ALTER.to_string();
        let rename = RENAME.to_string();
        let drop = DROP.to_string();

        match get_next_value(tokens)? {
            Token::Reserved(keyword) if keyword == add => self.add(tokens, query),
            Token::Reserved(keyword) if keyword == alter => self.alter(tokens, query),
            Token::Reserved(keyword) if keyword == rename => self.rename(tokens, query),
            Token::Reserved(keyword) if keyword == drop => self.drop(tokens, query),
            Token::Reserved(_) => Err(Errors::SyntaxError(UNEXPECTED_TOKEN.to_string())),
            _ => Err(Errors::SyntaxError(MISSING_KEYWORD.to_string())),
        }
    }

    fn add(
        &self,
        tokens: &mut Peekable<IntoIter<Token>>,
        query: &mut AlterTableQuery,
    ) -> Result<(), Errors> {
        let new_column_name = self.column_name(tokens)?;
        query.first_column = new_column_name;
        query.operation = Some(Operations::ADD);
        self.data_type(tokens, query)
    }

    fn alter(
        &self,
        tokens: &mut Peekable<IntoIter<Token>>,
        query: &mut AlterTableQuery,
    ) -> Result<(), Errors> {
        let column_name = self.column_name(tokens)?;
        query.first_column = column_name;
        query.operation = Some(Operations::ALTER);
        self.type_keyword(tokens, query)
    }

    fn type_keyword(
        &self,
        tokens: &mut Peekable<IntoIter<Token>>,
        query: &mut AlterTableQuery,
    ) -> Result<(), Errors> {
        match get_next_value(tokens)? {
            Token::Reserved(keyword) if keyword == TYPE => self.data_type(tokens, query),
            _ => Err(Errors::SyntaxError(UNEXPECTED_TOKEN.to_string())),
        }
    }

    fn data_type(
        &self,
        tokens: &mut Peekable<IntoIter<Token>>,
        query: &mut AlterTableQuery,
    ) -> Result<(), Errors> {
        let text = get_next_value(tokens)?;
        match text {
            Token::DataType(data) => {
                query.data = data;
                Ok(())
            }
            _ => Err(Errors::SyntaxError(UNEXPECTED_TOKEN.to_string())),
        }
    }

    fn rename(
        &self,
        tokens: &mut Peekable<IntoIter<Token>>,
        query: &mut AlterTableQuery,
    ) -> Result<(), Errors> {
        let old_column_name = self.column_name(tokens)?;
        query.first_column = old_column_name;
        query.operation = Some(Operations::RENAME);
        self.to_keyword(tokens, query)
    }

    fn to_keyword(
        &self,
        tokens: &mut Peekable<IntoIter<Token>>,
        query: &mut AlterTableQuery,
    ) -> Result<(), Errors> {
        match get_next_value(tokens)? {
            Token::Reserved(keyword) if keyword == TO => {
                let second_column = self.column_name(tokens)?;
                query.second_column = second_column;
                Ok(())
            }
            _ => Err(Errors::SyntaxError(UNEXPECTED_TOKEN.to_string())),
        }
    }
    fn drop(
        &self,
        tokens: &mut Peekable<IntoIter<Token>>,
        query: &mut AlterTableQuery,
    ) -> Result<(), Errors> {
        let unwanted_column_name = self.column_name(tokens)?;
        query.first_column = unwanted_column_name;
        query.operation = Some(Operations::DROP);
        Ok(())
    }

    fn column_name(&self, tokens: &mut Peekable<IntoIter<Token>>) -> Result<String, Errors> {
        match get_next_value(tokens)? {
            Token::Identifier(column_name) => Ok(column_name),
            _ => Err(Errors::SyntaxError(UNEXPECTED_TOKEN.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parsers::tokens::{data_type::DataType, token::Token};
    const QUERY_LACKS_PARAMETERS: &str = "Query lacks parameters";
    const TABLE: &str = "TABLE";
    const TABLE_NAME: &str = "kp.table_name";
    const FIRST_COLUMN: &str = "first_column";
    const SECOND_COLUMN: &str = "second_column";
    #[test]
    fn test_01_alter_table_using_add_is_valid() {
        let tokens = vec![
            Token::Reserved(TABLE.to_string()),
            Token::Identifier(TABLE_NAME.to_string()),
            Token::Reserved(ADD.to_string()),
            Token::Identifier(FIRST_COLUMN.to_string()),
            Token::DataType(DataType::Text),
        ];

        let alter_table_parser = AlterTableParser::new();
        let result = alter_table_parser.parse(tokens);

        assert!(result.is_ok());
        let query = result.unwrap();

        assert_eq!(query.table_name, TABLE_NAME);
        assert_eq!(query.first_column, FIRST_COLUMN);
        assert_eq!(query.operation, Some(Operations::ADD));
    }
    #[test]
    fn test_02_alter_table_using_alter_is_valid() {
        let tokens = vec![
            Token::Reserved(TABLE.to_string()),
            Token::Identifier(TABLE_NAME.to_string()),
            Token::Reserved(ALTER.to_string()),
            Token::Identifier(FIRST_COLUMN.to_string()),
            Token::Reserved(TYPE.to_string()),
            Token::DataType(DataType::Text),
        ];

        let alter_table_parser = AlterTableParser::new();
        let result = alter_table_parser.parse(tokens);

        assert!(result.is_ok());
        let query = result.unwrap();

        assert_eq!(query.table_name, TABLE_NAME);
        assert_eq!(query.first_column, FIRST_COLUMN);
        assert_eq!(query.operation, Some(Operations::ALTER));
    }
    #[test]
    fn test_03_alter_table_using_rename_is_valid() {
        let tokens = vec![
            Token::Reserved(TABLE.to_string()),
            Token::Identifier(TABLE_NAME.to_string()),
            Token::Reserved(RENAME.to_string()),
            Token::Identifier(FIRST_COLUMN.to_string()),
            Token::Reserved(TO.to_string()),
            Token::Identifier(SECOND_COLUMN.to_string()),
        ];

        let alter_table_parser = AlterTableParser::new();
        let result = alter_table_parser.parse(tokens);

        assert!(result.is_ok());
        let query = result.unwrap();

        assert_eq!(query.table_name, TABLE_NAME);
        assert_eq!(query.first_column, FIRST_COLUMN);
        assert_eq!(query.second_column, SECOND_COLUMN);
        assert_eq!(query.operation, Some(Operations::RENAME));
    }
    #[test]
    fn test_04_alter_table_using_drop_is_valid() {
        let tokens = vec![
            Token::Reserved(TABLE.to_string()),
            Token::Identifier(TABLE_NAME.to_string()),
            Token::Reserved(DROP.to_string()),
            Token::Identifier(FIRST_COLUMN.to_string()),
        ];

        let alter_table_parser = AlterTableParser::new();
        let result = alter_table_parser.parse(tokens);

        assert!(result.is_ok());
        let query = result.unwrap();

        assert_eq!(query.table_name, TABLE_NAME);
        assert_eq!(query.first_column, FIRST_COLUMN);
        assert_eq!(query.operation, Some(Operations::DROP));
    }
    #[test]
    fn test_05_alter_table_missing_table_keyword_should_error() {
        let tokens = vec![
            Token::Identifier(TABLE_NAME.to_string()),
            Token::Reserved(ADD.to_string()),
            Token::Identifier(FIRST_COLUMN.to_string()),
            Token::DataType(DataType::Text),
        ];

        let alter_table_parser = AlterTableParser::new();
        let result = alter_table_parser.parse(tokens);

        assert!(result.is_err());

        if let Err(Errors::SyntaxError(msg)) = result {
            assert_eq!(msg, UNEXPECTED_TOKEN);
        } else {
            panic!("El error no es del tipo Errors::SyntaxError");
        }
    }
    #[test]
    fn test_06_alter_table_missing_table_name_should_error() {
        let tokens = vec![
            Token::Reserved(TABLE.to_string()),
            Token::Reserved(ADD.to_string()),
            Token::Identifier(FIRST_COLUMN.to_string()),
            Token::DataType(DataType::Text),
        ];

        let alter_table_parser = AlterTableParser::new();
        let result = alter_table_parser.parse(tokens);

        assert!(result.is_err());

        if let Err(Errors::SyntaxError(msg)) = result {
            assert_eq!(msg, UNEXPECTED_TOKEN);
        } else {
            panic!("El error no es del tipo Errors::SyntaxError");
        }
    }
    #[test]
    fn test_07_alter_table_missing_option_should_error() {
        let tokens = vec![
            Token::Reserved(TABLE.to_string()),
            Token::Identifier(TABLE_NAME.to_string()),
            Token::Identifier(FIRST_COLUMN.to_string()),
        ];

        let alter_table_parser = AlterTableParser::new();
        let result = alter_table_parser.parse(tokens);

        assert!(result.is_err());

        if let Err(Errors::SyntaxError(msg)) = result {
            assert_eq!(msg, MISSING_KEYWORD);
        } else {
            panic!("El error no es del tipo Errors::SyntaxError");
        }
    }

    #[test]
    fn test_08_alter_table_missing_type_keyword_in_alter_option_should_error() {
        let tokens = vec![
            Token::Reserved(TABLE.to_string()),
            Token::Identifier(TABLE_NAME.to_string()),
            Token::Reserved(ALTER.to_string()),
            Token::Identifier(FIRST_COLUMN.to_string()),
            Token::DataType(DataType::Text),
        ];

        let alter_table_parser = AlterTableParser::new();
        let result = alter_table_parser.parse(tokens);

        assert!(result.is_err());

        if let Err(Errors::SyntaxError(msg)) = result {
            assert_eq!(msg, UNEXPECTED_TOKEN);
        } else {
            panic!("El error no es del tipo Errors::SyntaxError");
        }
    }
    #[test]
    fn test_09_alter_table_missing_to_keyword_in_rename_option_should_error() {
        let tokens = vec![
            Token::Reserved(TABLE.to_string()),
            Token::Identifier(TABLE_NAME.to_string()),
            Token::Reserved(RENAME.to_string()),
            Token::Identifier(FIRST_COLUMN.to_string()),
            Token::Identifier(SECOND_COLUMN.to_string()),
        ];

        let alter_table_parser = AlterTableParser::new();
        let result = alter_table_parser.parse(tokens);

        assert!(result.is_err());

        if let Err(Errors::SyntaxError(msg)) = result {
            assert_eq!(msg, UNEXPECTED_TOKEN);
        } else {
            panic!("El error no es del tipo Errors::SyntaxError");
        }
    }
    #[test]
    fn test_10_alter_table_missing_column_in_drop_option_should_error() {
        let tokens = vec![
            Token::Reserved(TABLE.to_string()),
            Token::Identifier(TABLE_NAME.to_string()),
            Token::Reserved(DROP.to_string()),
        ];

        let alter_table_parser = AlterTableParser::new();
        let result = alter_table_parser.parse(tokens);

        assert!(result.is_err());

        if let Err(Errors::SyntaxError(msg)) = result {
            assert_eq!(msg, QUERY_LACKS_PARAMETERS);
        } else {
            panic!("El error no es del tipo Errors::SyntaxError");
        }
    }

    #[test]
    fn test_11_alter_table_missing_type_in_alter_option_should_error() {
        let tokens = vec![
            Token::Reserved(TABLE.to_string()),
            Token::Identifier(TABLE_NAME.to_string()),
            Token::Reserved(ALTER.to_string()),
            Token::Identifier(FIRST_COLUMN.to_string()),
            Token::Reserved(TYPE.to_string()),
        ];

        let alter_table_parser = AlterTableParser::new();
        let result = alter_table_parser.parse(tokens);

        assert!(result.is_err());

        if let Err(Errors::SyntaxError(msg)) = result {
            assert_eq!(msg, QUERY_LACKS_PARAMETERS);
        } else {
            panic!("El error no es del tipo Errors::SyntaxError");
        }
    }
    #[test]
    fn test_12_alter_table_missing_column_to_renmae_in_rename_option_should_error() {
        let tokens = vec![
            Token::Reserved(TABLE.to_string()),
            Token::Identifier(TABLE_NAME.to_string()),
            Token::Reserved(RENAME.to_string()),
            Token::Identifier(FIRST_COLUMN.to_string()),
            Token::Reserved(TO.to_string()),
        ];

        let alter_table_parser = AlterTableParser::new();
        let result = alter_table_parser.parse(tokens);

        assert!(result.is_err());

        if let Err(Errors::SyntaxError(msg)) = result {
            assert_eq!(msg, QUERY_LACKS_PARAMETERS);
        } else {
            panic!("El error no es del tipo Errors::SyntaxError");
        }
    }
}
