use std::{iter::Peekable, vec::IntoIter};

use crate::{
    parsers::tokens::token::Token,
    queries::alter_table_query::{AlterTableQuery, Operations},
    utils::errors::Errors,
};

use crate::utils::token_conversor::get_next_value;

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

impl AlterTableParser {
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
