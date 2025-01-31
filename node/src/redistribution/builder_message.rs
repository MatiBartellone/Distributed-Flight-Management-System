use crate::{utils::{errors::Errors, types::token_conversor::{create_reserved_token, create_identifier_token, create_symbol_token, create_token_from_literal, create_paren_list_token}}, data_access::{row::Row, column::Column}, queries::query::Query, parsers::{tokens::token::Token, query_parser::query_parser}};

pub struct BuilderMessage;

impl BuilderMessage {
    fn build_insert(row: Row, table: String) -> Result<Box<dyn Query>, Errors> {
        let query = BuilderMessage::create_query_insert(row.columns, table)?;
        query_parser(query)
    }


    fn create_query_insert(
        columns: Vec<Column>,
        table: String,
    ) -> Result<Vec<Token>, Errors> {
        let mut query: Vec<Token> = Vec::new();
        query.push(create_reserved_token("INSERT"));
        query.push(create_reserved_token("INTO"));
        query.push(create_identifier_token(&table));
        let mut values: Vec<Token> = Vec::new();
        let mut headers: Vec<Token> = Vec::new();
        for column in columns {
            headers.push(create_identifier_token(&column.column_name));
            headers.push(create_symbol_token(","));
            values.push(create_token_from_literal(column.value));
            values.push(create_symbol_token(","));
        }
        query.push(create_paren_list_token(headers));

        query.push(create_reserved_token("VALUES"));

        query.push(create_paren_list_token(values));
        Ok(query)
    }
}