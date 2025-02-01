use std::collections::HashMap;

use crate::{utils::{errors::Errors, types::token_conversor::{create_reserved_token, create_identifier_token, create_symbol_token, create_token_from_literal, create_paren_list_token, create_iterate_list_token, create_comparison_operation_token, create_token_literal, create_logical_operation_token, create_brace_list_token, create_data_type_token}, constants::KEYSPACE_METADATA_PATH, parser_constants::COMMA}, data_access::{row::Row, column::Column}, queries::query::Query, parsers::{tokens::token::Token, query_parser::query_parser}, meta_data::meta_data_handler::use_keyspace_meta_data};
use crate::parsers::tokens::terms::ComparisonOperators::Equal;
use crate::parsers::tokens::terms::LogicalOperators::And;
use crate::parsers::tokens::data_type::DataType;
const COLON: &str = ":";
pub struct BuilderMessage;

impl BuilderMessage {
    pub fn build_insert(row: Row, table: String) -> Result<Box<dyn Query>, Errors> {
        let query = BuilderMessage::create_query_insert(row.columns, table)?;
        query_parser(query)
    }

    pub fn build_delete(row: Row, table: String) -> Result<Box<dyn Query>, Errors> {
        let query = BuilderMessage::create_query_delete(table, row)?;
        query_parser(query)
    }

    pub fn build_keyspace(keyspace: String) -> Result<Box<dyn Query>, Errors> {
        let query = BuilderMessage::create_query_keyspace(keyspace)?;
        query_parser(query)
    }

    //path = keyspace.table
    pub fn build_table(path: String) -> Result<Box<dyn Query>, Errors> {
        let query = BuilderMessage::create_query_table(path)?;
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

    fn create_query_delete(table: String, row: Row) -> Result<Vec<Token>, Errors> {
        let mut query: Vec<Token> = Vec::new();
        query.push(create_reserved_token("DELETE"));
        query.push(create_reserved_token("FROM"));
        query.push(create_identifier_token(&table));
        BuilderMessage::add_where(&mut query, &row)?;
        Ok(query)
    }

    fn add_where(query: &mut Vec<Token>, row: &Row) -> Result<(), Errors> {
        query.push(create_reserved_token("WHERE"));
        let mut sub_where: Vec<Token> = Vec::new();
        for (i, column) in row.columns.clone().into_iter().enumerate(){
            sub_where.push(create_identifier_token(&column.column_name));
            sub_where.push(create_comparison_operation_token(Equal));
            sub_where.push(create_token_literal(
                &column.value.value,
                column.value.data_type.clone(),
            ));
            if i < row.columns.len() - 1 {
                sub_where.push(create_logical_operation_token(And))
            }
        }
        query.push(create_iterate_list_token(sub_where));
        Ok(())
    }

    fn create_query_keyspace(keyspace: String) -> Result<Vec<Token>, Errors> {
        let query: Vec<Token> = vec![
            create_reserved_token("CREATE"),
            create_reserved_token("KEYSPACE"),
            create_identifier_token(&keyspace),
            create_reserved_token("WITH"),
            create_reserved_token("REPLICATION"),
            create_comparison_operation_token(Equal),
            brace_list(&keyspace)?
        ];
        Ok(query)
    }

    fn create_query_table(table: String) -> Result<Vec<Token>, Errors> {
        let query: Vec<Token> = vec![
            create_reserved_token("CREATE"),
            create_reserved_token("TABLE"),
            create_identifier_token(&table),
            paren_list(&table)?
        ];
        Ok(query)
    }

}


fn brace_list(keyspace: &str) -> Result<Token,Errors> {
    let (replication, strategy) = get_meta_data_keyspace(keyspace)?;
    let list: Vec<Token> = vec![
        create_token_literal("class", DataType::Text),
        create_symbol_token(COLON),
        create_token_literal(&strategy, DataType::Text),

        create_symbol_token(COMMA),

        create_token_literal("replication_factor", DataType::Text),
        create_symbol_token(COLON),
        create_token_literal(&replication, DataType::Int)
    ];
    

    Ok(create_brace_list_token(list))
}

fn get_meta_data_keyspace(keyspace: &str) -> Result<(String, String), Errors> {
    let replication = use_keyspace_meta_data(|handler| {
        handler.get_replication( KEYSPACE_METADATA_PATH.to_owned(), keyspace)
    })?;
    let strategy = use_keyspace_meta_data(|handler| {
        handler.get_strategy(KEYSPACE_METADATA_PATH.to_owned(), keyspace)
    })?;
    Ok((replication.to_string(), strategy))
}


fn paren_list(path: &str) -> Result<Token, Errors> {
    let (keyspace, table) = path.split_once('.').ok_or_else(|| Errors::ServerError("Failed to read keyspace.table".to_string()))?;
    let (pks, columns) = get_meta_data_table(keyspace, table)?;
    let mut list: Vec<Token> = Vec::new();
    for column in columns {
        list.push(create_identifier_token(&column.0));
        list.push(create_data_type_token(column.1));
        list.push(create_symbol_token(COMMA));
    }
    list.push(create_reserved_token("PRIMARY"));
    list.push(create_reserved_token("KEY"));
    list.push(sub_list(pks));
    Ok(create_paren_list_token(list))
}


fn sub_list(pks: Vec<String>) -> Token {
    let mut list: Vec<Token> = Vec::new();
    for (i, pk) in pks.clone().into_iter().enumerate() {
        list.push(create_identifier_token(&pk));

        if i < pks.len() - 1 {
            list.push(create_symbol_token(COMMA));
        }
    }
    create_paren_list_token(list)
}


fn get_meta_data_table(keyspace: &str, table: &str) -> Result<(Vec<String>, HashMap<String, DataType>), Errors> {
    let header_pks = use_keyspace_meta_data(|handler| {
        handler.get_primary_key( KEYSPACE_METADATA_PATH.to_owned(), keyspace, table)
    })?;
    let columns = use_keyspace_meta_data(|handler| {
        handler.get_columns_type( KEYSPACE_METADATA_PATH.to_owned(), keyspace, table)
    })?;
    Ok((header_pks.get_full_primary_key(), columns))
}