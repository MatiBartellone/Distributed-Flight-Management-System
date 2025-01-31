use crate::{utils::{errors::Errors, types::token_conversor::{create_reserved_token, create_identifier_token, create_symbol_token, create_token_from_literal, create_paren_list_token, create_iterate_list_token, create_comparison_operation_token, create_token_literal, create_logical_operation_token}}, data_access::{row::Row, column::{Column, self}}, queries::query::Query, parsers::{tokens::token::Token, query_parser::query_parser}, read_reparation::utils::to_hash_columns};
use crate::parsers::tokens::terms::ComparisonOperators::Equal;
use crate::parsers::tokens::terms::LogicalOperators::And;
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
}