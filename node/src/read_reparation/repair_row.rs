
use crate::parsers::tokens::terms::ComparisonOperators::Equal;
use crate::parsers::tokens::terms::LogicalOperators::And;
use crate::utils::types::token_conversor::{create_paren_list_token, create_symbol_token};
use crate::{
    data_access::{column::Column, row::Row},
    parsers::tokens::{literal::Literal, token::Token},
    utils::{
        errors::Errors,
        types::token_conversor::{
            create_comparison_operation_token, create_identifier_token, create_iterate_list_token,
            create_logical_operation_token, create_reserved_token, create_token_from_literal,
            create_token_literal,
        },
    },
};

use super::utils::to_hash_columns;

pub struct RepairRow {
    keyspace: String,
    table: String,
    pks: Vec<String>,
}

impl RepairRow {
    pub fn new() -> Self {
        RepairRow {
            keyspace: String::new(),
            table: String::new(),
            pks: Vec::new(),
        }
    }

    pub fn initializer(&mut self, keyspace: String, table: String, pks: Vec<String>) {
        self.keyspace = keyspace;
        self.table = table;
        self.pks = pks;
    }

    fn create_base_update(&self, query: &mut Vec<Token>) -> Result<(), Errors> {
        query.push(create_reserved_token("UPDATE"));
        query.push(create_identifier_token(&format!(
            "{}.{}",
            self.keyspace, self.table
        )));
        query.push(create_reserved_token("SET"));
        Ok(())
    }

    fn add_update_changes(identifier: &str, literal: &Literal) -> Vec<Token> {
        let mut res: Vec<Token> = Vec::new(); 
        res.push(create_identifier_token(identifier));
        res.push(create_comparison_operation_token(Equal));
        res.push(create_token_from_literal(literal.clone()));
        res.push(create_symbol_token(","));
        res
    }

    fn create_update_changes(
        &self,
        query: &mut Vec<Token>,
        best_column: Vec<Column>,
        node_columns: Vec<Column>,
    ) -> Result<bool, Errors> {
        let mut change_row = false;
        self.create_base_update(query)?;
        let best_col_map = to_hash_columns(best_column);
        let mut changes: Vec<Token> = Vec::new(); 
        for column in node_columns {
            if let Some(best_column) = best_col_map.get(&column.column_name) {
                if column.value.value != best_column.value.value {
                    changes.append(&mut Self::add_update_changes(&column.column_name, &best_column.value));
                    change_row = true
                }
            }
        }
        query.push(create_iterate_list_token(changes));
        Ok(change_row)
    }

    fn create_base_delete(&self, query: &mut Vec<Token>) -> Result<(), Errors> {
        query.push(create_reserved_token("DELETE"));
        query.push(create_reserved_token("FROM"));
        query.push(create_identifier_token(&format!(
            "{}.{}",
            self.keyspace, self.table
        )));
        Ok(())
    }

    fn create_base_insert(
        &self,
        query: &mut Vec<Token>,
        columns: Vec<Column>,
    ) -> Result<(), Errors> {
        query.push(create_reserved_token("INSERT"));
        query.push(create_reserved_token("INTO"));
        query.push(create_identifier_token(&format!(
            "{}.{}",
            self.keyspace, self.table
        )));
        let mut values: Vec<Token> = Vec::new();
        let mut headers: Vec<Token> = Vec::new();
        for best_column in columns {
            headers.push(create_identifier_token(&best_column.column_name));
            headers.push(create_symbol_token(","));
            values.push(create_token_from_literal(best_column.value));
            values.push(create_symbol_token(","));
        }
        query.push(create_paren_list_token(headers));

        query.push(create_reserved_token("VALUES"));

        query.push(create_paren_list_token(values));
        Ok(())
    }

    fn add_where(&self, query: &mut Vec<Token>, row: &Row) -> Result<(), Errors> {
        let columns = to_hash_columns(row.columns.clone());
        query.push(create_reserved_token("WHERE"));
        let mut sub_where: Vec<Token> = Vec::new();

        for (i, pk_header) in self.pks.clone().into_iter().enumerate() {
            if let Some(column) = columns.get(&pk_header) {
                sub_where.push(create_identifier_token(&pk_header));
                sub_where.push(create_comparison_operation_token(Equal));
                sub_where.push(create_token_literal(
                    &column.value.value,
                    column.value.data_type.clone(),
                ));
                if i < self.pks.len() - 1 {
                    sub_where.push(create_logical_operation_token(And))
                }
            }
        }
        query.push(create_iterate_list_token(sub_where));
        Ok(())
    }

    pub fn create_insert(&self, row: &Row) -> Result<Vec<Token>, Errors> {
        let mut query = Vec::new();
        self.create_base_insert(&mut query, row.columns.clone())?;
        Ok(query)
    }

    pub fn repair_row(&self, best_row: Row, node_row: Row) -> Result<(bool, Vec<Token>), Errors> {
        let mut query = Vec::new();
        let change_row = match (best_row.is_deleted(), node_row.is_deleted()) {
            (true, false) | (true, true) => {
                // Delete en nodo o limpieza
                self.create_base_delete(&mut query)?;
                self.add_where(&mut query, &node_row)?;
                true
            }
            (false, true) => {
                // Insert al nodo
                self.create_base_insert(&mut query, best_row.columns.clone())?;
                true
            }
            (false, false) => {
                
                // Actualización si difieren valores
                if self.create_update_changes(
                    &mut query,
                    best_row.columns.clone(),
                    node_row.columns.clone(),
                )? {
                    self.add_where(&mut query, &node_row)?;
                    true
                } else {
                    false
                }
            }
        };
        Ok((change_row, query))
    }
}

impl Default for RepairRow {
    fn default() -> Self {
            Self::new()
        }
    }

#[cfg(test)]
mod tests {
    use crate::parsers::tokens::{literal::create_literal, data_type::DataType};

    use super::*;
    


    fn create_column(name: &str, value: Literal) -> Column {
        Column::new(&name.to_string(), &value)
    }

    fn create_row(columns: Vec<Column>, pks: Vec<String> ,deleted: bool) -> Row {
        let mut res = Row::new(columns, pks);
        res.deleted = deleted;
        res
    }

    #[test]
    fn test_create_insert() {
        let mut repair_row = RepairRow::new();
        repair_row.initializer(
            "test_keyspace".to_string(),
            "test_table".to_string(),
            vec!["pk1".to_string(), "pk2".to_string()],
        );

        let row = create_row(
            vec![
                create_column("pk1", create_literal("1", DataType::Int)),
                create_column("pk2", create_literal("2", DataType::Int)),
                create_column("value", create_literal("abc", DataType::Text)),
            ],
            vec!["pk1".to_string(), "pk2".to_string()],
            false,
        );

        let query = repair_row.create_insert(&row).unwrap();
        let expected_tokens = vec![
            create_reserved_token("INSERT"),
            create_reserved_token("INTO"),
            create_identifier_token("test_keyspace.test_table"),
            create_paren_list_token(vec![
                create_identifier_token("pk1"),
                create_symbol_token(","),
                create_identifier_token("pk2"),
                create_symbol_token(","),
                create_identifier_token("value"),
                create_symbol_token(","),
            ]),
            
            create_reserved_token("VALUES"),
            create_paren_list_token(vec![
                create_token_literal("1", DataType::Int),
                create_symbol_token(","),
                create_token_literal("2", DataType::Int),
                create_symbol_token(","),
                create_token_literal("abc", DataType::Text),
                create_symbol_token(","),
            ]),
        ];
        assert_eq!(query, expected_tokens);
    }

    #[test]
    fn test_repair_row_delete() {
        let mut repair_row = RepairRow::new();
        repair_row.initializer(
            "test_keyspace".to_string(),
            "test_table".to_string(),
            vec!["pk1".to_string()],
        );

        let best_row = create_row(
            vec![create_column("pk1", create_literal("1", DataType::Int))],
            vec!["pk1".to_string()],
            true,
        ); //deleted row
        let node_row = create_row(
            vec![create_column("pk1", create_literal("1", DataType::Int))],
            vec!["pk1".to_string()],
            false,
        ); // Existing row

        let (change_row, query) = repair_row.repair_row(best_row, node_row).unwrap();
        let expected_tokens = vec![
            create_reserved_token("DELETE"),
            create_reserved_token("FROM"),
            create_identifier_token("test_keyspace.test_table"),
            create_reserved_token("WHERE"),
            create_iterate_list_token(vec![
                create_identifier_token("pk1"),
                create_comparison_operation_token(Equal),
                create_token_literal("1", DataType::Int),
            ]),
        ];
        assert!(change_row);
        assert_eq!(query, expected_tokens);
    }

    #[test]
    fn test_repair_row_insert() {
        let mut repair_row = RepairRow::new();
        repair_row.initializer(
            "test_keyspace".to_string(),
            "test_table".to_string(),
            vec!["pk1".to_string()],
        );

        let best_row = create_row(
            vec![
                create_column("pk1", create_literal("1", DataType::Int)),
                create_column("value", create_literal("abc", DataType::Text)),
            ],
            vec!["pk1".to_string()],
            false,
        ); // Non-deleted row
        let node_row = create_row(
            vec![
                create_column("pk1", create_literal("1", DataType::Int)),
                create_column("value", create_literal("abc", DataType::Text)),
            ],
            vec!["pk1".to_string()],
            true,
        ); // Deleted row

        let (change_row, query) = repair_row.repair_row(best_row, node_row).unwrap();
        let expected_tokens = vec![
            create_reserved_token("INSERT"),
            create_reserved_token("INTO"),
            create_identifier_token("test_keyspace.test_table"),
            create_paren_list_token(vec![
                create_identifier_token("pk1"),
                create_symbol_token(","),
                create_identifier_token("value"),
                create_symbol_token(","),
            ]),
            
            create_reserved_token("VALUES"),
            create_paren_list_token(vec![
                create_token_literal("1", DataType::Int),
                create_symbol_token(","),
                create_token_literal("abc", DataType::Text),
                create_symbol_token(","),
            ]),
        ];

        assert!(change_row);
        assert_eq!(query, expected_tokens);
    }

    #[test]
    fn test_repair_row_update() {
        let mut repair_row = RepairRow::new();
        repair_row.initializer(
            "test_keyspace".to_string(),
            "test_table".to_string(),
            vec!["pk1".to_string()],
        );

        let best_row = create_row(
            vec![
                create_column("pk1", create_literal("1", DataType::Int)),
                create_column("value", create_literal("new", DataType::Text)),
            ],
            vec!["pk1".to_string()],
            false,
        ); // Updated row
        let node_row = create_row(
            vec![
                create_column("pk1", create_literal("1", DataType::Int)),
                create_column("value", create_literal("old", DataType::Text)),
            ],
            vec!["pk1".to_string()],
            false,
        ); // Existing row with different value

        let (change_row, query) = repair_row.repair_row(best_row, node_row).unwrap();
        let expected_tokens = vec![
            create_reserved_token("UPDATE"),
            create_identifier_token("test_keyspace.test_table"),
            create_reserved_token("SET"),
            create_iterate_list_token(vec![
                create_identifier_token("value"),
                create_comparison_operation_token(Equal),
                create_token_literal("new", DataType::Text),
                create_symbol_token(","),
            ]),
            create_reserved_token("WHERE"),
            create_iterate_list_token(vec![
                create_identifier_token("pk1"),
                create_comparison_operation_token(Equal),
                create_token_literal("1", DataType::Int),
            ]),
        ];

        assert!(change_row);
        assert_eq!(query, expected_tokens)
    }
}

