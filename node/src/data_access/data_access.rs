use crate::data_access::row::{Column, Row};
use crate::parsers::tokens::data_type::DataType;
use crate::parsers::tokens::literal::Literal;
use crate::parsers::tokens::terms::ArithMath;
use crate::queries::evaluate::Evaluate;
use crate::queries::order_by_clause::OrderByClause;
use crate::queries::set_logic::assigmente_value::AssignmentValue;
use crate::queries::where_logic::where_clause::WhereClause;
use crate::utils::constants::ASC;
use crate::utils::errors::Errors;
use crate::utils::functions::{get_int_from_string, get_timestamp};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{metadata, remove_file, rename, File, OpenOptions};
use std::io::{BufReader, Seek, SeekFrom, Write};

#[derive(Debug, Serialize, Deserialize)]
pub struct DataAccess;

impl DataAccess {
    pub fn create_table(&self, table_name: &String) -> Result<(), Errors> {
        let path = self.get_file_path(table_name);
        if metadata(&path).is_ok() {
            return Err(Errors::AlreadyExists("Table already exists".to_string()));
        }
        self.create_file(&path)?;
        Ok(())
    }

    fn create_file(&self, path: &String) -> Result<(), Errors> {
        let mut file = File::create(path)
            .map_err(|_| Errors::ServerError(String::from("Could not create file")))?;
        file.write_all(b"[]")
            .map_err(|_| Errors::ServerError(String::from("Could not initialize table file")))?;
        Ok(())
    }

    pub fn truncate_table(&self, table_name: &String) -> Result<(), Errors> {
        let _file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(self.get_file_path(table_name))
            .map_err(|_| Errors::ServerError(String::from("Could not open file")))?;
        Ok(())
    }

    pub fn drop_table(&self, table_name: String) -> Result<(), Errors> {
        remove_file(self.get_file_path(&table_name))
            .map_err(|_| Errors::ServerError(String::from("Could not remove file")))?;
        Ok(())
    }

    pub fn insert(&self, table_name: &String, row: &Row) -> Result<(), Errors> {
        let path = self.get_file_path(table_name);
        if self.pk_already_exists(&path, &row.primary_key)? {
            return Err(Errors::AlreadyExists(
                "Primary key already exists".to_string(),
            ));
        }
        self.append_row(&path, row)
    }

    pub fn set_deleted_rows(
        &self,
        table_name: &String,
        where_clause: &WhereClause,
    ) -> Result<(), Errors> {
        let path = self.get_file_path(table_name);
        let temp_path = format!("{}.tmp", path);
        self.create_file(&temp_path)?;
        for row in self.get_deserialized_stream(&path)? {
            if where_clause.evaluate(&row.get_row_hash())? {
                self.append_row(&temp_path, &Row::new_deleted_row()?)?;
            } else {
                self.append_row(&temp_path, &row)?;
            }
        }
        rename(temp_path, path)
            .map_err(|_| Errors::ServerError(String::from("Error renaming file")))?;
        Ok(())
    }

    pub fn update_row(
        &self,
        table_name: &String,
        changes: &HashMap<String, AssignmentValue>,
        where_clause: &WhereClause,
    ) -> Result<(), Errors> {
        let path = self.get_file_path(table_name);
        let temp_path = format!("{}.tmp", path);
        self.create_file(&temp_path)?;
        for row in self.get_deserialized_stream(&path)? {
            if where_clause.evaluate(&row.get_row_hash())? {
                self.append_row(&temp_path, &self.build_updated_row(&row, changes)?)?;
            } else {
                self.append_row(&temp_path, &row)?;
            }
        }
        rename(temp_path, path)
            .map_err(|_| Errors::ServerError(String::from("Error renaming file")))?;
        Ok(())
    }

    fn build_updated_row(
        &self,
        row: &Row,
        changes: &HashMap<String, AssignmentValue>,
    ) -> Result<Row, Errors> {
        let mut new_columns = Vec::new();
        for column in &row.columns {
            if !changes.contains_key(&column.column_name) {
                new_columns.push(Column::new_from_column(column))
            } else {
                new_columns.push(Column::new_from_column(&self.get_updated_column(row, changes, column)?))
            }
        }
        Ok(Row::new(
            new_columns,
            Vec::from(row.primary_key.as_slice()),
        ))
    }

    fn get_updated_column(
        &self,
        row: &Row,
        changes: &HashMap<String, AssignmentValue>,
        actual_column: &Column,
    ) -> Result<Column, Errors> {
        let column_name = &actual_column.column_name;
        match changes.get(column_name) {
            Some(AssignmentValue::Column(column)) => Ok(Column::new(
                column_name,
                &row.get_some_column(column)?.value,
                get_timestamp()?,
            )),
            Some(AssignmentValue::Simple(literal)) => Ok(Column::new(
                column_name,
                literal,
                get_timestamp()?,
            )),
            Some(AssignmentValue::Arithmetic(column, arith, literal)) => {
                let value1 = get_int_from_string(&row.get_some_column(column)?.value.value)?;
                let value2 = get_int_from_string(&literal.value)?;
                let new_value = match arith {
                    ArithMath::Suma => value1 + value2,
                    ArithMath::Sub => value1 - value2,
                    ArithMath::Division => value1 / value2,
                    ArithMath::Rest => value1 % value2,
                    ArithMath::Multiplication => value1 * value2,
                };
                Ok(Column::new(
                    column_name,
                    &Literal::new(new_value.to_string(), DataType::Int),
                    get_timestamp()?,
                ))
            }
            _ => Err(Errors::ServerError(String::from("Column not found"))),
        }
    }

    pub fn select_rows(
        &self,
        table_name: &String,
        where_clause: &WhereClause,
        order_clauses: &Option<Vec<OrderByClause>>,
    ) -> Result<Vec<Row>, Errors> {
        let path = self.get_file_path(table_name);
        let filtered_path = self.get_file_path(&String::from("filtered"));
        self.create_file(&filtered_path)?;
        self.filter_rows(&path, &filtered_path, where_clause)?;
        if self.rows_count(&filtered_path)? > 1 {
            self.sort_rows(&filtered_path, order_clauses)?;
        }
        let rows = self.get_rows(&filtered_path)?;
        remove_file(filtered_path)
            .map_err(|_| Errors::ServerError(String::from("Could not remove file")))?;
        Ok(rows)
    }

    fn filter_rows(
        &self,
        path: &String,
        filtered_path: &String,
        where_clause: &WhereClause,
    ) -> Result<(), Errors> {
        for row in self.get_deserialized_stream(path)? {
            if where_clause.evaluate(&row.get_row_hash())? {
                self.append_row(filtered_path, &row)?;
            }
        }
        Ok(())
    }

    fn sort_rows(
        &self,
        path: &String,
        order_clauses_opt: &Option<Vec<OrderByClause>>,
    ) -> Result<(), Errors> {
        let Some(order_clauses) = order_clauses_opt else {
            return Ok(());
        };
        for order in order_clauses.iter().rev() {
            self.bubble_sort_file(path, order)?
        }
        Ok(())
    }

    fn bubble_sort_file(
        &self,
        path: &String,
        order_by_clause: &OrderByClause,
    ) -> Result<(), Errors> {
        let rows_count = self.rows_count(path)?;
        for n in 0..(rows_count - 1) {
            let temp_path = format!("{}.tmp", path);
            self.create_file(&temp_path)?;
            let mut rows = self.get_deserialized_stream(path)?;
            let mut actual_row = self.get_next_line(&mut rows)?;
            for _ in 0..(rows_count - n - 1) {
                if let Ok(next_row) = self.get_next_line(&mut rows) {
                    if self.should_swap_rows(&actual_row, &next_row, order_by_clause)? {
                        self.append_row(&temp_path, &next_row)?;
                    } else {
                        self.append_row(&temp_path, &actual_row)?;
                        actual_row = next_row
                    }
                }
            }
            self.append_row(&temp_path, &actual_row)?;
            for row in rows {
                self.append_row(&temp_path, &row)?;
            }
            rename(temp_path, path)
                .map_err(|_| Errors::ServerError(String::from("Error renaming file")))?;
        }
        Ok(())
    }

    fn should_swap_rows(
        &self,
        actual: &Row,
        next: &Row,
        order_by_clause: &OrderByClause,
    ) -> Result<bool, Errors> {
        if order_by_clause.order == ASC {
            return Ok(Row::cmp(actual, next, &order_by_clause.column) > 0);
        }
        Ok(Row::cmp(actual, next, &order_by_clause.column) < 0)
    }

    fn get_next_line(&self, rows: &mut impl Iterator<Item = Row>) -> Result<Row, Errors> {
        let Some(row) = rows.next() else {
            return Err(Errors::ServerError(String::from("Error deserializing row")));
        };
        Ok(row)
    }

    fn get_rows(&self, path: &String) -> Result<Vec<Row>, Errors> {
        let mut rows = Vec::new();
        for row in self.get_deserialized_stream(path)? {
            rows.push(row);
        }
        Ok(rows)
    }

    fn get_file_path(&self, table_name: &String) -> String {
        format!("src/data_access/{}.json", table_name)
    }

    fn open_file(&self, path: &String) -> Result<File, Errors> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(|_| Errors::ServerError("Failed to open table file".to_string()))?;
        Ok(file)
    }

    fn append_row(&self, path: &String, row: &Row) -> Result<(), Errors> {
        let mut file = self.open_file(path)?;
        let file_size = file
            .seek(SeekFrom::End(0))
            .map_err(|_| Errors::ServerError("Failed to seek in file".to_string()))?;
        if file_size > 2 {
            file.seek(SeekFrom::End(-1))
                .map_err(|_| Errors::ServerError("Failed to seek in file".to_string()))?;
            file.write_all(b",").map_err(|_| {
                Errors::ServerError("Failed to append row to table file".to_string())
            })?;
        } else {
            file.seek(SeekFrom::End(-1))
                .map_err(|_| Errors::ServerError("Failed to seek in file".to_string()))?;
        }
        let json_row = serde_json::to_string(&row)
            .map_err(|_| Errors::ServerError("Failed to serialize row".to_string()))?;
        file.write_all(json_row.as_bytes())
            .map_err(|_| Errors::ServerError("Failed to write row to file".to_string()))?;

        // Cerramos el array JSON
        file.write_all(b"]")
            .map_err(|_| Errors::ServerError("Failed to close JSON array".to_string()))?;

        Ok(())
    }

    fn pk_already_exists(&self, path: &String, primary_keys: &Vec<String>) -> Result<bool, Errors> {
        for row in self.get_deserialized_stream(path)? {
            if &row.primary_key == primary_keys {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn get_deserialized_stream(&self, path: &String) -> Result<impl Iterator<Item = Row>, Errors> {
        let file = self.open_file(path)?;
        let reader = BufReader::new(file);
        let rows: Vec<Row> =
            serde_json::from_reader(reader).map_err(|e| Errors::ServerError(e.to_string()))?;
        Ok(rows.into_iter())
    }

    fn rows_count(&self, path: &String) -> Result<usize, Errors> {
        Ok(self.get_deserialized_stream(path)?.count())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_access::row::Column;
    use crate::parsers::tokens::data_type::DataType;
    use crate::parsers::tokens::literal::Literal;
    use crate::parsers::tokens::terms::ComparisonOperators;
    use crate::queries::where_logic::comparison::ComparisonExpr;
    use std::fs::read_to_string;
    use std::path::Path;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    static TABLE_COUNTER: AtomicUsize = AtomicUsize::new(1);
    static TABLE_MUTEX: Mutex<()> = Mutex::new(());

    fn get_unique_table_name() -> String {
        let count = TABLE_COUNTER.fetch_add(1, Ordering::SeqCst);
        format!("test_table{}", count)
    }

    #[test]
    fn test_create_table_success() {
        let _lock = TABLE_MUTEX.lock();
        let data_access = DataAccess {};
        let table_name = get_unique_table_name();
        let result = data_access.create_table(&table_name);
        assert!(result.is_ok());

        let table_path = data_access.get_file_path(&table_name);
        let file_content = read_to_string(&table_path).unwrap();
        assert_eq!(file_content, "[]");
        assert!(Path::new(&table_path).exists());
        remove_file(data_access.get_file_path(&table_name)).unwrap();
    }

    #[test]
    fn test_create_table_already_exists() {
        let _lock = TABLE_MUTEX.lock();
        let data_access = DataAccess {};
        let table_name = get_unique_table_name();
        assert!(data_access.create_table(&table_name).is_ok());
        let result = data_access.create_table(&table_name);
        assert!(result.is_err());
        let expected = Err(Errors::AlreadyExists(String::from("Table already exists")));
        assert_eq!(result, expected);
        remove_file(data_access.get_file_path(&table_name)).unwrap();
    }

    #[test]
    fn test_alter_table_success() {
        let _lock = TABLE_MUTEX.lock();
        let data_access = DataAccess {};
        let table_name = get_unique_table_name();
        data_access.create_table(&table_name).unwrap();
        let result = data_access.truncate_table(&table_name);
        assert!(result.is_ok());
        remove_file(data_access.get_file_path(&table_name)).unwrap();
    }

    #[test]
    fn test_drop_table_success() {
        let _lock = TABLE_MUTEX.lock();
        let data_access = DataAccess {};
        let table_name = get_unique_table_name();
        data_access.create_table(&table_name).unwrap();
        let result = data_access.drop_table(table_name.clone());
        assert!(result.is_ok());
    }

    fn get_row1() -> Row {
        Row::new(
            vec![Column {
                column_name: "name".to_string(),
                value: Literal {
                    value: "John".to_string(),
                    data_type: DataType::Text,
                },
                time_stamp: "2024-10-22".to_string(),
            }],
            vec!["name".to_string()],
        )
    }

    fn get_row1_in_string() -> Result<String, Errors> {
        serde_json::to_string(&get_row1())
            .map_err(|_| Errors::ServerError("Failed to serialize row1".to_string()))
    }

    fn get_assignment() -> HashMap<String, AssignmentValue> {
        let mut assignments = HashMap::new();
        assignments.insert("name".to_string(), AssignmentValue::Simple(Literal::new("Jane".to_string(), DataType::Text)));
        assignments
    }
    fn get_row3() -> Row {
        Row::new(
            vec![Column {
                column_name: "name".to_string(),
                value: Literal {
                    value: "Jane".to_string(),
                    data_type: DataType::Text,
                },
                time_stamp: "2024-10-23".to_string(),
            }],
            vec!["_".to_string()],
        )
    }

    fn get_updated_string() -> String {
        "{\"columns\":[{\"column_name\":\"name\",\"value\":{\"value\":\"Jane\",\"data_type\":\"Text\"}".to_string()
    }

    #[test]
    fn test_insert_row_success() {
        let _lock = TABLE_MUTEX.lock();
        let data_access = DataAccess {};
        let table_name = get_unique_table_name();
        data_access.create_table(&table_name).unwrap();

        let row = get_row1();

        let result = data_access.insert(&table_name, &row);
        assert!(result.is_ok());
        let table_path = data_access.get_file_path(&table_name);
        let file_content = read_to_string(&table_path).unwrap();
        assert!(file_content.contains(get_row1_in_string().unwrap().as_str()));
        remove_file(table_path).unwrap();
    }

    #[test]
    fn test_insert_row_pk_already_exists() {
        let _lock = TABLE_MUTEX.lock();
        let data_access = DataAccess {};
        let table_name = get_unique_table_name();
        data_access.create_table(&table_name).unwrap();

        let row = get_row1();

        data_access.insert(&table_name, &row).unwrap();
        let result = data_access.insert(&table_name, &row);
        assert!(matches!(result, Err(Errors::AlreadyExists(_))));
        remove_file(data_access.get_file_path(&table_name)).unwrap();
    }

    #[test]
    fn test_update_row_success() {
        let _lock = TABLE_MUTEX.lock();
        let data_access = DataAccess {};
        let table_name = get_unique_table_name();
        data_access.create_table(&table_name).unwrap();

        let row1 = get_row1();
        data_access.insert(&table_name, &row1).unwrap();

        let literal = Literal {
            value: "John".to_string(),
            data_type: DataType::Text,
        };
        let where_clause = WhereClause::Comparison(ComparisonExpr::new(
            "name".to_string(),
            &ComparisonOperators::Equal,
            literal,
        ));

        let result = data_access.update_row(&table_name, &get_assignment(), &where_clause);
        assert!(result.is_ok());
        let table_path = data_access.get_file_path(&table_name);
        let file_content = read_to_string(&table_path).unwrap();
        assert!(file_content.contains(get_updated_string().as_str()));
        assert!(!file_content.contains(get_row1_in_string().unwrap().as_str()));

        remove_file(table_path).unwrap();
    }

    #[test]
    fn test_select_rows_success() {
        let _lock = TABLE_MUTEX.lock();
        let data_access = DataAccess {};
        let table_name = get_unique_table_name();
        data_access.create_table(&table_name).unwrap();

        let row1 = get_row1();
        let row3 = get_row3();
        data_access.insert(&table_name, &row1).unwrap();
        data_access.insert(&table_name, &row3).unwrap();

        let literal = Literal {
            value: "John".to_string(),
            data_type: DataType::Text,
        };
        let where_clause = WhereClause::Comparison(ComparisonExpr::new(
            "name".to_string(),
            &ComparisonOperators::Equal,
            literal,
        ));
        let result = data_access.select_rows(&table_name, &where_clause, &None);
        assert!(result.is_ok());
        let selected_rows = result.unwrap();
        assert_eq!(selected_rows.len(), 1);
        assert_eq!(selected_rows[0], row1);
        remove_file(data_access.get_file_path(&table_name)).unwrap();
    }
}
