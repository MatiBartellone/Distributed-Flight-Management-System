use crate::data_access::row::Row;
use crate::utils::errors::Errors;
use std::fs::{remove_file, rename, File, OpenOptions};
use std::io::{BufReader, Seek, SeekFrom, Write};
use serde_json::de::IoRead;
use serde_json::StreamDeserializer;
use crate::queries::evaluate::Evaluate;
use crate::queries::order_by_clause::OrderByClause;
use crate::queries::where_logic::where_clause::WhereClause;

pub struct DataAccess {}

impl DataAccess {
    pub fn new() -> DataAccess {
        DataAccess {}
    }

    pub fn create_table(&self, table_name: &String) -> Result<(), Errors> {
        let path = self.get_file_path(table_name);
        let file = self.open_file(&path);
        if file.is_ok() {
            return Err(Errors::AlreadyExists("Table already exists".to_string()));
        }
        File::create(path)
            .map_err(|_| Errors::ServerError(String::from("Could not create file")))?;
        Ok(())
    }

    pub fn alter_table(&self, table_name: String) -> Result<(), Errors> {
        let _file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(self.get_file_path(&table_name)).map_err(|_| Errors::ServerError(String::from("Could not open file")))?;
        Ok(())
    }

    pub fn drop_table(&self, table_name: String) -> Result<(), Errors> {
        remove_file(self.get_file_path(&table_name)).map_err(|_| Errors::ServerError(String::from("Could not remove file")))?;
        Ok(())
    }

    pub fn insert(&self, table_name: &String, row: &Row) -> Result<(), Errors> {
        let path = self.get_file_path(table_name);
        if self.pk_already_exists(&path, &row.primary_keys)? {
            return Err(Errors::AlreadyExists(
                "Primary key already exists".to_string(),
            ));
        }
        self.append_row(table_name, row)
    }

    pub fn update_row(&self, path: &String, new_row: Row, where_clause: WhereClause) -> Result<(), Errors> {
        let temp_path = format!("{}.tmp", path);
        self.create_table(&temp_path)?;
        for row in self.get_deserialized_stream(path)? {
            match row {
                Ok(row) => {
                    if where_clause.evaluate(&row.get_row_hash())?{
                        self.append_row(&temp_path, &new_row)?;
                    } else {
                        self.append_row(&temp_path, &row)?;
                    }
                }
                Err(_) => return Err(Errors::ServerError(String::from("Error deserializing row"))),
            }
        }
        rename(temp_path, path).map_err(|_| Errors::ServerError(String::from("Error renaming file")))?;
        Ok(())
    }

    pub fn select_rows(&self, table_name: &String, where_clause: WhereClause, order_by_clause: OrderByClause) -> Result<Vec<Row>, Errors> {
        let path = self.get_file_path(table_name);
        let filtered_path = self.get_file_path(&String::from("filtered"));
        self.filter_rows(&path, &filtered_path, where_clause)?;
        self.sort_rows(&filtered_path, order_by_clause)?;
        let rows = self.get_rows(&filtered_path)?;
        remove_file(filtered_path).map_err(|_| Errors::ServerError(String::from("Could not remove file")))?;
        Ok(rows)
    }

    fn filter_rows(&self, path: &String, filtered_path: &String, where_clause: WhereClause) -> Result<(), Errors> {
        for row in self.get_deserialized_stream(path)? {
            match row {
                Ok(row) => {
                    if where_clause.evaluate(&row.get_row_hash())?{
                        self.append_row(&filtered_path, &row)?;
                    }
                }
                Err(_) => return Err(Errors::ServerError(String::from("Error deserializing row"))),
            }
        }
        Ok(())
    }

    fn sort_rows(&self, path: &String, order_by_clause: OrderByClause) -> Result<(), Errors> {
        Ok(())
    }

    fn get_rows(&self, path: &String) -> Result<Vec<Row>, Errors> {
        let mut rows = Vec::new();
        for row in self.get_deserialized_stream(path)? {
            match row {
                Ok(row) => {
                    rows.push(row);
                }
                Err(_) => return Err(Errors::ServerError(String::from("Error deserializing row"))),
            }
        }
        Ok(rows)
    }

    fn get_file_path(&self, table_name: &String) -> String {
        format!("src/data_access/data/{}.json", table_name)
    }

    fn open_file(&self, table_name: &String) -> Result<File, Errors> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(self.get_file_path(table_name))
            .map_err(|_| Errors::ServerError("Failed to open table file".to_string()))?;
        Ok(file)
    }

    fn append_row(&self, path: &String, row: &Row) -> Result<(), Errors> {
        let mut file = self.open_file(&path)?;
        file.seek(SeekFrom::End(-1)).map_err(|_| Errors::ServerError("Failed to append row to table file".to_string()))?;
        file.write_all(b",").map_err(|_| Errors::ServerError("Failed to append row to table file".to_string()))?;
        let json_row = serde_json::to_string(&row).map_err(|_| Errors::ServerError("Failed to append row to table file".to_string()))?;
        file.write_all(json_row.as_bytes()).map_err(|_| Errors::ServerError("Failed to append row to table file".to_string()))?;
        file.write_all(b"]").map_err(|_| Errors::ServerError("Failed to append row to table file".to_string()))?;
        Ok(())
    }

    fn pk_already_exists(&self, path: &String, primary_keys: &Vec<String>) -> Result<bool, Errors> {
        for row in self.get_deserialized_stream(path)? {
            match row {
                Ok(row) => {
                    if &row.primary_keys == primary_keys {
                        return Ok(true);
                    }
                }
                Err(_) => return Err(Errors::ServerError(String::from("Error deserializing row"))),
            }
        }
        Ok(false)
    }

    fn get_deserialized_stream(&self, path: &String) -> Result<StreamDeserializer<IoRead<BufReader<File>>, Row>, Errors> {
        let file = self.open_file(path)?;
        let reader = BufReader::new(file);
        Ok(serde_json::Deserializer::from_reader(reader).into_iter::<Row>())
    }
}
