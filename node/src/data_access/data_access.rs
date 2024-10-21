use crate::data_access::row::Row;
use crate::utils::errors::Errors;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use crate::queries::evaluate::Evaluate;
use crate::queries::where_logic::where_clause::WhereClause;

pub struct DataAccess {}

impl DataAccess {
    pub fn new() -> DataAccess {
        DataAccess {}
    }

    pub fn create_table(&self, table_name: String) -> Result<(), Errors> {
        let file = self.open_file(&table_name);

        if file.is_ok() {
            return Err(Errors::AlreadyExists("Table already exists".to_string()));
        }

        File::create(format!("{}.json", table_name))
            .map_err(|_| Errors::ServerError(String::from("Could not create file")))?;
        Ok(())
    }

    pub fn alter(&self, table_name: String) -> Result<(), Errors> {
        let file = self.open_file(&table_name)?;
        Ok(())
    }

    pub fn insert(&self, table_name: &String, row: Row) -> Result<(), Errors> {
        let path = self.get_file_path(table_name);
        if self.pk_already_exists(&path, &row.primary_keys) {
            return Err(Errors::AlreadyExists(
                "Primary key already exists".to_string(),
            ));
        }
        self.append_row(table_name, row)
    }

    pub fn update_row(&self, path: &String, new_row: Row, where_clause: WhereClause) -> Result<(), Errors> {
        let file = self.open_file(path)?;
        let reader = BufReader::new(file);
        let stream = serde_json::Deserializer::from_reader(reader).into_iter::<Row>();
        for row in stream {
            match row {
                Ok(row) => {
                    if where_clause.evaluate(&row.get_row_hash())?{

                    }
                }
                Err(_) => return Err(Errors::ServerError(String::from("Error deserializing row"))),
            }
        }
        Ok(())
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

    fn append_row(&self, path: &String, row: Row) -> Result<(), Errors> {
        let err = Errors::ServerError("Failed to append row to table file".to_string());
        let mut file = self.open_file(&path)?;
        file.seek(SeekFrom::End(-1)).map_err(|_| &err)?;
        file.write_all(b",").map_err(|_| &err)?;
        let json_row = serde_json::to_string(&row).map_err(|_| &err)?;
        file.write_all(json_row.as_bytes()).map_err(|_| &err)?;
        file.write_all(b"]").map_err(|_| &err)?;
        Ok(())
    }

    fn pk_already_exists(&self, path: &String, primary_keys: &Vec<String>) -> Result<bool, Errors> {
        let file = self.open_file(path)?;
        let reader = BufReader::new(file);
        let stream = serde_json::Deserializer::from_reader(reader).into_iter::<Row>();
        for row in stream {
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
}
