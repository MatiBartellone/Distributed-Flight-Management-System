use std::fs::{File, OpenOptions};
use std::io::Read;
use crate::{parsers::tokens::literal::Literal, utils::errors::Errors};
use crate::data_access::row::Row;

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

        File::create(format!("{}.json", table_name)).map_err(|_| Errors::ServerError(String::from("Could not create file")))?;
        Ok(())
    }

    pub fn alter(&self, table_name: String) -> Result<(), Errors> {
        let file = self.open_file(&table_name)?;
        Ok(())
    }

    pub fn insert(&self, table_name: &String, row: Row) -> Result<(), Errors> {
        if self.pk_already_exists(&row) {
            return Err(Errors::AlreadyExists("Primary key already exists".to_string()));
        }
        let mut rows = self.get_deserialized_content(table_name)?;
        rows.push(row);
        self.write_new_content(table_name, rows)?;
        Ok(())
    }
    fn get_file_path(&self, table_name: &String) -> String {
        format!("src/data_access/data/{}.json", table_name)
    }
    fn open_file(&self, table_name: &String) -> Result<File, Errors> {
        let file = OpenOptions::new().read(true).write(true).open(self.get_file_path(table_name))
            .map_err(|_| Errors::ServerError("Failed to open table file".to_string()))?;
        Ok(file)
    }

    fn get_deserialized_content(&self, table_name: &String) -> Result<Vec<Row>, Errors> {
        let mut file = self.open_file(&table_name)?;
        let mut content = String::new();
        file.read_to_string(&mut content)
            .map_err(|_| Errors::ServerError("Failed to read file".to_string()))?;
        let rows: Vec<Row> = if content.trim().is_empty() {
            Vec::new()
        } else {
            serde_json::from_str(&content)
                .map_err(|_| Errors::ServerError("Failed to deserialize JSON".to_string()))?
        };
        Ok(rows)
    }

    fn write_new_content(&self, table_name: &String, rows: Vec<Row>) -> Result<(), Errors> {
        let file = self.open_file(&table_name)?;
        serde_json::to_writer_pretty(&file, &rows)
            .map_err(|_| Errors::ServerError("Failed to write JSON".to_string()))?;
        Ok(())
    }
    fn pk_already_exists(&self, row: &Row) -> Result<bool, Errors> {
        Ok(false)
    }
}
