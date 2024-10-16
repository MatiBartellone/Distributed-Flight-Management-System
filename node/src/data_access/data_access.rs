use crate::{parsers::tokens::literal::Literal, utils::errors::Errors};

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

        std::fs::File::create(format!("{}.json", table_name));
        Ok(())
    }

    pub fn alter(&self, table_name: String) -> Result<(), Errors> {
        let file = self.open_file(&table_name)?;
        Ok(())
    }

    pub fn insert(&self, table_name: &String, line: &Vec<Vec<Literal>>) -> Result<(), Errors> {
        let file = self.open_file(&table_name)?;
        Ok(())
    }

    fn open_file(&self, table_name: &String) -> Result<(), Errors> {
        let file_name = format!("{}.json", table_name);
        let file = std::fs::File::open(file_name)
            .map_err(|_| Errors::Invalid("Table does not exist".to_string()))?;

        Ok(())
    }
}
