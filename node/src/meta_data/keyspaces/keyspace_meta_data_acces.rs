use crate::{parsers::tokens::data_type::DataType, utils::errors::Errors};
use serde_json;
use std::fs::{File, OpenOptions};
use std::io::{Write, Seek, SeekFrom};
//use std::sync::{Arc, Mutex, MutexGuard};
use std::{collections::HashMap, io::Read};

use super::{keyspace::Keyspace, table::Table};
#[derive(Debug)]
pub struct KeyspaceMetaDataAccess {
    //file: Arc<Mutex<File>>, 
}

impl KeyspaceMetaDataAccess {
    /*pub fn new() -> Result<Self, Errors> {
        let file = OpenOptions::new()
        .read(true)  // Permitir lectura
        .write(true) // Permitir escritura
        .create(true) // Crear el archivo si no existe
        .open(PATH)
        .map_err(|_| Errors::ServerError("Unable to open or create file".to_string()))?;
        let mutex = Arc::new(Mutex::new(file));
        Ok(Self { file: mutex })
    } */

    fn open_file(path: String) -> Result<File, Errors> {
        let file = OpenOptions::new()
        .read(true)  
        .write(true) 
        .create(true)
        .truncate(false)
        .open(path)
        .map_err(|_| Errors::ServerError("Unable to open or create file".to_string()))?;
        Ok(file)
    }

    pub fn add_keyspace(
        path: String,
        name: &str,
        replication_strategy: Option<String>,
        replication_factor: Option<usize>,
    ) -> Result<(), Errors> {
        //let (mut file, mut keyspaces) = self.lock_and_extract_keyspaces()?;
        let mut file = Self::open_file(path)?;
        let mut keyspaces = Self::extract_hash_from_json(&mut file)?;
        if keyspaces.contains_key(name) {
            return Err(Errors::SyntaxError(
                "El keyspace ya está creado".to_string(),
            ));
        }
        let keyspace = Keyspace::new(replication_strategy, replication_factor);
        keyspaces.insert(name.to_owned(), keyspace);
        Self::save_hash_to_json(&mut file, &keyspaces)?; 
        Ok(())
    }

    pub fn alter_keyspace(
        path: String,
        name: &str,
        replication_strategy: Option<String>,
        replication_factor: Option<usize>,
    ) -> Result<(), Errors> {
        //let (mut file, mut keyspaces) = self.lock_and_extract_keyspaces()?;
        let mut file = Self::open_file(path)?;
        let mut keyspaces = Self::extract_hash_from_json(&mut file)?;
        let keyspace = get_keyspace_mutable(&mut keyspaces, name)?;

        if let Some(strategy) = replication_strategy {
            keyspace.set_replication_strategy(strategy);
        }

        if let Some(factor) = replication_factor {
            keyspace.set_replication_factor(factor);
        }
        Self::save_hash_to_json(&mut file, &keyspaces)?;
        Ok(())
    }

    pub fn drop_keyspace(path: String, name: &str) -> Result<(), Errors> {
        //let (mut file, mut keyspaces) = self.lock_and_extract_keyspaces()?;
        let mut file = Self::open_file(path)?;
        let mut keyspaces = Self::extract_hash_from_json(&mut file)?;
        keyspaces.remove(name);
        Self::save_hash_to_json(&mut file, &keyspaces)?;
        Ok(())
    }

    pub fn get_columns_type(
        path: String,
        keyspace_name: &str,
        table_name: &str,
    ) -> Result<HashMap<String, DataType>, Errors> {
        //let (mut file, mut keyspaces) = self.lock_and_extract_keyspaces()?;
        let mut file = Self::open_file(path)?;
        let mut keyspaces = Self::extract_hash_from_json(&mut file)?;
        let table = get_table_mutable(&mut keyspaces, keyspace_name, table_name)?;
        Self::reset_pointer(&mut file)?;
        Ok(table.columns.clone())
    }

    pub fn get_primary_key(
        path: String,
        keyspace_name: &str,
        table_name: &str,
    ) -> Result<String, Errors> {
        //let (mut file, mut keyspaces) = self.lock_and_extract_keyspaces()?;
        let mut file = Self::open_file(path)?;
        let mut keyspaces = Self::extract_hash_from_json(&mut file)?;
        let table = get_table_mutable(&mut keyspaces, keyspace_name, table_name)?;
        Self::reset_pointer(&mut file)?;
        Ok(table.primary_key.clone())
    }

    pub fn add_table(
        path: String,
        keyspace_name: &str,
        table_name: &str,
        primary_key: String,
        columns: HashMap<String, DataType>,
    ) -> Result<(), Errors> {
        //let (mut file, mut keyspaces) = self.lock_and_extract_keyspaces()?;
        let mut file = Self::open_file(path)?;
        let mut keyspaces = Self::extract_hash_from_json(&mut file)?;
        let keyspace = get_keyspace_mutable(&mut keyspaces, keyspace_name)?;
        if keyspace.tables.contains_key(table_name) {
            return Err(Errors::SyntaxError("La tabla ya está creada".to_string()));
        }
        let table = Table::new(primary_key, columns);
        keyspace.tables.insert(table_name.to_string(), table);
        Self::save_hash_to_json(&mut file, &keyspaces)?;
        Ok(())
    }

    pub fn delete_table(path: String, keyspace_name: &str, table_name: &str, ) -> Result<(), Errors> {
        let mut file = Self::open_file(path)?;
        let mut keyspaces = Self::extract_hash_from_json(&mut file)?;
        //let (mut file, mut keyspaces) = self.lock_and_extract_keyspaces()?;
        let keyspace = get_keyspace_mutable(&mut keyspaces, keyspace_name)?;
        if !keyspace.tables.contains_key(table_name) {
            return Err(Errors::SyntaxError(format!(
                "La tabla '{}' no existe en el keyspace '{}'",
                table_name, keyspace_name
            )));
        }
        keyspace.tables.remove(table_name);
        Self::save_hash_to_json(&mut file, &keyspaces)?;
        Ok(())
    }

    fn extract_hash_from_json(
        file: &mut File,
    ) -> Result<HashMap<String, Keyspace>, Errors> {
        let mut contents = String::new();
        file.read_to_string(&mut contents)
            .map_err(|_| Errors::ServerError("Unable to read file".to_string()))?;
        let existing_keyspaces: HashMap<String, Keyspace> = if contents.is_empty() {
            HashMap::new()
        } else {
            serde_json::from_str(&contents).map_err(|_| {
                Errors::ServerError("Failed to deserialize existing keyspaces".to_string())
            })?
        };
        Ok(existing_keyspaces)
    }


    fn save_hash_to_json(
        file: &mut File,
        keyspaces: &HashMap<String, Keyspace>,
    ) -> Result<(), Errors> {
        let json_data = serde_json::to_string_pretty(keyspaces)
        .map_err(|_| Errors::ServerError("Failed to serialize keyspaces".to_string()))?;
        file.set_len(0)
            .map_err(|_| Errors::ServerError("Failed to truncate file".to_string()))?;
        Self::reset_pointer(file)?;
        
        file.write_all(json_data.as_bytes())
            .map_err(|_| Errors::ServerError("Failed to write data to file".to_string()))?;
        file.flush()
            .map_err(|_| Errors::ServerError("Failed to flush data to file".to_string()))?;
        //self.reset_pointer(file)?;
        Ok(())
    }

    

    fn reset_pointer(
        file: &mut File,
    ) -> Result<(), Errors> {
        file.seek(SeekFrom::Start(0))
            .map_err(|_| Errors::ServerError("Failed to reset file pointer".to_string()))?;
        Ok(())
    }

   /*fn lock_and_extract_keyspaces(
        &self,
    ) -> Result<(MutexGuard<File>, HashMap<String, Keyspace>), Errors> {
        let mut file = self
            .file
            .lock()
            .map_err(|_| Errors::ServerError("Failed to acquire lock".to_string()))?;
        let keyspaces = self.extract_hash_from_json(&mut file)?;
        Ok((file, keyspaces))
    } */

    /* fn save_hash_to_json(
        &self,
        file: &mut std::sync::MutexGuard<File>,
        keyspaces: &HashMap<String, Keyspace>,
    ) -> Result<(), Errors> {
        let json_data = serde_json::to_string_pretty(keyspaces)
        .map_err(|_| Errors::ServerError("Failed to serialize keyspaces".to_string()))?;
        file.set_len(0)
            .map_err(|_| Errors::ServerError("Failed to truncate file".to_string()))?;
        self.reset_pointer(file)?;
        
        file.write_all(json_data.as_bytes())
            .map_err(|_| Errors::ServerError("Failed to write data to file".to_string()))?;
        file.flush()
            .map_err(|_| Errors::ServerError("Failed to flush data to file".to_string()))?;
        self.reset_pointer(file)?;
        Ok(())
    } */

}

fn get_keyspace_mutable<'a>(
    keyspaces: &'a mut HashMap<String, Keyspace>,
    name: &str,
) -> Result<&'a mut Keyspace, Errors> {
    keyspaces
        .get_mut(name)
        .ok_or_else(|| Errors::SyntaxError(format!("El keyspace '{}' no existe", name)))
}

fn get_table_mutable<'a>(
    keyspaces: &'a mut HashMap<String, Keyspace>,
    keyspace_name: &str,
    table_name: &str,
) -> Result<&'a mut Table, Errors> {
    let keyspace = get_keyspace_mutable(keyspaces, keyspace_name)?;
    keyspace.tables.get_mut(table_name).ok_or_else(|| {
        Errors::SyntaxError(format!(
            "La tabla '{}' no existe en el keyspace '{}'",
            table_name, keyspace_name
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::{self, OpenOptions};
    use std::collections::HashMap;

    fn create_test_file(name: &str) -> Result<(), Errors> {
        let _ = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(name)
            .map_err(|_| Errors::ServerError("Unable to create test file".to_string()));
        Ok(())
    }

    fn cleanup_test_file(name: &str) {
        let _ = fs::remove_file(name);
    }

    fn extract_json(file: &mut File) -> Result<HashMap<String, Keyspace>, Errors> {
        let mut contents = String::new();
        file.read_to_string(&mut contents)
            .map_err(|_| Errors::ServerError("Unable to read file".to_string()))?;
        if contents.is_empty() {
            return Err(Errors::ServerError("The file is empty".to_string()));
        }
        let existing_keyspaces: HashMap<String, Keyspace> = serde_json::from_str(&contents)
            .map_err(|_| Errors::ServerError("Failed to deserialize existing keyspaces".to_string()))?;
        Ok(existing_keyspaces)
    }

    fn assert_keyspace_replication(
        keyspace: &Keyspace,
        expected_strategy: &str,
        expected_factor: usize,
    ) {
        assert_eq!(keyspace.replication_strategy, expected_strategy, 
                   "Expected replication_strategy to be '{}', but got '{}'", 
                   expected_strategy, keyspace.replication_strategy);
    
        assert_eq!(keyspace.replication_factor, expected_factor, 
                   "Expected replication_factor to be '{}', but got '{}'", 
                   expected_factor, keyspace.replication_factor);
    }

    fn add_test_table_with_columns(file_name: &str) -> Result<(), Errors> {
        let mut columns = HashMap::new();
        columns.insert("column1".to_string(), DataType::Boolean);
        columns.insert("column2".to_string(), DataType::Int);
        columns.insert("column3".to_string(), DataType::Duration);
        columns.insert("column4".to_string(), DataType::Date);
        columns.insert("column5".to_string(), DataType::Text);

        KeyspaceMetaDataAccess::add_table(
            file_name.to_string(),
            "test_keyspace",
            "test_table",
            "column2".to_string(),
            columns
        )?;

        Ok(())
    }

    fn add_keyspace_test(file_name: &str) -> Result<(), Errors> {
        KeyspaceMetaDataAccess::add_keyspace(
            file_name.to_string(),
            "test_keyspace",
            Some("SimpleStrategy".to_string()),
            Some(3),
        )?;
        Ok(())
    }

    #[test]
    fn test_add_keyspace() {
        let file_name = "test_keyspace_add.json";
        create_test_file(file_name).expect("Failed to create test file");

        assert!(add_keyspace_test(file_name).is_ok());

        let mut file = File::open(file_name).expect("Failed to open test file");
        let keyspaces = extract_json(&mut file).unwrap();
        assert!(keyspaces.contains_key("test_keyspace"));

        let keyspace = keyspaces.get("test_keyspace").unwrap();

        assert_keyspace_replication(keyspace, "SimpleStrategy", 3);
        cleanup_test_file(file_name);
    }

    #[test]
    fn test_alter_keyspace() {
        let file_name = "test_keyspace_alter.json";
        create_test_file(file_name).expect("Failed to create test file");

        assert!(add_keyspace_test(file_name).is_ok());

        // Alter the keyspace
        let result = KeyspaceMetaDataAccess::alter_keyspace(
            file_name.to_string(),
            "test_keyspace",
            Some("NetworkTopologyStrategy".to_string()),
            Some(2),
        );        
        assert!(result.is_ok());

        let mut file = File::open(file_name).expect("Failed to open test file");
        let keyspaces = extract_json(&mut file).unwrap();
        let keyspace = keyspaces.get("test_keyspace").unwrap();
        assert_keyspace_replication(keyspace, "NetworkTopologyStrategy", 2);
        cleanup_test_file(file_name);
    }

    #[test]
    fn test_drop_keyspace() {
        let file_name = "test_keyspace_drop.json";
        create_test_file(file_name).expect("Failed to create test file");

        assert!(add_keyspace_test(file_name).is_ok());

        KeyspaceMetaDataAccess::drop_keyspace(file_name.to_string(), "test_keyspace_drop")
            .expect("Failed to drop keyspace");

        let mut file = File::open(file_name).expect("Failed to open test file");
        let keyspaces = extract_json(&mut file).unwrap();
        assert!(!keyspaces.contains_key("test_keyspace_drop"));

        cleanup_test_file(file_name);
    }

    #[test]
    fn test_add_table() {
        let file_name = "test_table_add_table.json";
        create_test_file(file_name).expect("Failed to create test file");

        assert!(add_keyspace_test(file_name).is_ok());

        assert!(add_test_table_with_columns(file_name).is_ok());

        let mut file = File::open(file_name).expect("Failed to open test file");
        let keyspaces = extract_json(&mut file).unwrap();
        let keyspace = keyspaces.get("test_keyspace").unwrap();
        assert!(keyspace.tables.contains_key("test_table"));

        cleanup_test_file(file_name);
    }

    #[test]
    fn test_delete_table() {
        let file_name = "test_table_delete.json";
        create_test_file(file_name).expect("Failed to create test file");

        assert!(add_keyspace_test(file_name).is_ok());
        assert!(add_test_table_with_columns(file_name).is_ok());
        
        // Agregar otra tabla
        let mut additional_columns = HashMap::new();
        additional_columns.insert("columnA".to_string(), DataType::Text);
        additional_columns.insert("columnB".to_string(), DataType::Int);
        KeyspaceMetaDataAccess::add_table(
            file_name.to_string(),
            "test_keyspace",
            "test_table_additional",
            "columnB".to_string(),
            additional_columns
        ).expect("Failed to add additional table");

        // Eliminar una de las tablas
        KeyspaceMetaDataAccess::delete_table(
            file_name.to_string(),
            "test_keyspace",
            "test_table"
        ).expect("Failed to delete table");

        let mut file = File::open(file_name).expect("Failed to open test file");
        let keyspaces = extract_json(&mut file).unwrap();
        let keyspace = keyspaces.get("test_keyspace").unwrap();
        assert!(!keyspace.tables.contains_key("test_table"), "Expected 'test_table' to be deleted.");
        
        // Verificar que la otra tabla aún está presente
        assert!(keyspace.tables.contains_key("test_table_additional"), "Expected 'test_table_additional' to still exist.");

        cleanup_test_file(file_name);
    }

    #[test]
    fn test_get_columns() {
        let file_name = "test_get_columns.json";
        create_test_file(file_name).expect("Failed to create test file");

        assert!(add_keyspace_test(file_name).is_ok());

        assert!(add_test_table_with_columns(file_name).is_ok());

        // Obtener las columnas de la tabla
        let columns = KeyspaceMetaDataAccess::get_columns_type(
            file_name.to_string(),
            "test_keyspace",
            "test_table"
        ).expect("Failed to get columns");

        // Verificar que las columnas son las esperadas
        assert_eq!(columns.len(), 5, "Expected 5 columns in 'test_table'.");
        assert!(columns.contains_key("column1"), "Expected 'column1' to be present.");
        assert!(columns.contains_key("column2"), "Expected 'column2' to be present.");
        assert!(columns.contains_key("column3"), "Expected 'column3' to be present.");
        assert!(columns.contains_key("column4"), "Expected 'column4' to be present.");
        assert!(columns.contains_key("column5"), "Expected 'column5' to be present.");

        cleanup_test_file(file_name);
    }

    #[test]
    fn test_get_primary_key() {
        let file_name = "test_get_primary_key.json";
        create_test_file(file_name).expect("Failed to create test file");

        assert!(add_keyspace_test(file_name).is_ok());

        assert!(add_test_table_with_columns(file_name).is_ok());

        let pk = KeyspaceMetaDataAccess::get_primary_key(
            file_name.to_string(),
            "test_keyspace",
            "test_table"
        ).expect("Failed to get primary key");

        assert_eq!(pk, "column2", "Expected primary key to be 'column2'.");

        cleanup_test_file(file_name);
    }
}


