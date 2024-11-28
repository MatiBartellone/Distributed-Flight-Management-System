use crate::{parsers::tokens::data_type::DataType, utils::errors::Errors};
use serde_json;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
//use std::sync::{Arc, Mutex, MutexGuard};
use super::{keyspace::Keyspace, table::Table};
use crate::utils::functions::{deserialize_from_str, write_all_to_file};
use crate::utils::types::primary_key::PrimaryKey;
use std::{collections::HashMap, io::Read};

#[derive(Debug)]
pub struct KeyspaceMetaDataAccess;

impl KeyspaceMetaDataAccess {
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
        &self,
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

    pub fn get_replication(&self, path: String, keyspace_name: &str) -> Result<usize, Errors> {
        let mut file = Self::open_file(path)?;
        let mut keyspaces = Self::extract_hash_from_json(&mut file)?;
        let keyspace = get_keyspace_mutable(&mut keyspaces, keyspace_name)?;
        Ok(keyspace.replication_factor)
    }

    pub fn get_tables_from_keyspace(
        &self,
        path: String,
        keyspace_name: &str,
    ) -> Result<Vec<String>, Errors> {
        let mut file = Self::open_file(path)?;
        let mut keyspaces = Self::extract_hash_from_json(&mut file)?;
        let keyspace = get_keyspace_mutable(&mut keyspaces, keyspace_name)?;
        Ok(keyspace.tables.keys().cloned().collect())
    }

    pub fn alter_keyspace(
        &self,
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

    pub fn drop_keyspace(&self, path: String, name: &str) -> Result<(), Errors> {
        let mut file = Self::open_file(path)?;
        let mut keyspaces = Self::extract_hash_from_json(&mut file)?;
        keyspaces.remove(name);
        Self::save_hash_to_json(&mut file, &keyspaces)?;
        Ok(())
    }

    pub fn get_columns_type(
        &self,
        path: String,
        keyspace_name: &str,
        table_name: &str,
    ) -> Result<HashMap<String, DataType>, Errors> {
        let mut file = Self::open_file(path)?;
        let mut keyspaces = Self::extract_hash_from_json(&mut file)?;
        let table = get_table_mutable(&mut keyspaces, keyspace_name, table_name)?;
        Self::reset_pointer(&mut file)?;
        Ok(table.columns.clone())
    }

    pub fn get_primary_key(
        &self,
        path: String,
        keyspace_name: &str,
        table_name: &str,
    ) -> Result<PrimaryKey, Errors> {
        let mut file = Self::open_file(path)?;
        let mut keyspaces = Self::extract_hash_from_json(&mut file)?;
        let table = get_table_mutable(&mut keyspaces, keyspace_name, table_name)?;
        Self::reset_pointer(&mut file)?;
        Ok(table.primary_key.to_owned())
    }

    pub fn add_table(
        &self,
        path: String,
        keyspace_name: &str,
        table_name: &str,
        primary_key: PrimaryKey,
        columns: HashMap<String, DataType>,
    ) -> Result<(), Errors> {
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

    pub fn delete_table(
        &self,
        path: String,
        keyspace_name: &str,
        table_name: &str,
    ) -> Result<(), Errors> {
        let mut file = Self::open_file(path)?;
        let mut keyspaces = Self::extract_hash_from_json(&mut file)?;
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

    pub fn new_column(
        &self,
        path: String,
        keyspace_name: &str,
        table_name: &str,
        column_name: &str,
        data_type: DataType,
    ) -> Result<(), Errors> {
        let mut file = Self::open_file(path)?;
        let mut keyspaces = Self::extract_hash_from_json(&mut file)?;
        let table = get_table_mutable(&mut keyspaces, keyspace_name, table_name)?;
        table.columns.insert(column_name.to_string(), data_type);
        Self::save_hash_to_json(&mut file, &keyspaces)?;
        Ok(())
    }

    pub fn drop_column(
        &self,
        path: String,
        keyspace_name: &str,
        table_name: &str,
        column_name: &str,
    ) -> Result<(), Errors> {
        let mut file = Self::open_file(path)?;
        let mut keyspaces = Self::extract_hash_from_json(&mut file)?;
        let table = get_table_mutable(&mut keyspaces, keyspace_name, table_name)?;
        table.columns.remove(column_name);
        Self::save_hash_to_json(&mut file, &keyspaces)?;
        Ok(())
    }

    pub fn rename_column(
        &self,
        path: String,
        keyspace_name: &str,
        table_name: &str,
        column_name1: &str,
        column_name2: &str,
    ) -> Result<(), Errors> {
        let mut file = Self::open_file(path)?;
        let mut keyspaces = Self::extract_hash_from_json(&mut file)?;
        let table = get_table_mutable(&mut keyspaces, keyspace_name, table_name)?;
        rename_key(
            &mut table.columns,
            column_name1.to_owned(),
            column_name2.to_owned(),
        );
        Self::save_hash_to_json(&mut file, &keyspaces)?;
        Ok(())
    }

    fn extract_hash_from_json(file: &mut File) -> Result<HashMap<String, Keyspace>, Errors> {
        let mut contents = String::new();
        file.read_to_string(&mut contents)
            .map_err(|_| Errors::ServerError("Unable to read file".to_string()))?;
        let existing_keyspaces: HashMap<String, Keyspace> = if contents.is_empty() {
            HashMap::new()
        } else {
            deserialize_from_str(&contents)?
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
        write_all_to_file(file, json_data.as_bytes())?;
        file.flush()
            .map_err(|_| Errors::ServerError("Failed to flush data to file".to_string()))?;

        Ok(())
    }

    fn reset_pointer(file: &mut File) -> Result<(), Errors> {
        file.seek(SeekFrom::Start(0))
            .map_err(|_| Errors::ServerError("Failed to reset file pointer".to_string()))?;
        Ok(())
    }
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

fn rename_key<K, V>(map: &mut HashMap<K, V>, old_key: K, new_key: K)
where
    K: std::hash::Hash + Eq,
{
    if let Some(value) = map.remove(&old_key) {
        map.insert(new_key, value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::fs::{self, OpenOptions};

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
        let existing_keyspaces: HashMap<String, Keyspace> = deserialize_from_str(&contents)?;
        Ok(existing_keyspaces)
    }

    fn assert_keyspace_replication(
        keyspace: &Keyspace,
        expected_strategy: &str,
        expected_factor: usize,
    ) {
        assert_eq!(
            keyspace.replication_strategy, expected_strategy,
            "Expected replication_strategy to be '{}', but got '{}'",
            expected_strategy, keyspace.replication_strategy
        );

        assert_eq!(
            keyspace.replication_factor, expected_factor,
            "Expected replication_factor to be '{}', but got '{}'",
            expected_factor, keyspace.replication_factor
        );
    }

    fn add_test_table_with_columns(file_name: &str) -> Result<(), Errors> {
        let mut columns = HashMap::new();
        columns.insert("column1".to_string(), DataType::Boolean);
        columns.insert("column2".to_string(), DataType::Int);
        columns.insert("column3".to_string(), DataType::Duration);
        columns.insert("column4".to_string(), DataType::Date);
        columns.insert("column5".to_string(), DataType::Text);
        let meta_data = KeyspaceMetaDataAccess {};
        meta_data.add_table(
            file_name.to_string(),
            "test_keyspace",
            "test_table",
            PrimaryKey::new(vec!["column2".to_string()], None),
            columns,
        )?;

        Ok(())
    }

    fn add_keyspace_test(file_name: &str) -> Result<(), Errors> {
        let meta_data = KeyspaceMetaDataAccess {};
        meta_data.add_keyspace(
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
        let meta_data = KeyspaceMetaDataAccess {};
        // Alter the keyspace
        let result = meta_data.alter_keyspace(
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

        let meta_data = KeyspaceMetaDataAccess {};
        meta_data
            .drop_keyspace(file_name.to_string(), "test_keyspace_drop")
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
        let meta_data = KeyspaceMetaDataAccess {};
        meta_data
            .add_table(
                file_name.to_string(),
                "test_keyspace",
                "test_table_additional",
                PrimaryKey::new(vec!["columnB".to_string()], None),
                additional_columns,
            )
            .expect("Failed to add additional table");

        // Eliminar una de las tablas
        meta_data
            .delete_table(file_name.to_string(), "test_keyspace", "test_table")
            .expect("Failed to delete table");

        let mut file = File::open(file_name).expect("Failed to open test file");
        let keyspaces = extract_json(&mut file).unwrap();
        let keyspace = keyspaces.get("test_keyspace").unwrap();
        assert!(
            !keyspace.tables.contains_key("test_table"),
            "Expected 'test_table' to be deleted."
        );

        // Verificar que la otra tabla aún está presente
        assert!(
            keyspace.tables.contains_key("test_table_additional"),
            "Expected 'test_table_additional' to still exist."
        );

        cleanup_test_file(file_name);
    }

    #[test]
    fn test_get_columns() {
        let file_name = "test_get_columns.json";
        create_test_file(file_name).expect("Failed to create test file");

        assert!(add_keyspace_test(file_name).is_ok());

        assert!(add_test_table_with_columns(file_name).is_ok());
        let meta_data = KeyspaceMetaDataAccess {};

        // Obtener las columnas de la tabla
        let columns = meta_data
            .get_columns_type(file_name.to_string(), "test_keyspace", "test_table")
            .expect("Failed to get columns");

        // Verificar que las columnas son las esperadas
        assert_eq!(columns.len(), 5, "Expected 5 columns in 'test_table'.");
        assert!(
            columns.contains_key("column1"),
            "Expected 'column1' to be present."
        );
        assert!(
            columns.contains_key("column2"),
            "Expected 'column2' to be present."
        );
        assert!(
            columns.contains_key("column3"),
            "Expected 'column3' to be present."
        );
        assert!(
            columns.contains_key("column4"),
            "Expected 'column4' to be present."
        );
        assert!(
            columns.contains_key("column5"),
            "Expected 'column5' to be present."
        );

        cleanup_test_file(file_name);
    }

    #[test]
    fn test_get_primary_key() {
        let file_name = "test_get_primary_key.json";
        create_test_file(file_name).expect("Failed to create test file");

        assert!(add_keyspace_test(file_name).is_ok());

        assert!(add_test_table_with_columns(file_name).is_ok());
        let meta_data = KeyspaceMetaDataAccess {};

        let vec_pk = meta_data
            .get_primary_key(file_name.to_string(), "test_keyspace", "test_table")
            .expect("Failed to get primary key");
        let pk = &vec_pk.partition_keys[0];
        assert_eq!(pk, "column2", "Expected primary key to be 'column2'.");
        cleanup_test_file(file_name);
    }
}
