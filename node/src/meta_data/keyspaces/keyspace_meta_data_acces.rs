use std::fs::File;
use std::sync::{Mutex, Arc, MutexGuard};
use std::{collections::HashMap, io::Read};
use std::io::Write;
use serde_json;
use crate::{utils::errors::Errors, parsers::tokens::data_type::DataType};

use super::{keyspace::Keyspace, table::Table};

#[derive(Debug)]
pub struct KeyspaceMetaDataAccess {
    file: Arc<Mutex<File>>, // Compartir el Mutex
}

impl KeyspaceMetaDataAccess {

    pub fn new(path: &str) -> Result<Self, Errors> {
        let file = File::open(path).map_err(|_| Errors::ServerError("Unable to read file".to_string()))?; 
        let mutex = Arc::new(Mutex::new(file));
        
        Ok(Self{file: mutex})
    }

    pub fn add_keyspace(
        &mut self,
        name: &str,
        replication_strategy: Option<String>,
        replication_factor: Option<usize>,
    ) -> Result<(), Errors> {
        let (mut file, mut keyspaces) = self.lock_and_extract_keyspaces()?;
        if keyspaces.contains_key(name) {
            return Err(Errors::SyntaxError(
                "El keyspace ya está creado".to_string(),
            ));
        }
        let keyspace = Keyspace::new(replication_strategy, replication_factor);
        keyspaces.insert(name.to_owned(), keyspace);
        self.save_hash_to_json(&mut file,&keyspaces)?; 
        Ok(())
    }

    pub fn alter_keyspace(
        &mut self,
        name: &str,
        replication_strategy: Option<String>,
        replication_factor: Option<usize>,
    ) -> Result<(), Errors> {
        let (mut file, mut keyspaces) = self.lock_and_extract_keyspaces()?;
        let keyspace = get_keyspace_mutable(&mut keyspaces, name)?;

        if let Some(strategy) = replication_strategy {
            keyspace.set_replication_strategy(strategy);
        }

        if let Some(factor) = replication_factor {
            keyspace.set_replication_factor(factor);
        }
        self.save_hash_to_json(&mut file, &keyspaces)?;
        Ok(())
    }

    pub fn drop_keyspace(&mut self, name: &str) -> Result<(), Errors> {
        let (mut file, mut keyspaces) = self.lock_and_extract_keyspaces()?;
        keyspaces.remove(name);
        self.save_hash_to_json(&mut file,&keyspaces)?; 
        Ok(())
    }

    pub fn get_columns_type(
        &mut self,
        keyspace_name: &str,
        table_name: &str,
    ) -> Result<HashMap<String, DataType>, Errors> {
        let (_file, mut keyspaces) = self.lock_and_extract_keyspaces()?;
        let table = get_table_mutable(&mut keyspaces, keyspace_name, table_name)?;
        Ok(table.columns.clone())
    }

    pub fn get_primary_key(
        &mut self,
        keyspace_name: &str,
        table_name: &str,
    ) -> Result<String, Errors> {
        let (_file, mut keyspaces) = self.lock_and_extract_keyspaces()?;
        let table = get_table_mutable(&mut keyspaces, keyspace_name, table_name)?;
        Ok(table.primary_key.clone())
    }

    pub fn add_table(
        &mut self,
        keyspace_name: &str,
        table_name: &str,
        primary_key: String,
        columns: HashMap<String, DataType>,
    ) -> Result<(), Errors> {
        let (mut file, mut keyspaces) = self.lock_and_extract_keyspaces()?;
        let keyspace = get_keyspace_mutable(&mut keyspaces, keyspace_name)?;
        if keyspace.tables.contains_key(table_name) {
            return Err(Errors::SyntaxError("La tabla ya está creada".to_string()));
        }
        let table = Table::new(primary_key, columns);
        keyspace.tables.insert(table_name.to_string(), table);
        self.save_hash_to_json(&mut file,&keyspaces)?;
        Ok(())
    }

    pub fn delete_table(
        &mut self,
        keyspace_name: &str,
        table_name: &str,
    ) -> Result<(), Errors> {
        let (mut file, mut keyspaces) = self.lock_and_extract_keyspaces()?;
        let keyspace = get_keyspace_mutable(&mut keyspaces, keyspace_name)?;
        if !keyspace.tables.contains_key(table_name) {
            return Err(Errors::SyntaxError(format!(
                "La tabla '{}' no existe en el keyspace '{}'",
                table_name, keyspace_name
            )));
        }
        keyspace.tables.remove(table_name);
        self.save_hash_to_json(&mut file,&keyspaces)?;
        Ok(())
    }

    fn extract_hash_from_json(
        &self,
        file: &mut std::sync::MutexGuard<File>,
    ) -> Result<HashMap<String, Keyspace>, Errors> {
        let mut contents = String::new();
        
        // Leer del archivo ya bloqueado
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
        &self,
        file: &mut std::sync::MutexGuard<File>,
        keyspaces: &HashMap<String, Keyspace>,
    ) -> Result<(), Errors> {
        let json_data = serde_json::to_string_pretty(keyspaces)
            .map_err(|_| Errors::ServerError("Failed to serialize keyspaces".to_string()))?;
    
        // Limpiar el archivo
        file.set_len(0).map_err(|_| Errors::ServerError("Failed to truncate file".to_string()))?;
        
        // Escribir el nuevo contenido
        file.write_all(json_data.as_bytes())
            .map_err(|_| Errors::ServerError("Failed to write data to file".to_string()))?;

        Ok(())
    }

    fn lock_and_extract_keyspaces(
        &self,
    ) -> Result<(MutexGuard<File>, HashMap<String, Keyspace>), Errors> {
        let mut file = self.file.lock().map_err(|_| Errors::ServerError("Failed to acquire lock".to_string()))?;
        let keyspaces = self.extract_hash_from_json(&mut file)?;
        Ok((file, keyspaces))
    }
}

fn get_keyspace_mutable<'a>(
    keyspaces: &'a mut HashMap<String, Keyspace>,
    name: &str
) -> Result<&'a mut Keyspace, Errors> {
    keyspaces.get_mut(name).ok_or_else(|| {
        Errors::SyntaxError(format!("El keyspace '{}' no existe", name))
    })
}

fn get_table_mutable<'a>(
    keyspaces: &'a mut HashMap<String, Keyspace>,
    keyspace_name: &str,
    table_name: &str
) -> Result<&'a mut Table, Errors> {
    let keyspace = get_keyspace_mutable(keyspaces, keyspace_name)?;
    keyspace.tables.get_mut(table_name).ok_or_else(|| {
        Errors::SyntaxError(format!("La tabla '{}' no existe en el keyspace '{}'", table_name, keyspace_name))
    })
}

