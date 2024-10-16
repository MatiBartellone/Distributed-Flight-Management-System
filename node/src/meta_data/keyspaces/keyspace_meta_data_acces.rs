use crate::{parsers::tokens::data_type::DataType, utils::errors::Errors};
use serde_json;
use std::io::Write;
use std::sync::Mutex;
use std::{collections::HashMap, fs::OpenOptions, io::Read};

const RUTA: &str = "ruta/al/archivo";
use super::{keyspace::Keyspace, table::Table};

#[derive(Debug)]
struct KeyspaceMetaDataAccess {
    dir: String,
}

impl KeyspaceMetaDataAccess {
    pub fn new(dir: String) -> Self {
        KeyspaceMetaDataAccess { dir }
    }

    pub fn add_keyspace(
        &mut self,
        name: &str,
        replication_strategy: Option<String>,
        replication_factor: Option<usize>,
    ) -> Result<(), Errors> {
        let mut keyspaces = self.extract_hash_from_json()?;
        if keyspaces.contains_key(name) {
            return Err(Errors::SyntaxError(
                "El keyspace ya está creado".to_string(),
            ));
        }
        let keyspace = Keyspace::new(replication_strategy, replication_factor);
        keyspaces.insert(name.to_owned(), keyspace);
        self.save_hash_to_json(&keyspaces)?;
        Ok(())
    }

    pub fn alter_keyspace(
        &mut self,
        name: &str,
        replication_strategy: Option<String>,
        replication_factor: Option<usize>,
    ) -> Result<(), Errors> {
        let mut keyspaces = self.extract_hash_from_json()?;
        let keyspace = self.get_keyspace_mutable(&mut keyspaces, name)?;

        if let Some(strategy) = replication_strategy {
            keyspace.set_replication_strategy(strategy);
        }

        if let Some(factor) = replication_factor {
            keyspace.set_replication_factor(factor);
        }
        self.save_hash_to_json(&keyspaces)?;
        Ok(())
    }

    pub fn drop_keyspace(&mut self, name: &str) -> Result<(), Errors> {
        let mut keyspaces = self.extract_hash_from_json()?;
        keyspaces.remove(name);
        self.save_hash_to_json(&keyspaces)?;
        Ok(())
    }

    pub fn get_columns_type(
        &mut self,
        keyspace_name: &str,
        table_name: &str,
    ) -> Result<HashMap<String, DataType>, Errors> {
        let mut keyspaces = self.extract_hash_from_json()?;
        let table = self.get_table_mutable(&mut keyspaces, keyspace_name, table_name)?;
        Ok(table.columns.clone())
    }

    pub fn get_primary_key(
        &mut self,
        keyspace_name: &str,
        table_name: &str,
    ) -> Result<String, Errors> {
        let mut keyspaces = self.extract_hash_from_json()?;
        let table = self.get_table_mutable(&mut keyspaces, keyspace_name, table_name)?;
        Ok(table.primary_key.clone())
    }

    pub fn add_table(
        &mut self,
        keyspace_name: &str,
        table_name: &str,
        primary_key: String,
        columns: HashMap<String, DataType>,
    ) -> Result<(), Errors> {
        let mut keyspaces = self.extract_hash_from_json()?;
        let keyspace = self.get_keyspace_mutable(&mut keyspaces, keyspace_name)?;
        if keyspace.tables.contains_key(table_name) {
            return Err(Errors::SyntaxError("La tabla ya está creada".to_string()));
        }
        let table = Table::new(primary_key, columns);
        keyspace.tables.insert(table_name.to_string(), table);
        self.save_hash_to_json(&keyspaces)?;
        Ok(())
    }

    pub fn delete_table(&mut self, keyspace_name: &str, table_name: &str) -> Result<(), Errors> {
        let mut keyspaces = self.extract_hash_from_json()?;
        let keyspace = self.get_keyspace_mutable(&mut keyspaces, keyspace_name)?;
        if !keyspace.tables.contains_key(table_name) {
            return Err(Errors::SyntaxError(format!(
                "La tabla '{}' no existe en el keyspace '{}'",
                table_name, keyspace_name
            )));
        }
        keyspace.tables.remove(table_name);
        self.save_hash_to_json(&keyspaces)?;
        Ok(())
    }

    fn get_table_mutable<'a>(
        &'a mut self,
        keyspaces: &'a mut HashMap<String, Keyspace>,
        keyspace_name: &str,
        table_name: &str,
    ) -> Result<&'a mut Table, Errors> {
        let keyspace = self.get_keyspace_mutable(keyspaces, keyspace_name)?;
        keyspace.tables.get_mut(table_name).ok_or_else(|| {
            Errors::SyntaxError(format!(
                "La tabla '{}' no existe en el keyspace '{}'",
                table_name, keyspace_name
            ))
        })
    }

    fn get_keyspace_mutable<'a>(
        &'a mut self,
        keyspaces: &'a mut HashMap<String, Keyspace>,
        name: &str,
    ) -> Result<&'a mut Keyspace, Errors> {
        keyspaces
            .get_mut(name)
            .ok_or_else(|| Errors::SyntaxError(format!("El keyspace '{}' no existe", name)))
    }

    fn extract_hash_from_json(&self) -> Result<HashMap<String, Keyspace>, Errors> {
        let filename = format!("{}", self.dir); // Usar la ruta constante
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&filename)
            .map_err(|_| Errors::ServerError("Unable to open or create file".to_string()))?;

        let mut contents = String::new();
        file.read_to_string(&mut contents)
            .map_err(|_| Errors::ServerError("Unable to read file".to_string()))?;

        // Si el archivo está vacío, inicializamos un HashMap
        let existing_keyspaces: HashMap<String, Keyspace> = if contents.is_empty() {
            HashMap::new()
        } else {
            serde_json::from_str(&contents).map_err(|_| {
                Errors::ServerError("Failed to deserialize existing keyspaces".to_string())
            })?
        };
        Ok(existing_keyspaces)
    }

    fn save_hash_to_json(&self, keyspaces: &HashMap<String, Keyspace>) -> Result<(), Errors> {
        let filename = format!("{}", self.dir); // Ruta al archivo JSON
        let json_data = serde_json::to_string_pretty(keyspaces)
            .map_err(|_| Errors::ServerError("Failed to serialize keyspaces".to_string()))?;

        let mut file = OpenOptions::new()
            .write(true)
            .truncate(true) // Limpiar el contenido del archivo antes de escribir los nuevos datos
            .open(&filename)
            .map_err(|_| Errors::ServerError("Unable to open file for writing".to_string()))?;

        file.write_all(json_data.as_bytes())
            .map_err(|_| Errors::ServerError("Failed to write data to file".to_string()))?;

        Ok(())
    }
}

