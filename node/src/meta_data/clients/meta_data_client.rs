use serde_json::Deserializer;
use std::io::Write;
use std::{
    fs::{self, File, OpenOptions},
    io::BufReader,
    path::Path,
    thread,
};

use crate::utils::errors::Errors;

use super::client::Client;

pub struct ClientMetaDataAcces {}

impl ClientMetaDataAcces {
    fn open_file(path: &str) -> Result<File, Errors> {
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .truncate(false)
            .open(path)
            .map_err(|_| Errors::ServerError("Unable to open or create file".to_string()))?;
        Ok(file)
    }

    fn extract_iterate_from_json(
        path: &str,
    ) -> Result<impl Iterator<Item = Result<Client, Errors>>, Errors> {
        let file = Self::open_file(path)?; // Abrir archivo
        let reader = BufReader::new(file); // Crear un lector bufferizado
        let stream = Deserializer::from_reader(reader)
            .into_iter::<Client>()
            .map(|result| {
                result.map_err(|_| Errors::ServerError("Unable to deserialize client".to_string()))
            });
        Ok(stream)
    }

    fn save_to_json(file: &mut File, client: &Client) -> Result<(), Errors> {
        let serialized_client = serde_json::to_string(client)
            .map_err(|_| Errors::ServerError("Unable to serialize client".to_string()))?;
        writeln!(file, "{}", serialized_client)
            .map_err(|_| Errors::ServerError("Unable to write file".to_string()))?;

        Ok(())
    }

    fn create_aux_json(number: &str) -> Result<String, Errors> {
        let filename = format!("{}.json", number);
        let path = Path::new(&filename);
        let _ = File::create(path)
            .map_err(|_| Errors::ServerError("Unable to create aux file".to_string()))?;
        Ok(filename)
    }

    fn thread_id_string() -> String {
        let thread_id = thread::current().id();
        format!("{:?}", thread_id)
    }

    pub fn add_new_client(&self, path: String) -> Result<(), Errors> {
        let new_client = Client::new();
        let mut file = Self::open_file(&path)?;
        Self::save_to_json(&mut file, &new_client)?;
        Ok(())
    }

    fn process_clients_alter<F>(path: String, process_fn: F) -> Result<(), Errors>
    where
        F: Fn(&mut Client) -> bool,
    {
        let id_client = Self::thread_id_string();
        let aux_file = Self::create_aux_json(&id_client)?;
        let mut file = Self::open_file(&aux_file)?;
        let clients = Self::extract_iterate_from_json(&path)?;

        for client_result in clients {
            let mut client = client_result
                .map_err(|_| Errors::ServerError("Error reading client".to_string()))?;
            if client.is_id(&id_client) && !process_fn(&mut client) {
                continue;
            }
            Self::save_to_json(&mut file, &client)?;
        }

        fs::rename(aux_file, path)
            .map_err(|_| Errors::ServerError("Error renaming file".to_string()))?;
        Ok(())
    }

    pub fn authorize_client(&self, path: String) -> Result<(), Errors>{
        Self::process_clients_alter(path, |client| {
            client.authorize();
            true
        })
    }

    pub fn startup_client(&self, path: String) -> Result<(), Errors> {
        Self::process_clients_alter(path, |client| {
            client.start_up();
            true
        })
    }

    pub fn use_keyspace(&self, path: String, keyspace: &str) -> Result<(), Errors> {
        Self::process_clients_alter(path.to_string(), |client| {
            client.set_keyspace(keyspace.to_string());
            true
        })
    }

    pub fn delete_client(&self, path: String) -> Result<(), Errors> {
        Self::process_clients_alter(path, |_| false)
    }

    fn process_client_view<F, T>(path: String, process_fn: F) -> Result<T, Errors>
    where
        F: Fn(&Client) -> Result<T, Errors>,
    {
        let id_client = Self::thread_id_string();
        let clients = Self::extract_iterate_from_json(&path)?;

        for client_result in clients {
            let client = client_result
                .map_err(|_| Errors::ServerError("Error reading client".to_string()))?;
            if client.is_id(&id_client) {
                return process_fn(&client);
            }
        }

        Err(Errors::ServerError("Error, client not found".to_string()))
    }

    pub fn get_keyspace(&self, path: String) -> Result<Option<String>, Errors> {
        Self::process_client_view(path, |client| Ok(client.get_keyspace()))
    }

    pub fn is_authorized(&self, path: String) -> Result<bool, Errors> {
        Self::process_client_view(path, |client| Ok(client.is_authorized()))
    }

    pub fn had_started(&self, path: String) -> Result<bool, Errors> {
        Self::process_client_view(path, |client| Ok(client.has_started()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::{self, File};
    use std::io::{self, BufRead};
    use std::path::Path;

    fn create_test_file(name: &str) -> Result<(), Errors> {
        let meta_data = ClientMetaDataAcces {};
        meta_data.add_new_client(name.to_string())?;
        let path_for_thread = name.to_string();
        let handle = thread::spawn(move || {
            meta_data
                .add_new_client(path_for_thread)
                .expect("Failed to add client in thread");
        });
        handle.join().expect("Failed to join thread");
        Ok(())
    }

    fn cleanup_temp_file(path: &str) {
        let _ = fs::remove_file(path);
    }

    fn count_lines_in_file(file_path: &str) -> io::Result<usize> {
        let file = File::open(file_path)?;
        let reader = BufReader::new(file);
        let line_count = reader.lines().count();
        Ok(line_count)
    }

    #[test]
    fn test_add_new_client() {
        let name = "test_add_client.json";
        create_test_file(name).expect("Failed to create test file"); //ya se hacen dos adds dentro de la creación
        let file_exists = Path::new(&name).exists();
        assert!(file_exists);
        let line_count = count_lines_in_file(name).expect("Failed to count lines in file");
        assert_eq!(
            line_count, 2,
            "Expected 2 lines in the file, found {}",
            line_count
        );
        cleanup_temp_file(name);
    }

    #[test]
    fn test_get_keyspace() {
        let name = "test_get_keyspace.json";
        create_test_file(name).expect("Failed to create test file");
        let meta_data = ClientMetaDataAcces {};
        let key = meta_data
            .get_keyspace(name.to_owned())
            .expect("Failed to get keyspace");
        assert_eq!(key, None);
        cleanup_temp_file(name);
    }

    #[test]
    fn test_is_authorized() {
        let name = "test_is_authorized.json";
        create_test_file(name).expect("Failed to create test file");
        let meta_data = ClientMetaDataAcces {};
        let key = meta_data
            .is_authorized(name.to_owned())
            .expect("Failed to get autoritzation");
        assert!(!key);
        cleanup_temp_file(name);
    }

    #[test]
    fn test_had_started() {
        let name = "test_had_started.json";
        create_test_file(name).expect("Failed to create test file");
        let meta_data = ClientMetaDataAcces {};
        let key = meta_data
            .had_started(name.to_owned())
            .expect("Failed to get startup");
        assert!(!key);
        cleanup_temp_file(name);
    }

    #[test]
    fn test_authorize_client() {
        let name = "test_autorize.json";
        create_test_file(name).expect("Failed to create test file");
        let meta_data = ClientMetaDataAcces {};
        meta_data.authorize_client(name.to_owned()).expect("Failed to authorize client");
        let key = meta_data
            .is_authorized(name.to_owned())
            .expect("Failed to get autoritzation");
        assert!(key);
        cleanup_temp_file(name);
    }

    #[test]
    fn test_startup_client() {
        let name = "test_startup_client.json";
        create_test_file(name).expect("Failed to create test file");
        let meta_data = ClientMetaDataAcces {};
        meta_data.startup_client(name.to_string()).expect("Failed to startup client");
        let key = meta_data
            .had_started(name.to_owned())
            .expect("Failed to get startup");
        assert!(key);
        cleanup_temp_file(name);
    }

    #[test]
    fn test_use_keyspace() {
        let name = "test_use_keyspace.json";
        create_test_file(name).expect("Failed to create test file");
        let meta_data = ClientMetaDataAcces {};
        meta_data.use_keyspace(name.to_string(), "keyspace_new").expect("Failed to use keyspace client");
        let key = meta_data
            .get_keyspace(name.to_owned())
            .expect("Failed to get keyspace");
        assert_eq!(key, Some("keyspace_new".to_owned()));
        cleanup_temp_file(name);
    }

    #[test]
    fn test_delete_client() {
        let name = "test_delete_client.json";
        create_test_file(name).expect("Failed to create test file");
        let meta_data = ClientMetaDataAcces {};
        meta_data.delete_client(name.to_string()).expect("Failed to delete client");
        let key = meta_data.get_keyspace(name.to_owned());
        assert!(key.is_err());
        let line_count = count_lines_in_file(name).expect("Failed to count lines in file");
        assert_eq!(
            line_count, 1,
            "Expected 1 lines in the file, found {}",
            line_count
        );
        cleanup_temp_file(name);
    }
}
