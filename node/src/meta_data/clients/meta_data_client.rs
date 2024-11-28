use super::client::Client;
use crate::utils::constants::CLIENT_METADATA_PATH;
use crate::utils::errors::Errors;
use crate::utils::errors::Errors::ServerError;
use crate::utils::functions::deserialize_from_slice;
use std::fs::remove_file;
use std::io::Write;
use std::{
    fs::{self, File},
    thread,
};

pub struct ClientMetaDataAcces {}

impl ClientMetaDataAcces {
    fn open_file(path: &str) -> Result<File, Errors> {
        fs::create_dir_all(CLIENT_METADATA_PATH).map_err(|e| ServerError(e.to_string()))?;
        let file =
            File::create(path).map_err(|_| ServerError("Unable to create file".to_string()))?;
        Ok(file)
    }

    fn get_client(path: &str) -> Result<Client, Errors> {
        let content = fs::read_to_string(path)
            .map_err(|_| ServerError(String::from("Error reading file")))?;
        let client: Client = deserialize_from_slice(content.as_bytes())?;
        Ok(client)
    }

    fn save_to_json(file: &mut File, client: &Client) -> Result<(), Errors> {
        let serialized_client = serde_json::to_vec(client)
            .map_err(|_| ServerError("Unable to serialize client".to_string()))?;
        file.write_all(serialized_client.as_slice())
            .map_err(|_| ServerError("Unable to write file".to_string()))?;

        Ok(())
    }

    fn get_file_path(path: &str) -> String {
        format!("{}{}.json", path, Self::thread_id_string())
    }

    fn thread_id_string() -> String {
        let thread_id = thread::current().id();
        format!("{:?}", thread_id)
    }

    pub fn add_new_client(&self, path: String) -> Result<(), Errors> {
        let new_client = Client::new();
        let file_path = Self::get_file_path(path.as_str());
        let mut file = Self::open_file(file_path.as_str())?;
        Self::save_to_json(&mut file, &new_client)?;
        Ok(())
    }

    fn process_clients_alter<F>(path: String, process_fn: F) -> Result<(), Errors>
    where
        F: Fn(&mut Client) -> bool,
    {
        let file_path = Self::get_file_path(path.as_str());
        let mut client = Self::get_client(&file_path)?;
        let mut file = Self::open_file(file_path.as_str())?;
        process_fn(&mut client);
        Self::save_to_json(&mut file, &client)?;
        Ok(())
    }

    pub fn authorize_client(&self, path: String) -> Result<(), Errors> {
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
        remove_file(Self::get_file_path(path.as_str()))
            .map_err(|_| ServerError("Unable to delete file".to_string()))
    }

    fn process_client_view<F, T>(path: String, process_fn: F) -> Result<T, Errors>
    where
        F: Fn(&Client) -> Result<T, Errors>,
    {
        let file_path = Self::get_file_path(path.as_str());
        let client = Self::get_client(&file_path)?;
        process_fn(&client)
    }

    pub fn get_keyspace(&self, path: String) -> Result<Option<String>, Errors> {
        Self::process_client_view(path, |client| Ok(client.get_keyspace()))
    }

    pub fn is_authorized(&self, path: String) -> Result<bool, Errors> {
        Self::process_client_view(path, |client| Ok(client.is_authorized()))
    }

    pub fn has_started(&self, path: String) -> Result<bool, Errors> {
        Self::process_client_view(path, |client| Ok(client.has_started()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let _ = remove_file(path);
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
            .has_started(name.to_owned())
            .expect("Failed to get startup");
        assert!(!key);
        cleanup_temp_file(name);
    }

    #[test]
    fn test_authorize_client() {
        let name = "test_autorize.json";
        create_test_file(name).expect("Failed to create test file");
        let meta_data = ClientMetaDataAcces {};
        meta_data
            .authorize_client(name.to_owned())
            .expect("Failed to authorize client");
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
        meta_data
            .startup_client(name.to_string())
            .expect("Failed to startup client");
        let key = meta_data
            .has_started(name.to_owned())
            .expect("Failed to get startup");
        assert!(key);
        cleanup_temp_file(name);
    }

    #[test]
    fn test_use_keyspace() {
        let name = "test_use_keyspace.json";
        create_test_file(name).expect("Failed to create test file");
        let meta_data = ClientMetaDataAcces {};
        meta_data
            .use_keyspace(name.to_string(), "keyspace_new")
            .expect("Failed to use keyspace client");
        let key = meta_data
            .get_keyspace(name.to_owned())
            .expect("Failed to get keyspace");
        assert_eq!(key, Some("keyspace_new".to_owned()));
        cleanup_temp_file(name);
    }
}
