use std::{fs::{File, OpenOptions, self}, io::BufReader, path::Path, thread};
use std::io::Write;
use serde_json::Deserializer;

use crate::utils::errors::Errors;

use super::client::Client;



pub struct ClientMetaDataAcces{}

impl ClientMetaDataAcces {
    fn open_file(path: &str) -> Result<File, Errors> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .map_err(|_| Errors::ServerError("Unable to open or create file".to_string()))?;
        Ok(file)
    }

    fn extract_iterate_from_json(path: &str) -> Result<impl Iterator<Item = Result<Client, Errors>>, Errors> {
        let file = Self::open_file(&path)?; // Abrir archivo
        let reader = BufReader::new(file); // Crear un lector bufferizado
        let stream = Deserializer::from_reader(reader)
            .into_iter::<Client>()
            .map(|result| {
                result.map_err(|_| Errors::ServerError("Unable to deserialize client".to_string()))
            });
        Ok(stream)
    }

    fn save_to_json(
        file: &mut File,
        client: &Client,
    ) -> Result<(), Errors> {
        let serialized_client = serde_json::to_string(client)
            .map_err(|_| Errors::ServerError("Unable to serialize client".to_string()))?;
        writeln!(file, "{}", serialized_client)
            .map_err(|_| Errors::ServerError("Unable to write file".to_string()))?;
        
        Ok(())
    }

    fn create_aux_json(number: &str) -> Result<String, Errors> {
        let filename = format!("{}.json", number);
        let path = Path::new(&filename);
        let _ = File::create(&path)
        .map_err(|_| Errors::ServerError("Unable to create aux file".to_string()))?;
        Ok(filename)
    }

    fn thread_id_string() -> String {
        let thread_id = thread::current().id(); 
        format!("{:?}", thread_id)
    }

    pub fn add_new_client(path: String) -> Result<(), Errors> {
        let new_client = Client::new();
        let mut file = Self::open_file(&path)?;
        Self::save_to_json(&mut file, &new_client)?;
        Ok(())
    }

    fn process_clients_alter<F>(path: String, process_fn: F) -> Result<(), Errors>
    where
        F: Fn(&mut Client) -> Result<(), Errors>,
    {
        let id_client = Self::thread_id_string();
        let aux_file = Self::create_aux_json(&id_client)?; 
        let mut file = Self::open_file(&aux_file)?;
        let clients = Self::extract_iterate_from_json(&path)?;

        for client_result in clients {
            let mut client = client_result.map_err(|_| Errors::ServerError("Error reading client".to_string()))?;
            if client.is_id(&id_client) {
                process_fn(&mut client)?; // Aplicar la lógica que cambia
            }
            Self::save_to_json(&mut file, &client)?;
        }

        fs::rename(aux_file, path).map_err(|_| Errors::ServerError("Error renaming file".to_string()))?;
        Ok(())
    }

    pub fn authorize_client(path: String) -> Result<(), Errors> {
        Self::process_clients_alter(path, |client| {
            client.authorize();
            Ok(())
        })
    }

    pub fn startup_client(path: String) -> Result<(), Errors> {
        Self::process_clients_alter(path, |client| {
            client.start_up();
            Ok(())
        })
    }

    pub fn use_keyspace(path: String, keyspace: &str) -> Result<(), Errors> {
        Self::process_clients_alter(path.to_string(), |client| {
            client.set_keyspace(keyspace.to_string());
            Ok(())
        })
    }


    fn process_client_view<F, T>(path: String, process_fn: F) -> Result<T, Errors>
    where
        F: Fn(&Client) -> Result<T, Errors>,
    {
        let id_client = Self::thread_id_string();
        let clients = Self::extract_iterate_from_json(&path)?;

        for client_result in clients {
            let client = client_result.map_err(|_| Errors::ServerError("Error reading client".to_string()))?;
            if client.is_id(&id_client) {
                return process_fn(&client);
            }
        }

        Err(Errors::ServerError("Error, client not found".to_string()))
    }

    pub fn get_keyspace(path: String) -> Result<Option<String>, Errors> {
        Self::process_client_view(path, |client| Ok(client.get_keyspace()))
    }

    pub fn is_authorized(path: String) -> Result<bool, Errors> {
        Self::process_client_view(path, |client| Ok(client.is_authorized()))
    }

    pub fn had_started(path: String) -> Result<bool, Errors> {
        Self::process_client_view(path, |client| Ok(client.has_started()))
    }
}


#[cfg(test)]
mod tests {
    use super::*;
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
}