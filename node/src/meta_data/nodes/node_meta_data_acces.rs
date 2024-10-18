use std::fs::{File, OpenOptions};

use crate::utils::errors::Errors;

use super::{node_group::NodeGroup, node::Node};

use twox_hash::XxHash32;
use std::hash::{Hasher};




#[derive(Debug)]
pub struct NodesMetaDataAccess;

impl NodesMetaDataAccess {
    fn open(path: &str) -> Result<File, Errors> {
        let file = OpenOptions::new()
        .read(true)  // Permitir lectura
        .write(true) // Permitir escritura
        .create(true) // Crear el archivo si no existe
            .truncate(true)
        .open(path)
        .map_err(|_| Errors::ServerError("Unable to open or create file".to_string()))?;
        Ok(file)
    }

    fn read_node_group(path: &str) -> Result<NodeGroup, Errors> {
        let file = Self::open(path)?;
        let node_group: NodeGroup = serde_json::from_reader(&file)
            .map_err(|_| Errors::ServerError("Failed to read or deserialize NodeGroup".to_string()))?;
        Ok(node_group)
    }

    fn write_node_group(path: &str, node_group: &NodeGroup) -> Result<(), Errors> {
        let file = Self::open(path)?;
        serde_json::to_writer(&file, &node_group)
            .map_err(|_| Errors::ServerError("Failed to write NodeGroup to file".to_string()))?;
        Ok(())
    }

    //el special node es el nodo en el que estamos
    pub fn get_own_ip(path: &str) -> Result<String, Errors> {
        let node_group = Self::read_node_group(path)?;
        Ok(node_group.ip_principal().to_string())
    }

    //Si no hay que delegar, retorna None
    pub fn get_delegation(path: &str, key: String)-> Result<Option<Node>, Errors> {
        let hasshing_key = hash_string_murmur3(&key);
        let node_group = Self::read_node_group(path)?;
        let pos = hasshing_key % node_group.len_nodes();
        Ok(node_group.get_node(pos))
    }
}

fn hash_string_murmur3(input: &str) -> usize {
        let mut hasher = XxHash32::with_seed(0); 
        hasher.write(input.as_bytes());
        hasher.finish() as usize
    }
