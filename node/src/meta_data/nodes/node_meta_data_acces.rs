use super::cluster::Cluster;
use crate::{utils::{errors::Errors, constants::KEYSPACE_METADATA}, meta_data::keyspaces::keyspace_meta_data_acces::KeyspaceMetaDataAccess};
use murmur3::murmur3_32;
use std::{
    fs::{File, OpenOptions},
    io::Cursor,
};

#[derive(Debug)]
pub struct NodesMetaDataAccess;

impl NodesMetaDataAccess {
    fn open(path: &str) -> Result<File, Errors> {
        let file = OpenOptions::new()
            .read(true) // Permitir lectura
            .write(true) // Permitir escritura
            .create(true)
            .truncate(false)
            .open(path)
            .map_err(|_| Errors::ServerError("Unable to open or create file".to_string()))?;
        Ok(file)
    }

    fn read_cluster(path: &str) -> Result<Cluster, Errors> {
        let file = Self::open(path)?;
        let cluster: Cluster = serde_json::from_reader(&file).map_err(|_| {
            Errors::ServerError("Failed to read or deserialize Cluster".to_string())
        })?;
        Ok(cluster)
    }

    pub fn write_cluster(path: &str, cluster: &Cluster) -> Result<(), Errors> {
        //let file = Self::open(path)?;
        let file = File::create(path)
            .map_err(|_| Errors::ServerError("Failed to open file for writing".to_string()))?;
        serde_json::to_writer(&file, &cluster)
            .map_err(|_| Errors::ServerError("Failed to write Cluster to file".to_string()))?;
        Ok(())
    }

    pub fn get_own_ip(&self, path: &str) -> Result<String, Errors> {
        let cluster = Self::read_cluster(path)?;
        Ok(cluster.get_own_ip().to_string())
    }
    pub fn get_own_ip_(path: &str) -> Result<String, Errors> {
        let cluster = Self::read_cluster(path)?;
        Ok(cluster.get_own_ip().to_string())
    }

    //Si no hay que delegar, retorna None
    /*pub fn get_delegation(path: &str, key: Option<String>)-> Result<Option<Vec<String>>, Errors> {
        if let some
        let hasshing_key = hash_string_murmur3(&key);
        let cluster = Self::read_cluster(path)?;
        let pos = hasshing_key % cluster.len_nodes();
        Ok(cluster.get_node(pos))
    }*/

    pub fn get_partition_ips(
        &self,
        path: &str,
        primary_key: &Option<Vec<String>>,
        keyspace: String
    ) -> Result<Vec<String>, Errors> { 
        let cluster = Self::read_cluster(path)?;
        if let Some(primary_key) = primary_key {
            let hashing_key = hash_string_murmur3(&primary_key.join(""));
            let pos = hashing_key % cluster.len_nodes();
            let replication = KeyspaceMetaDataAccess::get_replication(KEYSPACE_METADATA.to_owned(), &keyspace)?;
            Ok(cluster.get_nodes(pos, replication))
        } else {
            // todo, todas las ips
            Ok(cluster.get_all_ips())
        }
    }
}

fn hash_string_murmur3(input: &str) -> usize {
    let mut buffer = Cursor::new(input.as_bytes());
    let hash = murmur3_32(&mut buffer, 0).expect("Unable to compute hash");
    hash as usize
}
