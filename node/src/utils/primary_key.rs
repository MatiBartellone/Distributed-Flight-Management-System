use std::collections::HashSet;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct PrimaryKey {
    pub partition_keys: Vec<String>,
    pub clustering_columns: Vec<String>,
}

impl PrimaryKey {
    pub fn new(partition_key: Vec<String>, clustering_columns: Option<Vec<String>>) -> Self {
        Self {
            partition_keys: partition_key,
            clustering_columns: clustering_columns.unwrap_or_default(),
        }
    }

    pub fn new_empty() -> Self {
        Self {
            partition_keys: Vec::new(),
            clustering_columns: Vec::new(),
        }
    }

    pub fn get_partition_key(&self) -> &Vec<String> {
        &self.partition_keys
    }

    pub fn get_clustering_columns(&self) -> &Vec<String> {
        &self.clustering_columns
    }

    pub fn get_full_primary_key(&self) -> Vec<String> {
        let mut primary_key = Vec::new();
        primary_key.extend_from_slice(self.get_partition_key());
        primary_key.extend_from_slice(self.get_clustering_columns());
        primary_key
    }

    pub fn add_partition_key(&mut self, partition_key: String) {
        self.partition_keys.push(partition_key);
    }
    pub fn add_clustering_column(&mut self, clustering_column: String) {
        self.clustering_columns.push(clustering_column);
    }

    pub fn get_full_pk_in_hash(&self) -> HashSet<String> {
        let mut full_hash = HashSet::new();
        full_hash.extend(self.get_partition_key().to_owned());
        full_hash.extend(self.get_clustering_columns().to_owned());
        full_hash
    }
}