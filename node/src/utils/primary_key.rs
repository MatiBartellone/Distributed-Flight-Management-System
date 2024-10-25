use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct PrimaryKey {
    partition_key: Vec<String>,
    clustering_columns: Vec<String>,
}

impl PrimaryKey {
    pub fn new(partition_key: Vec<String>, clustering_columns: Option<Vec<String>>) -> Self {
        Self {
            partition_key,
            clustering_columns: clustering_columns.unwrap_or_default(),
        }
    }

    pub fn default() -> Self {
        Self {
            partition_key: Vec::new(),
            clustering_columns: Vec::new(),
        }
    }

    pub fn get_partition_key(&self) -> &Vec<String> {
        &self.partition_key
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
}