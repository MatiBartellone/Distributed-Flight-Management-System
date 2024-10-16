use std::collections::HashMap;

use serde::{Serialize, Deserialize};

use super::table::Table;



#[derive(Debug, Serialize, Deserialize)]
pub struct Keyspace {
    name: String,
    pub tables: HashMap<String, Table>,
    replication_strategy: String,
    replication_factor: usize,
}

impl Keyspace {
    pub fn new(
        replication_strategy: Option<String>,   // Puede recibir la estrategia o no
        replication_factor: Option<usize>,     // Puede recibir el factor o no
    ) -> Keyspace {
        let strategy = match replication_strategy {
            Some(strategy) => strategy,
            None => "Simple Replication".to_string(),  // Valor predeterminado
        };

        let factor = match replication_factor {
            Some(factor) => factor,
            None => 3,  // Valor predeterminado
        };

        Keyspace {
            tables: HashMap::new(),  // Inicializamos el HashMap de tablas vacío
            replication_strategy: strategy,
            replication_factor: factor,
        }
    }

    pub fn set_replication_strategy(&mut self, strategy: String) {
        self.replication_strategy = strategy;
    }

    pub fn set_replication_factor(&mut self, factor: usize) {
        self.replication_factor = factor;
    }
}