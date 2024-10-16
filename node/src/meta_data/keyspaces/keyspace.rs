use std::{collections::HashMap, string};

use serde::{Serialize, Deserialize};

use crate::parsers::tokens::data_type::DataType;


#[derive(Debug, Serialize, Deserialize)]
pub struct Table {
    pub primary_key: String,
    pub columns: HashMap<String, DataType>, //Podria ser <Token::Identifier, DataType> 
}                                           //Como prefieran

impl Table {
    pub fn new(primary_key: String, columns: HashMap<String, DataType>) -> Self {
        Table {
            primary_key,
            columns,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Keyspace {
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