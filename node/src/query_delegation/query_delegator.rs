use crate::hinted_handoff::handler::Handler;
use crate::hinted_handoff::stored_query::StoredQuery;
use crate::meta_data::meta_data_handler::{use_keyspace_meta_data, use_node_meta_data};
use crate::queries::query::{Query, QueryEnum};
use crate::query_delegation::query_serializer::QuerySerializer;
use crate::read_reparation::read_repair::ReadRepair;
use crate::utils::config_constants::TIMEOUT_SECS;
use crate::utils::consistency_level::ConsistencyLevel;
use crate::utils::constants::{KEYSPACE_METADATA_PATH, NODES_METADATA_PATH};
use crate::utils::errors::Errors;
use crate::utils::functions::{read_from_stream_no_zero, write_to_stream};
use crate::utils::types::node_ip::NodeIp;
use std::collections::HashMap;
use std::net::TcpStream;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::Duration;

pub struct QueryDelegator {
    #[allow(dead_code)]
    primary_key: Option<Vec<String>>,
    query: Box<dyn Query>,
    consistency: ConsistencyLevel,
}

impl QueryDelegator {
    pub fn new(
        primary_key: Option<Vec<String>>,
        query: Box<dyn Query>,
        consistency: ConsistencyLevel,
    ) -> Self {
        Self {
            primary_key,
            query,
            consistency,
        }
    }

    pub fn send(&self) -> Result<Vec<u8>, Errors> {
        let responses = Arc::new(Mutex::new(HashMap::new()));
        let (tx, rx) = mpsc::channel();
        let error = Arc::new(Mutex::new(None));

        for ip in self.get_nodes_ip()? {
            let Some(query_enum) = QueryEnum::from_query(&self.query) else {
                return Err(Errors::ServerError(String::from(
                    "QueryEnum does not exist",
                )));
            };
            let tx = tx.clone();
            let error = Arc::clone(&error);
            let _ = thread::spawn(move || {
                match QueryDelegator::send_to_node(ip, query_enum.into_query()) {
                    Ok((ip, response)) => if tx.send((ip, response)).is_ok() {},
                    Err(e) => {
                        let mut error_lock = error.lock().unwrap();
                        *error_lock = Some(e);
                    }
                }
            });
        }
        // get responses until n = consistency
        let timeout = Duration::from_secs(TIMEOUT_SECS);
        for _ in 0..self.consistency.get_consistency(self.get_replication()?) {
            match rx.recv_timeout(timeout) {
                Ok((ip, response)) => {
                    let mut res = responses.lock().unwrap();
                    res.insert(ip, response);
                }
                _ => {
                    return match error.lock().unwrap().take() {
                        Some(e) => Err(e),
                        None => Err(Errors::ReadTimeout(String::from("Timeout"))),
                    };
                }
            }
        }
        let final_responses = responses.lock().unwrap();
        self.get_response(final_responses.to_owned())
    }

    fn get_replication(&self) -> Result<usize, Errors> {
        if self.primary_key.is_none() {
            return use_node_meta_data(|handler| handler.get_nodes_quantity(NODES_METADATA_PATH));
        }
        use_keyspace_meta_data(|handler| {
            handler.get_replication(
                KEYSPACE_METADATA_PATH.to_string(),
                &self.query.get_keyspace()?,
            )
        })
    }

    fn get_nodes_ip(&self) -> Result<Vec<NodeIp>, Errors> {
        use_node_meta_data(|handler| {
            handler.get_partition_full_ips(
                NODES_METADATA_PATH,
                &self.primary_key,
                self.query.get_keyspace()?,
            )
        })
    }

    pub fn send_to_node(ip: NodeIp, query: Box<dyn Query>) -> Result<(NodeIp, Vec<u8>), Errors> {
        match TcpStream::connect(ip.get_query_delegation_socket()) {
            Ok(mut stream) => {
                write_to_stream(&mut stream, QuerySerializer::serialize(&query)?.as_slice())?;
                //flush_stream(&mut stream)?;
                let response = read_from_stream_no_zero(&mut stream)?;
                if let Some(e) = Errors::deserialize(response.as_slice()) {
                    return Err(e);
                }
                Ok((ip, response))
            }
            Err(e) => {
                use_node_meta_data(|handler| handler.set_inactive(NODES_METADATA_PATH, &ip))?;
                Handler::store_query(StoredQuery::new(&query)?, ip)?;
                Err(Errors::UnavailableException(e.to_string()))
            }
        }
    }

    fn get_response(&self, responses: HashMap<NodeIp, Vec<u8>>) -> Result<Vec<u8>, Errors> {
        let expected_bytes = 2i32.to_be_bytes();
        // Filtrar las respuestas tipo row
        let responses_to_repair: HashMap<_, _> = responses
            .iter()
            .filter(|(_, response)| response.starts_with(&expected_bytes))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        // Si hay respuestas tipo row, delega a read repair
        if !responses_to_repair.is_empty() {
            let mut read_repair = ReadRepair::new(&responses_to_repair)?; 
            return read_repair.get_response();
        }
        let response = responses.values().next().unwrap_or(&Vec::new()).to_vec();
        Ok(response)
    }
}
