use crate::meta_data::nodes::node_meta_data_acces::NodesMetaDataAccess;
use crate::node_communication::query_serializer::QuerySerializer;
use crate::queries::query::{Query, QueryEnum};
use crate::utils::consistency_level::ConsistencyLevel;
use crate::utils::constants::{nodes_meta_data_path, QUERY_DELEGATION_PORT};
use crate::utils::errors::Errors;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;

const REPLICATION: i32 = 3;
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
        let responses = Arc::new(Mutex::new(Vec::new()));
        let (tx, rx) = mpsc::channel();
        let mut handles = Vec::new();

        for ip in self.get_nodes_ip()? {
            let Some(query_enum) = QueryEnum::from_query(&self.query) else {
                return Err(Errors::ServerError(String::from(
                    "QueryEnum does not exist",
                )));
            };
            let tx = tx.clone();
            let handle = thread::spawn(move || {
                if let Ok(response) = QueryDelegator::send_to_node(ip, query_enum.into_query()) {
                    if tx.send(response).is_ok() {}
                }
            });
            handles.push(handle);
        }

        // Recibir respuestas hasta alcanzar la consistencia
        for _ in 0..self.consistency.get_consistency(REPLICATION as usize) {
            if let Ok(response) = rx.recv() {
                let mut res = responses.lock().unwrap();
                res.push(response);
            }
        }

        // // Esperar a que todos los threads terminen?
        // for handle in handles {
        //     handle.join().unwrap();
        // }
        let final_responses = responses.lock().unwrap();
        self.get_response(final_responses.clone())
    }

    fn get_nodes_ip(&self) -> Result<Vec<String>, Errors> {
        let ips = NodesMetaDataAccess::get_partition_ips(
            nodes_meta_data_path().as_ref(),
            &None, //&self.primary_key,
        )?;
        let mut full_ips = Vec::new();
        for ip in ips {
            full_ips.push(format!("{}:{}", ip, QUERY_DELEGATION_PORT));
        }
        Ok(full_ips)
    }

    fn send_to_node(ip: String, query: Box<dyn Query>) -> Result<Vec<u8>, Errors> {
        match TcpStream::connect(ip) {
            Ok(mut stream) => {
                if stream
                    .write(QuerySerializer::serialize(&query)?.as_slice())
                    .is_err()
                {
                    return Err(Errors::ServerError(String::from(
                        "Unable to send query to node",
                    )));
                };
                stream.flush().expect("");
                let mut buf = [0; 1024];
                match stream.read(&mut buf) {
                    Ok(n) => Ok(buf[0..n].to_vec()),
                    Err(_) => Err(Errors::ServerError(String::from(
                        "Unable to read from node",
                    ))),
                }
            }
            Err(e) => Err(Errors::ServerError(e.to_string())),
        }
    }

    fn get_response(&self, responses: Vec<Vec<u8>>) -> Result<Vec<u8>, Errors> {
        let Some(response) = responses.first() else {
            return Err(Errors::ServerError(String::from("No response found")));
        };
        for r in &responses {
            if r != response {
                // READ REPAIR
            }
        }
        Ok(response.clone())
    }
}
