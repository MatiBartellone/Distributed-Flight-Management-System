use crate::meta_data::meta_data_handler::MetaDataHandler;
use crate::node_communication::query_serializer::QuerySerializer;
use crate::queries::query::{Query, QueryEnum};
use crate::utils::consistency_level::ConsistencyLevel;
use crate::utils::constants::{nodes_meta_data_path, KEYSPACE_METADATA, NODES_METADATA};
use crate::utils::errors::Errors;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;

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
                match QueryDelegator::send_to_node(ip, query_enum.into_query()) {
                    Ok(response) => {
                        if tx.send(response).is_ok() {
                        }
                    },
                    Err(e) => {
                        if tx.send(e.to_string().as_bytes().to_vec()).is_ok() {
                        }
                    }
                }
            });
            handles.push(handle);
        }
        // Recibir respuestas hasta alcanzar la consistencia
        for _ in 0..self.consistency.get_consistency(self.get_replication()?) {
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
        self.get_response(final_responses.to_owned())
    }

    fn get_replication(&self) -> Result<usize, Errors> {
        let mut stream = MetaDataHandler::establish_connection()?;
        let instance =
            MetaDataHandler::get_instance(&mut stream)?;
        let keyspace_metadata = instance.get_keyspace_meta_data_access();
        let nodes_meta_data = instance.get_nodes_metadata_access();
        if self.primary_key.is_none() {
            return nodes_meta_data.get_nodes_quantity(NODES_METADATA)
        }
        keyspace_metadata.get_replication(KEYSPACE_METADATA.to_string(), &self.query.get_keyspace()?)
    }

    fn get_nodes_ip(&self) -> Result<Vec<String>, Errors> {
        let mut stream = MetaDataHandler::establish_connection()?;
        let nodes_meta_data =
            MetaDataHandler::get_instance(&mut stream)?.get_nodes_metadata_access();
        let ips = nodes_meta_data.get_partition_full_ips(
            nodes_meta_data_path().as_ref(),
            &self.primary_key,
            self.query.get_keyspace()?,
        )?;
        dbg!(&ips);
        Ok(ips)
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
