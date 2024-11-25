use crate::hinted_handoff::handler::Handler;
use crate::hinted_handoff::stored_query::StoredQuery;
use crate::meta_data::meta_data_handler::MetaDataHandler;
use crate::queries::query::{Query, QueryEnum};
use crate::query_delegation::query_serializer::QuerySerializer;
use crate::read_reparation::read_repair::ReadRepair;
use crate::utils::consistency_level::ConsistencyLevel;
use crate::utils::constants::{KEYSPACE_METADATA_PATH, NODES_METADATA_PATH, TIMEOUT_SECS};
use crate::utils::errors::Errors;
use crate::utils::functions::{flush_stream, read_from_stream_no_zero, use_node_meta_data};
use crate::utils::node_ip::NodeIp;
use std::collections::HashMap;
use std::io::{Read, Write};
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
                    Ok((ip, response)) => if tx.send((ip,response)).is_ok() {},
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
                    }
                }
            }
        }
        let final_responses = responses.lock().unwrap();
        self.get_response(final_responses.to_owned())
    }

    fn get_replication(&self) -> Result<usize, Errors> {
        let mut stream = MetaDataHandler::establish_connection()?;
        let instance = MetaDataHandler::get_instance(&mut stream)?;
        let keyspace_metadata = instance.get_keyspace_meta_data_access();
        let nodes_meta_data = instance.get_nodes_metadata_access();
        if self.primary_key.is_none() {
            return nodes_meta_data.get_nodes_quantity(NODES_METADATA_PATH);
        }
        keyspace_metadata.get_replication(
            KEYSPACE_METADATA_PATH.to_string(),
            &self.query.get_keyspace()?,
        )
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
                if stream
                    .write(QuerySerializer::serialize(&query)?.as_slice())
                    .is_err()
                {
                    return Err(Errors::ServerError(String::from(
                        "Unable to send query to node",
                    )));
                };
//<<<<<<< read_repair
                stream.flush().expect("");
                let mut buf = [0; 1024];
                match stream.read(&mut buf) {
                    Ok(n) => Ok((ip, buf[0..n].to_vec())),
                    Err(_) => Err(Errors::ServerError(String::from(
                        "Unable to read from node",
                    ))),
                }
//=====
                flush_stream(&mut stream)?;
                read_from_stream_no_zero(&mut stream)
//>>>>>>> main
            }
            Err(e) => {
                use_node_meta_data(|handler| handler.set_inactive(NODES_METADATA_PATH, &ip))?;
                Handler::store_query(StoredQuery::new(&query)?, ip)?;
                Err(Errors::UnavailableException(e.to_string()))
            }
        }
    }

    fn get_response(&self, responses: HashMap<NodeIp, Vec<u8>>) -> Result<Vec<u8>, Errors> {
        let mut read_repair = ReadRepair::new(&responses)?;
        read_repair.get_response()
    }
}




