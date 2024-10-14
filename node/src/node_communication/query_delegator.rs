use std::cmp::Ordering;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use crate::queries::query::Query;
use crate::utils::errors::Errors;
use crate::node_communication::query_serializer::QuerySerializer;


const REPLICATION : i32 = 3;
pub struct QueryDelegator{
    node: i32,
    serialized_query: Vec<u8>,
    consistency: i32
}

impl QueryDelegator {
    pub fn new(node: i32, query: Box<dyn Query>, consistency: i32) -> Result<Self, Errors> {
        Ok(Self{
            node,
            serialized_query: QuerySerializer::serialize(&query)?,
            consistency
        })
    }

    // pub fn send(&self) -> Result<String, Errors> {
    //     let mut responses = Vec::new();
    //     let mut response_quantity = 0;
    //     for ip in self.get_nodes_ip()? {
    //         thread::spawn( move || {
    //             if let Ok(response) = self.send_to_node(ip) {
    //                 response_quantity += 1; // HAY QUE CHEQUEAR DE ALGUNA FORMA SI CUMPLE CON LA CONSISTENCY Y CORTAR ANTES
    //                 responses.push(response);
    //             }
    //         });
    //     }
    //     Ok(self.get_response(responses)?)
    // }
    pub fn send(&self, consistency: usize) -> Result<String, Errors> {
        let serialized_query = self.serialized_query.clone();
        let responses = Arc::new(Mutex::new(Vec::new()));
        let (tx, rx) = mpsc::channel();
        let mut handles = Vec::new();

        for ip in self.get_nodes_ip()? {
            let serialized_query = serialized_query.clone();
            let tx = tx.clone();
            let handle = thread::spawn(move || {
                if let Ok(response) = QueryDelegator::send_to_node(ip, serialized_query) {
                    if tx.send(response).is_ok() {
                    }
                }
            });
            handles.push(handle);
        }

        // Recibir respuestas hasta alcanzar la consistencia
        for _ in 0..consistency {
            if let Ok(response) = rx.recv() {
                let mut res = responses.lock().unwrap();
                res.push(response);
            }
        }

        // Esperar a que todos los threads terminen
        for handle in handles {
            handle.join().unwrap();
        }
        let final_responses = responses.lock().unwrap();
        Ok(self.get_response(final_responses.clone())?)
    }

    fn get_nodes_ip(&self) -> Result<Vec<String>, Errors> {
        let mut ips: Vec<String> = Vec::new();
        for node in self.node..self.node + REPLICATION {
            //ips.push(get_ip(node))      EXTRAE DE METADATA ACCESS
        }
        Ok(ips)
    }

    fn send_to_node(ip: String, serialized_query: Vec<u8>) -> Result<String, Errors>  {
        match TcpStream::connect(ip) {
            Ok(mut stream) => {
                if stream.write(&serialized_query.as_slice()).is_err() {
                    return Err(Errors::ServerError(String::from("Unable to send query to node")));
                };
                stream.flush().expect("");
                let mut buf = [0; 1024];
                match stream.read(&mut buf) {
                    Ok(_) => Ok(String::from_utf8_lossy(&buf).to_string()),
                    Err(_) => Err(Errors::ServerError(String::from("Unable to read from node")))
                }
            },
            Err(e) => {
                Err(Errors::ServerError(e.to_string()))
            }
        }
    }

    fn get_response(&self, responses: Vec<String>) -> Result<String, Errors> {
        let Some(response) = responses.first() else {
            return Err(Errors::ServerError(String::from("No response found")));
        };
        for r in &responses {
            if String::cmp(&r, response) != Ordering::Equal {
                // READ REPAIR
            }
        }
        Ok(response.to_string())
    }
}