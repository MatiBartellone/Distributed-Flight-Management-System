use std::cmp::Ordering;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use crate::frame::Frame;
use crate::queries::query::Query;
use crate::utils::consistency_level::ConsistencyLevel;
use crate::utils::errors::Errors;
//use crate::node_communication::query_serializer::QuerySerializer;


const REPLICATION : i32 = 3;
pub struct QueryDelegator{
    node: i32,
    request_frame: Frame,
    consistency: ConsistencyLevel
}

impl QueryDelegator {
    pub fn new(node: i32, request_frame: Frame, consistency: ConsistencyLevel) -> Self {
        Self{
            node,
            request_frame,
            consistency
        }
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
    pub fn send(&self) -> Result<Frame, Errors> {
        let request_frame = self.request_frame.clone();
        let responses = Arc::new(Mutex::new(Vec::new()));
        let (tx, rx) = mpsc::channel();
        let mut handles = Vec::new();

        for ip in self.get_nodes_ip()? {
            let request_frame = request_frame.clone();
            let tx = tx.clone();
            let handle = thread::spawn(move || {
                if let Ok(response) = QueryDelegator::send_to_node(ip, request_frame) {
                    if tx.send(response).is_ok() {
                    }
                }
            });
            handles.push(handle);
        }

        // Recibir respuestas hasta alcanzar la consistencia
        for _ in 0..self.consistency.get_consistency(3) {
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
        return Ok(vec!["127.0.0.2:8080".to_string()]);
        let mut ips: Vec<String> = Vec::new();
        for node in self.node..self.node + REPLICATION {
            //ips.push(get_ip(node))      EXTRAE DE METADATA ACCESS
        }
        Ok(ips)
    }

    fn send_to_node(ip: String, request_frame: Frame) -> Result<Frame, Errors>  {
        match TcpStream::connect(ip) {
            Ok(mut stream) => {
                if stream.write(request_frame.to_bytes().as_slice()).is_err() {
                    return Err(Errors::ServerError(String::from("Unable to send query to node")));
                };
                stream.flush().expect("");
                let mut buf = [0; 1024];
                match stream.read(&mut buf) {
                    Ok(n) => {
                        let frame = Frame::parse_frame(&buf[..n]).expect("Error parsing frame");
                        Ok(frame)
                    }
                    Err(_) => Err(Errors::ServerError(String::from("Unable to read from node")))
                }
            },
            Err(e) => {
                Err(Errors::ServerError(e.to_string()))
            }
        }
    }

    fn get_response(&self, responses: Vec<Frame>) -> Result<Frame, Errors> {
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