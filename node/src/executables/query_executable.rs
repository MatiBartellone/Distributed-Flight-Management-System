use std::fs::File;
use serde::{Deserialize, Serialize};
use crate::executables::executable::Executable;
use crate::frame::Frame;
use crate::node_communication::query_delegator::QueryDelegator;
use crate::queries::query::Query;
use crate::response_builders::frame_builder::FrameBuilder;
use crate::utils::consistency_level::ConsistencyLevel;
use crate::utils::errors::Errors;
use crate::utils::parser_constants::RESULT;

pub struct QueryExecutable{
    query: Box<dyn Query>,
    consistency: ConsistencyLevel
}

impl QueryExecutable {
    pub fn new(query: Box<dyn Query>, consistency: ConsistencyLevel) -> QueryExecutable {
        QueryExecutable { query, consistency}
    }
}

impl Executable for QueryExecutable {
    fn execute(&self, request: Frame) -> Result<Frame, Errors> {
        let _ = self.consistency;

        if needs_to_delegate() {
            dbg!("soy nodo 1");
            let delegator = QueryDelegator::new(1, request, ConsistencyLevel::One);
            delegator.send()
        } else {
            println!("soy nodo 2");
            let msg = self.query.run()?;
            FrameBuilder::build_response_frame(request, RESULT, msg.as_bytes().to_vec())
        }
    }
}

fn needs_to_delegate() -> bool {
    if get_ip() == "127.0.0.1" {
        return true;
    }
    false
}
fn get_ip() -> String {
    let filename = "src/node_info.json";
    let file = File::open(filename).expect("file not found");
    // Deserializar el contenido del archivo a Node
    let node: Node = serde_json::from_reader(file).expect("error while reading json");
    node.ip
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Node{
    pub ip: String,
    pub port: u16,
}

impl Node{
    pub fn new(ip: String, port: u16) -> Node{
        Node{ip, port}
    }

    pub fn get_full_ip(&self) -> String{
        format!("{}:{}", self.ip, self.port)
    }

    pub fn write_to_file(&self, filename: &str) {
        let file = File::create(filename).expect("Unable to create file");
        serde_json::to_writer(file, self).expect("Unable to write to file");
    }
}
