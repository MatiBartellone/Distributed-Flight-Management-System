use std::{collections::HashMap, io::{self, Write}, sync::mpsc::Receiver};

use crate::utils::{
        consistency_level::ConsistencyLevel,
        constants::{OP_AUTHENTICATE, OP_AUTH_CHALLENGE, OP_AUTH_RESPONSE, OP_AUTH_SUCCESS},
        frame::Frame,
        types_to_bytes::TypesToBytes,
    };
use super::cassandra_connector::CassandraConnection;

pub const VERSION: u8 = 3;
pub const FLAGS: u8 = 0;
pub const STREAM: i16 = 4;
pub const OP_CODE_QUERY: u8 = 7;
pub const OP_CODE_START: u8 = 1;

#[derive(Clone)]
pub struct CassandraClient {
    connection: CassandraConnection,
}

impl CassandraClient {
    pub fn new(node: &str) -> Result<Self, String> {
        let connection = CassandraConnection::new(node)?;
        Ok(Self { connection })
    }

    // Wraps functions of CassandraConnection
    pub fn send_frame(&self, frame: &mut Frame, frame_id: &usize) -> Result<Receiver<Frame>, String> {
        frame.stream = *frame_id as i16;
        self.connection.send_frame(frame)
    }

    pub fn read_frame_response(&self) -> Result<(), String> {
        self.connection.read_frame_response()
    }

    // Get ready the client for use in keyspace airport
    pub fn inicializate(&self, ) -> Result<(), String> {
        self.start_up()
    }

    // Send a startup
    fn start_up(&self) -> Result<(), String> {
        let body = self.get_start_up_body()?;
        let mut frame = Frame::new(
            VERSION,
            FLAGS,
            STREAM,
            OP_CODE_START,
            body.len() as u32,
            body,
        );
        let frame_id = STREAM as usize;
        let rx = self.send_frame(&mut frame, &frame_id)?;
        self.handle_frame_response(rx)
    }

    fn get_start_up_body(&self) -> Result<Vec<u8>, String> {
        let mut types_to_bytes = TypesToBytes::default();
        let mut options_map = HashMap::new();
        options_map.insert("CQL_VERSION".to_string(), "3.0.0".to_string());
        types_to_bytes.write_string_map(&options_map)?;
        Ok(types_to_bytes.into_bytes())
    }

    // Send the authentication until it success
    fn authenticate_response(&self) -> Result<(), String> {
        let body = self.get_authenticate_body()?;
        let mut frame = Frame::new(
            VERSION,
            FLAGS,
            STREAM,
            OP_AUTH_RESPONSE,
            body.len() as u32,
            body,
        );
        let frame_id = STREAM as usize;
        let rx = self.send_frame(&mut frame, &frame_id)?;
        self.handle_frame_response(rx)
    }

    fn get_authenticate_body(&self) -> Result<Vec<u8>, String> {
        let mut types_to_bytes = TypesToBytes::default();
        let credentials = get_user_data("Credentials with format (user:password)\n");
        types_to_bytes.write_long_string(&credentials)?;
        Ok(types_to_bytes.into_bytes())
    }

    // Get a query body with consistency
    pub fn get_body_query(
        &self,
        query: &str,
        consistency_level: ConsistencyLevel,
    ) -> Result<Vec<u8>, String> {
        let mut types_to_bytes = TypesToBytes::default();
        types_to_bytes.write_long_string(query)?;
        types_to_bytes.write_consistency(consistency_level)?;
        Ok(types_to_bytes.into_bytes())
    }

    pub fn get_body_query_strong(&self, query: &str) -> Result<Vec<u8>, String> {
        self.get_body_query(query, ConsistencyLevel::Quorum)
    }

    pub fn get_body_query_weak(&self, query: &str) -> Result<Vec<u8>, String> {
        self.get_body_query(query, ConsistencyLevel::One)
    }

    // Handles the read frame
    fn handle_frame_response(&self, rx: Receiver<Frame>) -> Result<(), String> {
        let _ = self.connection.read_frame_response()?;
        match rx.recv().unwrap().opcode {
            OP_AUTHENTICATE | OP_AUTH_CHALLENGE => self.authenticate_response(),
            OP_AUTH_SUCCESS => Ok(()),
            _ => Err("Invalid OP response".to_string()),
        }
    }
}

fn get_user_data(msg: &str) -> String {
    print!("{}", msg);
    io::stdout().flush().expect("Failed to flush stdout");
    let mut data = String::new();
    io::stdin()
        .read_line(&mut data)
        .expect("Error reading data");
    data.trim().to_string()
}