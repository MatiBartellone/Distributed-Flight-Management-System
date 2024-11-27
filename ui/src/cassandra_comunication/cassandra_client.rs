use std::collections::HashMap;

use tokio::runtime::Runtime;

use crate::utils::{
        consistency_level::ConsistencyLevel, constants::{OP_AUTHENTICATE, OP_AUTH_CHALLENGE, OP_AUTH_RESPONSE, OP_AUTH_SUCCESS}, frame::Frame, system_functions::get_user_data, types_to_bytes::TypesToBytes
    };
use super::cassandra_connector::CassandraConnection;

pub const VERSION: u8 = 3;
pub const FLAGS: u8 = 0;
pub const STREAM: i16 = 10;
pub const OP_CODE_QUERY: u8 = 7;
pub const OP_CODE_START: u8 = 1;

pub struct CassandraClient {
    connection: CassandraConnection,
}

impl CassandraClient {
    /// Creates a new CassandraClient with the given node
    pub fn new(node: &str) -> Result<Self, String> {
        let connection = Runtime::new()
            .map_err(|e| format!("Failed to create runtime: {}", e))?
            .block_on(CassandraConnection::new(node))?;
        Ok(Self { connection })
    }
    
    /// Send a frame to the server and returns the response
    pub fn send_and_receive(&mut self, frame: &mut Frame) -> Result<Frame, String> {
        let result = Runtime::new()
            .map_err(|e| format!("Failed to create runtime: {}", e))?
            .block_on(self.connection.send_and_receive(frame));
        result
    }

    /// Get ready the client for use in keyspace airport
    pub fn inicializate(&mut self) -> Result<(), String> {
        self.start_up()
    }

    /// Send a startup
    fn start_up(&mut self) -> Result<(), String> {
        let body = self.get_start_up_body()?;
        let mut frame = Frame::new(
            VERSION,
            FLAGS,
            STREAM,
            OP_CODE_START,
            body.len() as u32,
            body,
        );
        let frame_response = self.send_and_receive(&mut frame)?;
        self.handle_frame_response(frame_response)
    }

    fn get_start_up_body(&self) -> Result<Vec<u8>, String> {
        let mut types_to_bytes = TypesToBytes::default();
        let mut options_map = HashMap::new();
        options_map.insert("CQL_VERSION".to_string(), "3.0.0".to_string());
        types_to_bytes.write_string_map(&options_map)?;
        Ok(types_to_bytes.into_bytes())
    }

    // Send the authentication until it success
    fn authenticate_response(&mut self) -> Result<(), String> {
        let body = self.get_authenticate_body()?;
        let mut frame = Frame::new(
            VERSION,
            FLAGS,
            STREAM,
            OP_AUTH_RESPONSE,
            body.len() as u32,
            body,
        );
        let frame_response = self.send_and_receive(&mut frame)?;
        self.handle_frame_response(frame_response)
    }

    fn get_authenticate_body(&self) -> Result<Vec<u8>, String> {
        let mut types_to_bytes = TypesToBytes::default();
        let credentials = get_user_data("Credentials with format (user:password)\n");
        types_to_bytes.write_long_string(&credentials)?;
        Ok(types_to_bytes.into_bytes())
    }

    fn get_body_query(
        &self,
        query: &str,
        consistency_level: ConsistencyLevel,
    ) -> Result<Vec<u8>, String> {
        let mut types_to_bytes = TypesToBytes::default();
        types_to_bytes.write_long_string(query)?;
        types_to_bytes.write_consistency(consistency_level)?;
        Ok(types_to_bytes.into_bytes())
    }

    /// Get the body for a query with strong consistency
    pub fn get_body_query_strong(&self, query: &str) -> Result<Vec<u8>, String> {
        self.get_body_query(query, ConsistencyLevel::Quorum)
    }

    /// Get the body for a query with weak consistency
    pub fn get_body_query_weak(&self, query: &str) -> Result<Vec<u8>, String> {
        self.get_body_query(query, ConsistencyLevel::One)
    }

    // Handles the read frame
    fn handle_frame_response(&mut self, frame: Frame) -> Result<(), String> {
        match frame.opcode {
            OP_AUTHENTICATE | OP_AUTH_CHALLENGE => self.authenticate_response(),
            OP_AUTH_SUCCESS => Ok(()),
            _ => Err("Invalid OP response".to_string()),
        }
    }
}