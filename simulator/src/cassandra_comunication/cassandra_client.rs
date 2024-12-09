use std::collections::HashMap;

use crate::utils::{
        bytes_cursor::BytesCursor, consistency_level::ConsistencyLevel, constants::{OP_AUTH_RESPONSE, OP_ERROR, OP_RESULT, ROW_RESPONSE}, frame::Frame, types_to_bytes::TypesToBytes
    };
use super::cassandra_connector::CassandraConnection;

pub const VERSION: u8 = 3;
pub const FLAGS: u8 = 0;
pub const STREAM: i16 = 10;
pub const OP_CODE_QUERY: u8 = 7;
pub const OP_CODE_START: u8 = 1;

pub struct CassandraClient {
    connection: CassandraConnection
}

impl CassandraClient {
    pub fn new(node: &str) -> Result<Self, String> {
        let connection = CassandraConnection::new(node)?;
        Ok(Self { connection })
    }

    pub fn send_and_receive(&mut self, frame: &mut Frame) -> Result<Frame, String> {
        self.connection.send_and_receive(frame)
    }

    /// Send a startup message to the server to start the connection
    pub fn start_up(&mut self) -> Result<(), String> {
        let body = self.get_start_up_body()?;
        let mut frame = Frame::new(
            VERSION,
            FLAGS,
            STREAM,
            OP_CODE_START,
            body.len() as u32,
            body,
        );
        let _ = self.send_and_receive(&mut frame)?;
        Ok(())
    }

    fn get_start_up_body(&self) -> Result<Vec<u8>, String> {
        let mut types_to_bytes = TypesToBytes::default();
        let mut options_map = HashMap::new();
        options_map.insert("CQL_VERSION".to_string(), "3.0.0".to_string());
        types_to_bytes.write_string_map(&options_map)?;
        Ok(types_to_bytes.into_bytes())
    }

    // Send the authentication until it success
    pub fn authenticate(&mut self, user: &str, password: &str) -> Result<(), String> {
        let body = self.get_authenticate_body(user, password)?;
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

    fn get_authenticate_body(&self, user: &str, password: &str) -> Result<Vec<u8>, String> {
        let credentials = format!("{}:{}", user, password);
        let mut types_to_bytes = TypesToBytes::default();
        types_to_bytes.write_long_string(&credentials)?;
        Ok(types_to_bytes.into_bytes())
    }

    // Handles the read frame
    fn handle_frame_response(&mut self, frame: Frame) -> Result<(), String> {
        match frame.opcode {
            OP_ERROR => {
                let mut cursor = BytesCursor::new(frame.body.as_slice());
                let _ = vec![cursor.read_u8()?, cursor.read_u8()?];
                let msg = cursor.read_string()?;
                Err(format!("Error: {}",  msg))
            }
            _ => Ok(()),
        }
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

    fn get_body_query_strong(&self, query: &str) -> Result<Vec<u8>, String> {
        self.get_body_query(query, ConsistencyLevel::Quorum)
    }

    fn get_body_query_weak(&self, query: &str) -> Result<Vec<u8>, String> {
        self.get_body_query(query, ConsistencyLevel::One)
    }

    fn get_body_result(&self, frame: Frame) -> Result<Vec<u8>, String> {
        if frame.opcode != OP_RESULT {
            return Err("Error reading the frame".to_string());
        }
        Ok(frame.body)
    }

    fn get_strong_query_frame(&mut self, query: &str, frame_id: &usize) -> Result<Frame, String> {
        let body = self.get_body_query_strong(query)?;
        self.get_query_frame(&body, frame_id)
    }

    fn get_weak_query_frame(&mut self, query: &str, frame_id: &usize) -> Result<Frame, String> {
        let body = self.get_body_query_weak(query)?;
        self.get_query_frame(&body, frame_id)
    }

    fn get_query_frame(&self, body: &[u8], frame_id: &usize) -> Result<Frame, String> {
        Ok(Frame::new(
            VERSION,
            FLAGS,
            *frame_id as i16,
            OP_CODE_QUERY,
            body.len() as u32,
            body.to_vec(),
        ))
    }

    fn get_body_frame_response(&mut self, frame: &mut Frame) -> Result<Vec<u8>, String> {
        let frame_response = self.send_and_receive(frame)?;
        let body_response = self.get_body_result(frame_response)?;
        Ok(body_response)
    }

    fn get_response_row(&self, body: &[u8]) -> Result<Vec<HashMap<String, String>>, String> {
        self.get_rows(body)
    }

    fn get_rows(&self, body: &[u8]) -> Result<Vec<HashMap<String, String>>, String> {
        let mut cursor = BytesCursor::new(body);
        let type_response = cursor.read_int()?;
        if type_response != ROW_RESPONSE {
            return Err("Invalid type response".to_string());
        }
        let _ = cursor.read_int()?;
        let columns_count = cursor.read_int()?;
        let _keyspace = cursor.read_string()?;
        let _table = cursor.read_string()?;
        
        let mut column_names = Vec::new();
        for _ in 0..columns_count {
            let column_name = cursor.read_string()?;
            let _ = cursor.read_i16()?;
            column_names.push(column_name);
        }

        let row_count = cursor.read_int()?;
        let mut rows = Vec::new();
        for _ in 0..row_count {
            let mut row = HashMap::new();
            for column_name in &column_names {
                let value = cursor.read_string()?;
                row.insert(column_name.to_string(), value);
            }
            rows.push(row);
        }
        Ok(rows)
    }

    pub fn execute_strong_select_query(&mut self, query: &str, frame_id: &usize) -> Result<Vec<HashMap<String, String>>, String> {
        let mut frame = self.get_strong_query_frame(query, frame_id)?;
        let rows = self.get_body_frame_response(&mut frame)?;
        self.get_response_row(&rows)
    }

    pub fn execute_weak_select_query(&mut self, query: &str, frame_id: &usize) -> Result<Vec<HashMap<String, String>>, String> {
        let mut frame = self.get_weak_query_frame(query, frame_id)?;
        let rows = self.get_body_frame_response(&mut frame)?;
        self.get_response_row(&rows)
    }

    fn execute_query_without_response(&mut self, query: &str, consistency_level: ConsistencyLevel, frame_id: &usize) -> Result<(), String> {
        let body = self.get_body_query(query, consistency_level)?;
        let mut frame = self.get_query_frame(&body, frame_id)?;
        let _ = self.send_and_receive(&mut frame)?;
        Ok(())
    }

    pub fn execute_strong_query_without_response(&mut self, query: &str, frame_id: &usize) -> Result<(), String> {
        self.execute_query_without_response(query, ConsistencyLevel::Quorum, frame_id)
    }
    
    pub fn execute_weak_query_without_response(&mut self, query: &str, frame_id: &usize) -> Result<(), String> {
        self.execute_query_without_response(query, ConsistencyLevel::One, frame_id)
    }
}