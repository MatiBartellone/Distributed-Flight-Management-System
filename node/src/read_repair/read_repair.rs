use std::{collections::HashMap, usize};

use crate::{utils::{errors::Errors, bytes_cursor::BytesCursor, response::Response, constants::BEST}, data_access::row::Row};

use super::row_response::RowResponse;



pub struct ReadRepair {
    responses_bytes: HashMap<String, Vec<u8>>,
    meta_data_bytes: HashMap<String, Vec<u8>>,
}

impl ReadRepair {

    pub fn set_responses(&mut self, responses: &HashMap<String, Vec<u8>>) -> Result<(), Errors>{
        for (ip, response) in responses {
            let (response_node, meta_data_response) = ReadRepair::split_bytes(response)?;
            self.responses_bytes.insert(ip.to_string(), response_node);
            self.responses_bytes.insert(ip.to_string(), meta_data_response);
        }
        Ok(())
    }   

    pub fn get_response(&mut self) -> Result<Vec<u8>, Errors> {
        if self.repair_innecesary() {
            return self.get_first_response();
        }
        let better_response = self.get_better_response()?;
        self.responses_bytes.insert(BEST.to_string(), better_response);
        self.repair()?;
        self.responses_bytes
        .get(BEST)
        .cloned() 
        .ok_or_else(|| Errors::TruncateError("No keys found".to_string())) 
    }

    fn repair(&self) -> Result<(), Errors> {
        let better_response = self.responses_bytes.get(BEST).ok_or_else(|| Errors::TruncateError("No keys found".to_string()))?;
        for (ip, response) in &self.responses_bytes {
            if response != better_response {
                self.repair_node(ip)?;
            }
        }
        Ok(())
    }

    fn repair_node(&self, ip: &str) -> Result<(), Errors> {
        let node_rows = self.read_response(ip.to_string())?;
        let best = self.read_response(BEST.to_string())?;
        
        Ok(())
    }

    

    fn get_better_response(&mut self) -> Result<Vec<u8>, Errors> {
        let mut ips = self.responses_bytes.keys();
        let first_ip = ips.next().ok_or_else(|| Errors::TruncateError("No keys found".to_string()))?;
        let mut rows = self.read_response(first_ip.to_string())?;
        for ip in ips {
            let next_response = self.read_response(ip.to_string())?;
            compare_response(&mut rows, next_response);
        }
        let (keyspace, table) = self.get_keyspace_table(first_ip.to_string())?;
        Response::protocol_row(rows, &keyspace, &table)
    }    

    fn read_response(&self, ip: String) -> Result<Vec<Row>, Errors> {
        let mut translate = RowResponse::new();
        let protocol = self.responses_bytes.get(&ip).ok_or_else(|| Errors::TruncateError(format!("Key {} not found in responses_bytes", ip)))?;
        let meta_data = self.meta_data_bytes.get(&ip).ok_or_else(|| Errors::TruncateError(format!("Key {} not found in responses_bytes", ip)))?;
        translate.read_row_response(protocol.to_vec(), meta_data.to_vec())
    }

    fn get_keyspace_table(&self, ip: String) -> Result<(String, String), Errors> {
        let mut translate = RowResponse::new();
        let protocol = self.responses_bytes.get(&ip).ok_or_else(|| Errors::TruncateError(format!("Key {} not found in responses_bytes", ip)))?;
        let meta_data = self.meta_data_bytes.get(&ip).ok_or_else(|| Errors::TruncateError(format!("Key {} not found in responses_bytes", ip)))?;
        translate.read_keyspace_table(protocol.to_vec(), meta_data.to_vec())
    }

    fn get_first_response(&self) -> Result<Vec<u8>, Errors> {
        self.responses_bytes
            .values()
            .next()
            .cloned()
            .ok_or_else(|| Errors::ServerError(String::from("No response found")))
    }

    fn repair_innecesary(&self) -> bool {
        if self.responses_bytes.is_empty() {
            return true;
        }
        let mut iterator = self.responses_bytes.values();
        let first_response = match iterator.next() {
            Some(response) => response,
            None => return true, 
        };
    
        let all_equal = self.responses_bytes.values().all(|response| response == first_response);
        if all_equal {
            return true;
        }
        let all_rows = self.responses_bytes.values().all(|response| response.starts_with(&[0x00, 0x02]));
        if !all_rows {
            return true;
        }
        false
    }

    fn split_bytes(data: &[u8]) -> Result<(Vec<u8>, Vec<u8>), Errors> {
        let division_offset = &data[data.len() - 4..];
        let mut cursor = BytesCursor::new(division_offset);
        let division = cursor.read_int()? as usize;
        let data_section = data[..division].to_vec();
        let timestamps_section = data[division..data.len() - 4].to_vec();
        Ok((data_section, timestamps_section))
    }
}

fn compare_response(original: &mut Vec<Row>, new: Vec<Row>) {
    for (ori_row, new_row) in original.iter_mut().zip(new) {
        compare_row(ori_row, new_row);
    }
}

fn compare_row(original: &mut Row, new: Row) {
    for (col_ori, col_new) in original.columns.iter_mut().zip(new.columns) {
        if col_ori.time_stamp < col_new.time_stamp {
            *col_ori = col_new;
        }
    }
}

