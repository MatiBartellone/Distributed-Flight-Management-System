use std::{collections::HashMap, io::{Cursor, Bytes}, usize};

use crate::{utils::{errors::Errors, bytes_cursor::BytesCursor}, data_access::row::Row, parsers::tokens::data_type::DataType};



pub struct ReadRepair {
    responses_bytes: HashMap<String, Vec<u8>>,
    meta_data_bytes: HashMap<String, Vec<u8>>,
    keyspace_table: String,
    rows: Vec<Row> 
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

    pub fn get_response(self) -> Result<Vec<u8>, Errors> {
        if self.repair_innecesary() {
            return self.get_first_response();
        }
        Ok(vec![])
    }

    pub fn read_protocol_response(&mut self, ip: String) -> Result<(), Errors> {
        let bytes = self.responses_bytes
            .get(&ip)
            .ok_or_else(|| Errors::TruncateError(format!("Key {} not found in responses_bytes", ip)))?;
        let mut cursor = BytesCursor::new(&bytes[8..]);
        let columns_count = cursor.read_int()?;
        let keyspace = cursor.read_string()?;
        let table = cursor.read_string()?;
        self.keyspace_table = format!("{}.{}", keyspace, table);
        let 
        Ok(())
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

fn byte_to_data_type(byte: i16) -> Result<DataType, Errors> {
    match byte {
        0x0004 => Ok(DataType::Boolean),
        0x000B => Ok(DataType::Date),
        0x0006 => Ok(DataType::Decimal),
        0x000F => Ok(DataType::Duration),
        0x0009 => Ok(DataType::Int),
        0x000A => Ok(DataType::Text),
        0x000C => Ok(DataType::Time),
        _ => Err(Errors::ProtocolError(format!("Unknown data type byte: {}", byte))),
    }
}