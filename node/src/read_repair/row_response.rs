use std::collections::HashMap;

use crate::{utils::{errors::Errors, bytes_cursor::BytesCursor}, parsers::tokens::data_type::DataType};



pub struct RowResponse {
    column_protocol: HashMap<String, DataType>,
    values_protocol: Vec<Vec<String>>,
    keyspace_table: String,
    time_stamps: Vec<Vec<u64>>,
    pk_name: Vec<String>,
}

impl RowResponse {
    pub fn read_protocol_response(&mut self,bytes: Vec<u8>) -> Result<(), Errors> {
        let mut cursor = BytesCursor::new(&bytes[8..]);
        let columns_count = cursor.read_int()? as usize;
        let keyspace = cursor.read_string()?;
        let table = cursor.read_string()?;
        self.keyspace_table = format!("{}.{}", keyspace, table);
        for _ in 0..columns_count {
            let column = cursor.read_string()?;
            let data_type_bytes = cursor.read_i16()?;
            let data_type = byte_to_data_type(data_type_bytes)?;
            self.column_protocol.insert(column, data_type);
        }
        let count_rows = cursor.read_int()? as usize;
        for _ in 0..count_rows {
            let mut row: Vec<String> = Vec::new();
            for _ in 0..columns_count {
                let value = cursor.read_string()?;
                row.push(value);
            }
            self.values_protocol.push(row)
        }
        Ok(())
    }

    pub fn read_meta_data_response(&mut self, bytes: Vec<u8>, rows_count: usize, column_count: usize) -> Result<(), Errors> {
        let mut cursor = BytesCursor::new(&bytes);
        for _ in 0..rows_count {
            let mut row: Vec<u64> = Vec::new();
            for _ in 0..column_count{
                let time_stump = cursor.read_u64()?;
                row.push(time_stump);
            }
            self.time_stamps.push(row)
        }
        let count_pks = cursor.read_short()?;
        for _ in 0..count_pks{
            let pk = cursor.read_string()?;
            self.pk_name.push(pk);

        }
        Ok(())
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