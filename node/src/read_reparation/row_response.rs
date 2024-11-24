use std::collections::HashMap;

use crate::{utils::{errors::Errors, bytes_cursor::BytesCursor}, parsers::tokens::{data_type::DataType, literal::Literal}, data_access::row::{Row, Column}};



pub struct RowResponse {
    column_protocol: HashMap<String, DataType>,
    values_protocol: Vec<Vec<String>>,
    keyspace: String,
    table: String,
    time_stamps: Vec<Vec<u64>>,
    pk_values: Vec<Vec<String>>,
    pk_name: Vec<String>,
}

impl RowResponse {

    pub fn new() -> Self {
        Self {
            column_protocol: HashMap::new(),     
            values_protocol: Vec::new(),     
            keyspace: String::new(),
            table: String::new(),      
            time_stamps: Vec::new(),           
            pk_values: Vec::new(),            
            pk_name: Vec::new(),               
        }
    }

    pub fn read_row_response(&mut self, protocol: Vec<u8>, meta_data: Vec<u8>) -> Result<Vec<Row>, Errors> {
        let (count_rows, columns_count) = self.read_protocol_response(protocol)?;
        self.read_meta_data_response(meta_data, count_rows, columns_count)?;
        self.create_rows()
    }

    pub fn read_keyspace_table(&mut self, protocol: Vec<u8>, meta_data: Vec<u8>) -> Result<(String, String), Errors> {
        let (count_rows, columns_count) = self.read_protocol_response(protocol)?;
        self.read_meta_data_response(meta_data, count_rows, columns_count)?;
        Ok((self.keyspace.to_owned(), self.table.to_owned()))
    }

    pub fn read_pk_headers(&mut self, protocol: Vec<u8>, meta_data: Vec<u8>) -> Result<Vec<String>, Errors> {
        let (count_rows, columns_count) = self.read_protocol_response(protocol)?;
        self.read_meta_data_response(meta_data, count_rows, columns_count)?;
        Ok(self.pk_name.clone())
    }

    fn read_protocol_response(&mut self, bytes: Vec<u8>) -> Result<(usize, usize), Errors> {
        let mut cursor = BytesCursor::new(&bytes[8..]);
        let columns_count = cursor.read_int()? as usize;
        self.keyspace = cursor.read_string()?;
        self.table = cursor.read_string()?;
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
        Ok((count_rows, columns_count))
    }

    fn read_meta_data_response(&mut self, bytes: Vec<u8>, rows_count: usize, column_count: usize) -> Result<(), Errors> {
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
        for _ in 0..rows_count {
            let mut pk_values_row: Vec<String> = Vec::new();
            for _ in 0..count_pks {
                pk_values_row.push(cursor.read_string()?)
            }
            self.pk_values.push(pk_values_row);
        }
        Ok(())
    }

    fn create_rows(&self) -> Result<Vec<Row>, Errors> {
        let mut rows: Vec<Row> = Vec::new();
        for ((values_row, timestamps_row), pk_values) in self.values_protocol.iter().zip(&self.time_stamps).zip(&self.pk_values) {
            let mut columns: Vec<Column> = Vec::new();
            for ((column, _data_type), (value, timestamp)) in self
                .column_protocol
                .iter()
                .zip(values_row.iter().zip(timestamps_row)) {
                    let literal = Literal::new(value.to_string(), _data_type.clone());
                    let column_struct = Column::new(column, &literal, *timestamp);
                    columns.push(column_struct);
            }
            let row = Row::new(columns, pk_values.to_vec());
            rows.push(row);
        }
        Ok(rows)
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

impl Default for RowResponse {
    fn default() -> Self {
         Self::new()
    }
}