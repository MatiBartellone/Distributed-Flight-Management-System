use std::collections::HashMap;

use crate::{utils::{errors::Errors, bytes_cursor::BytesCursor}, parsers::tokens::{data_type::DataType, literal::create_literal}, data_access::row::{Row, Column}};

use super::data_response::DataResponse;



pub struct RowResponse;

impl RowResponse {

    pub fn read_rows(bytes: Vec<u8>) -> Result<Vec<Row>, Errors> {
        let mut cursor = BytesCursor::new(&bytes);
        let _ = cursor.read_int();
        let count_rows = cursor.read_short()?;
        let mut res: Vec<Row> = Vec::new();
        for _ in 0..count_rows {
            let mut columns: Vec<Column> = Vec::new();
            let count_columns = cursor.read_short()?;
            for _ in 0..count_columns {
                let column = Self::read_column(&mut cursor)?;
                columns.push(column)
            }

            let mut primary_keys: Vec<String> = Vec::new(); 
            let count_pks = cursor.read_short()?;
            for _ in 0..count_pks {
                let pk = cursor.read_string()?; 
                primary_keys.push(pk)
            }
            let row = Row::new(columns, primary_keys);
            res.push(row);
        }
        Ok(res)
    }

    fn read_column(cursor: &mut BytesCursor) -> Result<Column, Errors> {
        let name = cursor.read_string()?;
        let value = cursor.read_string()?;
        let data_type = byte_to_data_type(cursor.read_i16()?)?;
        let time_stamp = cursor.read_u64()?;
        let literal = create_literal(&value, data_type);
        Ok(Column::new(&name, &literal, time_stamp))
    }

    pub fn read_meta_data_response(bytes: Vec<u8>) -> Result<DataResponse, Errors> {
        let mut cursor = BytesCursor::new(&bytes);
        let mut headers_pks: HashMap<String, DataType> = HashMap::new();
        let keyspace = cursor.read_string()?;
        let table = cursor.read_string()?;
        let count_primary_keys = cursor.read_short()?;
        for _ in 0..count_primary_keys {
            let title = cursor.read_string()?;
            let data_type_bytes = cursor.read_i16()?;
            let data_type = byte_to_data_type(data_type_bytes)?;
            headers_pks.insert(title, data_type);
        }
        Ok(DataResponse::new(headers_pks, table, keyspace))
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



#[cfg(test)]
mod tests {
    use crate::{utils::{types_to_bytes::TypesToBytes, response::Response}, parsers::tokens::literal::Literal};

    use super::*;
    


    
    fn mock_rows() -> Vec<Row> {
        vec![
            Row::new(
                vec![
                    Column {
                        column_name: "col1".to_string(),
                        value: Literal {
                            data_type: DataType::Int,
                            value: "42".to_string(),
                        },
                        time_stamp: 123456789,
                    },
                    Column {
                        column_name: "col2".to_string(),
                        value: Literal {
                            data_type: DataType::Text,
                            value: "hello".to_string(),
                        },
                        time_stamp: 123456789,
                    },
                ],
                vec!["key1".to_string()],
            ),
            Row::new(
                vec![
                    Column {
                        column_name: "col1".to_string(),
                        value: Literal {
                            data_type: DataType::Int,
                            value: "84".to_string(),
                        },
                        time_stamp: 987654321,
                    },
                    Column {
                        column_name: "col2".to_string(),
                        value: Literal {
                            data_type: DataType::Text,
                            value: "world".to_string(),
                        },
                        time_stamp: 987654321,
                    },
                ],
                vec!["key2".to_string()],
            ),
        ]
    }
}