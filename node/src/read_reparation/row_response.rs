use std::collections::HashMap;

use crate::{utils::{errors::Errors, functions::deserialize_from_str}, parsers::tokens::{data_type::DataType, literal::create_literal}, data_access::row::{Row, Column}};
use crate::utils::types::bytes_cursor::BytesCursor;
use super::data_response::DataResponse;



pub struct RowResponse;

impl RowResponse {

    pub fn read_rows(bytes: Vec<u8>) -> Result<Vec<Row>, Errors> {
        let mut cursor = BytesCursor::new(&bytes);
        let _ = cursor.read_int();
        let count_rows = cursor.read_short()?;
        let mut res: Vec<Row> = Vec::new();
        for _ in 0..count_rows {
            let row : Row = deserialize_from_str(&cursor.read_string()?)?;
            res.push(row);
        }
        dbg!(&res);
        Ok(res)
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
    use crate::utils::{response::Response, types_to_bytes::TypesToBytes};

    use super::*;

    #[test]
    fn test_read_rows() {
        // Crear datos para las filas
        let column1 = Column::new(&"col1".to_string(), &create_literal("42", DataType::Int));
        let column2 = Column::new(&"col2".to_string(), &create_literal("test", DataType::Text));
        let row = Row::new(vec![column1, column2], vec!["pk_value".to_string()]);
        let rows = vec![row];
        let mut encoder = TypesToBytes::default();
        // Codificar los datos en bytes
        Response::write_rows(&rows, &mut encoder).unwrap();

        // Leer los datos usando `read_rows`
        let result = RowResponse::read_rows(encoder.into_bytes()).unwrap();

        // Verificar el resultado
        assert_eq!(result.len(), 1);
        let first_row = &result[0];
        assert_eq!(first_row.columns.len(), 2);
        assert_eq!(first_row.columns[0].column_name, "col1");
        assert_eq!(first_row.columns[1].column_name, "col2");
        assert_eq!(first_row.primary_key, vec!["pk_value"]);
    }
}
