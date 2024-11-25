use crate::{
    data_access::row::Row, parsers::tokens::data_type::DataType,
    utils::types_to_bytes::TypesToBytes, meta_data::meta_data_handler::MetaDataHandler,
};

use super::{errors::Errors, constants::KEYSPACE_METADATA_PATH};
pub struct Response;

impl Response {
    pub fn void() -> Result<Vec<u8>, Errors> {
        let mut encoder = TypesToBytes::default();
        encoder
            .write_int(0x0001)
            .map_err(Errors::TruncateError)?;
        Ok(encoder.into_bytes())
    }

    pub fn set_keyspace(keyspace: &str) -> Result<Vec<u8>, Errors> {
        let mut encoder = TypesToBytes::default();
        encoder
            .write_int(0x0003)
            .map_err(Errors::TruncateError)?;
        encoder
            .write_string(keyspace)
            .map_err(Errors::TruncateError)?;
        Ok(encoder.into_bytes())
    }

    pub fn schema_change(
        change_type: &str,
        target: &str,
        options: &str,
    ) -> Result<Vec<u8>, Errors> {
        let mut encoder = TypesToBytes::default();
        encoder
            .write_int(0x0005)
            .map_err(Errors::TruncateError)?;
        encoder
            .write_string(change_type)
            .map_err(Errors::TruncateError)?;
        encoder
            .write_string(target)
            .map_err(Errors::TruncateError)?;
        encoder
            .write_string(options)
            .map_err(Errors::TruncateError)?;
        Ok(encoder.into_bytes())
    }

    pub fn rows(rows: Vec<Row>, keyspace: &str, table: &str) -> Result<Vec<u8>, Errors> {
        let mut encoder = TypesToBytes::default();
        Response::write_protocol_response(&rows, keyspace, table, &mut encoder)?;
        let division_offset = encoder.length();
        //Division
        Response::write_meta_data_response(&rows, keyspace, table, &mut encoder)?;
        encoder.write_int(division_offset as i32).map_err(Errors::TruncateError)?;
        Ok(encoder.into_bytes())
    }

    pub fn protocol_row(rows: Vec<Row>, keyspace: &str, table: &str) -> Result<Vec<u8>, Errors> {
        let mut encoder = TypesToBytes::default();
        Response::write_protocol_response(&rows, keyspace, table, &mut encoder)?;
        Ok(encoder.into_bytes())
    }

    pub fn write_protocol_response(rows: &Vec<Row>, keyspace: &str, table: &str, encoder: &mut TypesToBytes) -> Result<(), Errors>{
        encoder.write_int(0x0002).map_err(Errors::TruncateError)?;
        encoder.write_int(0x0001).map_err(Errors::TruncateError)?;
        if let Some(first_row) = rows.first() {
            encoder.write_int(first_row.columns.len() as i32).map_err(Errors::TruncateError)?;
        }
        encoder.write_string(keyspace).map_err(Errors::TruncateError)?;
        encoder.write_string(table).map_err(Errors::TruncateError)?;
        for column in &rows[0].columns {
            encoder.write_string(&column.column_name).map_err(Errors::TruncateError)?;
            let data_type_id = Response::data_type_to_byte(column.value.data_type.clone());
            encoder.write_i16(data_type_id).map_err(Errors::TruncateError)?;
        }
        encoder.write_int(rows.len() as i32).map_err(Errors::TruncateError)?;
        for row in rows {
            for column in &row.columns {
                encoder.write_string(&column.value.value).map_err(Errors::TruncateError)?;
            }
        }
        Ok(())
    }

    fn write_meta_data_response(rows: &Vec<Row>, keyspace: &str, table: &str,encoder: &mut TypesToBytes) -> Result<(), Errors> {
        for row in rows {
            for column in &row.columns {
                let time_stamp_bytes = column.time_stamp;
                encoder.write_u64(time_stamp_bytes).map_err(Errors::TruncateError)?;
            }
        }
        encoder.write_short(rows[0].primary_key.len() as u16).map_err(Errors::TruncateError)?;
        for row in rows {
            for pk in &row.primary_key{
                encoder.write_string(pk).map_err(Errors::TruncateError)?;
            }
        }
        let pks = get_pks(keyspace, table)?;
        encoder.write_short(pks.len() as u16).map_err(Errors::TruncateError)?;
        for pk in pks {
            encoder.write_string(&pk).map_err(Errors::TruncateError)?;
        }
        for row in rows {
            for pk_value in &row.primary_key {
                encoder.write_string(pk_value).map_err(Errors::TruncateError)?;
            }
        }
        Ok(())
    }

    fn data_type_to_byte(data: DataType) -> i16 {
        match data {
            DataType::Boolean => 0x0004,  // Código de tipo para `BOOLEAN`
            DataType::Date => 0x000B,     // Código de tipo para `DATE`
            DataType::Decimal => 0x0006,  // Código de tipo para `DECIMAL`
            DataType::Duration => 0x000F, // Código de tipo para `DURATION`
            DataType::Int => 0x0009,      // Código de tipo para `INT`
            DataType::Text => 0x000A,     // Código de tipo para `TEXT`
            DataType::Time => 0x000C,     // Código de tipo para `TIME`
        }
    }
}

fn get_pks(keyspace: &str, table: &str) -> Result<Vec<String>, Errors> {
    let mut stream = MetaDataHandler::establish_connection()?;
    let meta_data_handler = MetaDataHandler::get_instance(&mut stream)?;
    let keyspace_meta_data = meta_data_handler.get_keyspace_meta_data_access();
    let pks = keyspace_meta_data.get_primary_key(KEYSPACE_METADATA_PATH.to_owned(), keyspace, table)?;
    Ok(pks.get_full_primary_key())
}

#[cfg(test)]
mod tests {
    use crate::{utils::{response::Response, types_to_bytes::TypesToBytes}, parsers::tokens::{data_type::DataType, literal::Literal}, data_access::row::{Column, Row}};

    #[test]
    fn test_void_response() {
        let result = Response::void();
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), vec![0x00, 0x00, 0x00, 0x01]); 
    }

    #[test]
    fn test_set_keyspace() {
        let keyspace = "test_keyspace";
        let result = Response::set_keyspace(keyspace);
        assert!(result.is_ok());
        let bytes = result.unwrap();
        assert_eq!(bytes[..4], [0x00, 0x00, 0x00, 0x03]);
        assert!(String::from_utf8_lossy(&bytes[4..]).contains(keyspace));
    }

    #[test]
    fn test_schema_change() {
        let result = Response::schema_change("CREATE", "table", "options");
        assert!(result.is_ok());
        let bytes = result.unwrap();
        assert_eq!(bytes[..4], [0x00, 0x00, 0x00, 0x05]); 
        let body = String::from_utf8_lossy(&bytes[4..]);
        assert!(body.contains("CREATE"));
        assert!(body.contains("table"));
        assert!(body.contains("options"));
    }

    #[test]
    fn test_protocol_row() {
        let rows = mock_rows(); 
        let keyspace = "test_keyspace";
        let table = "test_table";
        let result = Response::protocol_row(rows, keyspace, table);
        assert!(result.is_ok());
    }

    #[test]
    fn test_data_type_to_byte() {
        assert_eq!(Response::data_type_to_byte(DataType::Boolean), 0x0004);
        assert_eq!(Response::data_type_to_byte(DataType::Int), 0x0009);
        assert_eq!(Response::data_type_to_byte(DataType::Text), 0x000A);
    }


    #[test]
    fn test_write_protocol_response() {
        let rows = mock_rows();
        let keyspace = "test_keyspace";
        let table = "test_table";
        let mut encoder = TypesToBytes::default();
        let result = Response::write_protocol_response(&rows, keyspace, table, &mut encoder);
        assert!(result.is_ok());
        assert!(!encoder.into_bytes().is_empty());
    }

    fn mock_columns() -> Vec<Column> {
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
        ]
    }

   fn mock_rows() -> Vec<Row> {
    vec![Row::new(
        mock_columns(),
        vec!["key1".to_string()], 
    )]
}
}
