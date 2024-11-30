use super::{constants::KEYSPACE_METADATA_PATH, errors::Errors};
use crate::meta_data::meta_data_handler::use_keyspace_meta_data;
use crate::utils::functions::serialize_to_string;
use crate::{
    data_access::row::Row, parsers::tokens::data_type::DataType,
    utils::types_to_bytes::TypesToBytes,
};
use std::collections::HashMap;

pub struct Response;

impl Response {
    pub fn void() -> Result<Vec<u8>, Errors> {
        let mut encoder = TypesToBytes::default();
        encoder.write_int(0x0001)?;
        Ok(encoder.into_bytes())
    }

    pub fn set_keyspace(keyspace: &str) -> Result<Vec<u8>, Errors> {
        let mut encoder = TypesToBytes::default();
        encoder.write_int(0x0003)?;
        encoder.write_string(keyspace)?;
        Ok(encoder.into_bytes())
    }

    pub fn schema_change(
        change_type: &str,
        target: &str,
        options: &str,
    ) -> Result<Vec<u8>, Errors> {
        let mut encoder = TypesToBytes::default();
        encoder.write_int(0x0005)?;
        encoder.write_string(change_type)?;
        encoder.write_string(target)?;
        encoder.write_string(options)?;
        Ok(encoder.into_bytes())
    }

    pub fn protocol_row(
        rows: Vec<Row>,
        keyspace: &str,
        table: &str,
        headers: Vec<String>,
    ) -> Result<Vec<u8>, Errors> {
        let mut encoder = TypesToBytes::default();
        Response::write_protocol_response(&rows, keyspace, table, headers, &mut encoder)?;
        Ok(encoder.into_bytes())
    }

    fn write_protocol_response(
        rows: &Vec<Row>,
        keyspace: &str,
        table: &str,
        headers: Vec<String>,
        encoder: &mut TypesToBytes,
    ) -> Result<(), Errors> {
        encoder.write_int(0x0002)?;
        encoder.write_int(0x0001)?;
        encoder.write_int(headers.len() as i32)?;

        encoder.write_string(keyspace)?;
        encoder.write_string(table)?;
        let map_type = get_column(keyspace, table)?;
        for header in &headers {
            encoder.write_string(header)?;
            if let Some(data_type) = map_type.get(header) {
                let data_type_id = Response::data_type_to_byte(data_type.clone());
                encoder.write_i16(data_type_id)?;
            }
        }
        encoder.write_int(rows.len() as i32)?;
        for row in rows {
            if row.deleted {
                continue;
            }
            for header in &headers {
                match row.get_some_column(header) {
                    Ok(column) => encoder.write_string(&column.value.value)?,
                    _ => encoder.write_string("None")?,
                }
            }
        }
        Ok(())
    }

    pub fn rows(
        rows: Vec<Row>,
        keyspace: &str,
        table: &str,
        columns: &Vec<String>,
    ) -> Result<Vec<u8>, Errors> {
        let mut encoder = TypesToBytes::default();
        Response::write_rows(&rows, &mut encoder)?;
        let division_offset = encoder.length();
        //Division
        Response::write_meta_data_response(&mut encoder, keyspace, table, columns)?;
        encoder.write_int(division_offset as i32)?;
        Ok(encoder.into_bytes())
    }

    pub fn write_rows(rows: &Vec<Row>, encoder: &mut TypesToBytes) -> Result<(), Errors> {
        encoder.write_int(0x0002)?;
        encoder.write_short(rows.len() as u16)?;
        for row in rows {
            encoder.write_string(serialize_to_string(row)?.as_str())?;
        }
        Ok(())
    }

    pub fn write_meta_data_response(
        encoder: &mut TypesToBytes,
        keyspace: &str,
        table: &str,
        columns: &Vec<String>,
    ) -> Result<(), Errors> {
        encoder.write_string(keyspace)?;
        encoder.write_string(table)?;
        let pks = get_pks(keyspace, table)?;
        encoder.write_short(pks.len() as u16)?;
        for (pk, type_) in pks {
            encoder.write_string(&pk)?;
            let data_type_id = Response::data_type_to_byte(type_);
            encoder.write_i16(data_type_id)?;
        }
        encoder.write_short(columns.len() as u16)?;
        for name in columns {
            encoder.write_string(name)?;
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

fn get_pks(keyspace: &str, table: &str) -> Result<HashMap<String, DataType>, Errors> {
    use_keyspace_meta_data(|handler| {
        let pks = handler.get_primary_key(KEYSPACE_METADATA_PATH.to_owned(), keyspace, table)?;
        let types =
            handler.get_columns_type(KEYSPACE_METADATA_PATH.to_string(), keyspace, table)?;
        Ok(filter_keys(pks.get_full_primary_key(), types))
    })
}

fn get_column(keyspace: &str, table: &str) -> Result<HashMap<String, DataType>, Errors> {
    use_keyspace_meta_data(|handler| {
        handler.get_columns_type(KEYSPACE_METADATA_PATH.to_string(), keyspace, table)
    })
}

fn filter_keys(vec: Vec<String>, map: HashMap<String, DataType>) -> HashMap<String, DataType> {
    let mut result: HashMap<String, DataType> = HashMap::new();
    for elem in vec {
        if let Some(value) = map.get(&elem) {
            result.insert(elem, value.clone());
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use crate::{parsers::tokens::data_type::DataType, utils::response::Response};

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
    fn test_data_type_to_byte() {
        assert_eq!(Response::data_type_to_byte(DataType::Boolean), 0x0004);
        assert_eq!(Response::data_type_to_byte(DataType::Int), 0x0009);
        assert_eq!(Response::data_type_to_byte(DataType::Text), 0x000A);
    }
}
