use crate::{
    data_access::row::Row, parsers::tokens::data_type::DataType,
    utils::types_to_bytes::TypesToBytes,
};

use super::errors::Errors;
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
        encoder.write_int(0x0002).map_err(Errors::TruncateError)?;
        encoder.write_int(0x0001).map_err(Errors::TruncateError)?;
        if let Some(first_row) = rows.first() {
            encoder.write_int(first_row.columns.len() as i32).map_err(Errors::TruncateError)?;
        }
        let keyspace_bytes = keyspace.as_bytes();
        encoder.write_int(keyspace_bytes.len() as i32).map_err(Errors::TruncateError)?;
        encoder.write_bytes(keyspace_bytes);
        let table_bytes = table.as_bytes();
        encoder.write_int(table_bytes.len() as i32).map_err(Errors::TruncateError)?;
        encoder.write_bytes(table_bytes);
        for column in &rows[0].columns {
            let column_name_bytes = column.column_name.as_bytes();
            encoder.write_int(column_name_bytes.len() as i32).map_err(Errors::TruncateError)?;
            encoder.write_bytes(column_name_bytes);
            let data_type_id = Response::data_type_to_byte(column.value.data_type.clone());
            encoder.write_i16(data_type_id).map_err(Errors::TruncateError)?;
        }
        encoder.write_int(rows.len() as i32).map_err(Errors::TruncateError)?;
        for row in rows {
            for column in row.columns {
                let value_bytes = column.value.value.as_bytes();
                encoder.write_int(value_bytes.len() as i32).map_err(Errors::TruncateError)?;
                encoder.write_bytes(value_bytes);
            }
        }
        Ok(encoder.into_bytes())
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
