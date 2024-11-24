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


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_protocol_response() {
        let mut row_response = RowResponse::new();

        // Bytes simulando una respuesta válida de protocolo
        let protocol_bytes: Vec<u8> = vec![
            0, 0, 0, 2, // Número de columnas (2)
            0, 3, b'f', b'o', b'o', // Keyspace: "foo"
            0, 3, b'b', b'a', b'r', // Tabla: "bar"
            0, 4, b'n', b'a', b'm', b'e', // Columna 1: "name"
            0, 0x000A, // Tipo de dato: Text
            0, 2, b'i', b'd', // Columna 2: "id"
            0, 0x0009, // Tipo de dato: Int
            0, 0, 0, 2, // Número de filas (2)
            0, 5, b'A', b'l', b'i', b'c', b'e', // Valor fila 1, columna 1: "Alice"
            0, 1, b'1', // Valor fila 1, columna 2: "1"
            0, 3, b'B', b'o', b'b', // Valor fila 2, columna 1: "Bob"
            0, 1, b'2', // Valor fila 2, columna 2: "2"
        ];

        let result = row_response.read_protocol_response(protocol_bytes);
        assert!(result.is_ok());
        let (rows_count, columns_count) = result.unwrap();
        assert_eq!(rows_count, 2);
        assert_eq!(columns_count, 2);
        assert_eq!(row_response.keyspace, "foo");
        assert_eq!(row_response.table, "bar");
        assert!(row_response.column_protocol.contains_key("name"));
        assert!(row_response.column_protocol.contains_key("id"));
    }

    #[test]
    fn test_read_meta_data_response() {
        let mut row_response = RowResponse::new();

        // Bytes simulando una respuesta válida de meta-data
        let meta_data_bytes: Vec<u8> = vec![
            0, 0, 0, 0, 0, 0, 0, 1, // Timestamp fila 1, columna 1
            0, 0, 0, 0, 0, 0, 0, 2, // Timestamp fila 1, columna 2
            0, 1, // Número de claves primarias (1)
            0, 2, b'i', b'd', // Clave primaria: "id"
            0, 1, // Valor de la clave primaria fila 1: "1"
            0, 1, b'1',
        ];

        let result = row_response.read_meta_data_response(meta_data_bytes, 1, 2);
        assert!(result.is_ok());
        assert_eq!(row_response.pk_name, vec!["id"]);
        assert_eq!(row_response.time_stamps[0], vec![1, 2]);
        assert_eq!(row_response.pk_values[0], vec!["1"]);
    }

    #[test]
    fn test_create_rows() {
        let mut row_response = RowResponse::new();

        // Configurar manualmente valores para simular datos previos
        row_response.column_protocol.insert("name".to_string(), DataType::Text);
        row_response.column_protocol.insert("id".to_string(), DataType::Int);
        row_response.values_protocol = vec![
            vec!["Alice".to_string(), "1".to_string()],
            vec!["Bob".to_string(), "2".to_string()],
        ];
        row_response.time_stamps = vec![
            vec![1234567890, 1234567891],
            vec![1234567892, 1234567893],
        ];
        row_response.pk_values = vec![
            vec!["1".to_string()],
            vec!["2".to_string()],
        ];

        let result = row_response.create_rows();
        assert!(result.is_ok());
        let rows = result.unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].primary_key, vec!["1".to_string()]);
        assert_eq!(rows[1].primary_key, vec!["2".to_string()]);
    }

    #[test]
    fn test_byte_to_data_type() {
        assert_eq!(byte_to_data_type(0x000A), Ok(DataType::Text));
        assert_eq!(byte_to_data_type(0x0009), Ok(DataType::Int));
        assert!(byte_to_data_type(0xFFFF).is_err());
    }
}