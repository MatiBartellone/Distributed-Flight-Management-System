use std::collections::HashMap;

use crate::{utils::{types::node_ip::NodeIp, errors::Errors, constants::BEST, response::Response}, parsers::tokens::data_type::DataType, data_access::row::Row};

use super::{utils::split_bytes, row_response::RowResponse, row_comparer::RowComparer, data_response::DataResponse};

pub struct ResponseManager {
    responses_bytes: HashMap<String, Vec<u8>>,
    meta_data_bytes: HashMap<String, Vec<u8>>,
}

impl ResponseManager {
    pub fn new(responses: &HashMap<NodeIp, Vec<u8>>) -> Result<Self, Errors> {
        let mut responses_bytes = HashMap::new();
        let mut meta_data_bytes = HashMap::new();

        for (ip, response) in responses {
            let (response_node, meta_data_response) = split_bytes(response)?;
            responses_bytes.insert(ip.get_string_ip(), response_node);
            meta_data_bytes.insert(ip.get_string_ip(), meta_data_response);
        }

        Ok(Self {
            responses_bytes,
            meta_data_bytes,
        })
    }

    pub fn get_ips(&self) -> Vec<String> {
        self.responses_bytes.keys().cloned().collect()
    }

    pub fn get_first_response(&self) -> Result<Vec<u8>, Errors> {
        if let Some((ip, _)) = self.responses_bytes.iter().next() {
            self.cast_to_protocol_row(ip)
        } else {
            Err(Errors::ServerError("No responses available".to_string()))
        }
    }

    pub fn repair_unnecessary(&self) -> Result<bool, Errors> {
        if !self.all_responses_equal()? {
            return Ok(false);
        }
        if self.some_row_is_deleted()? {
            return Ok(false);
        }
        Ok(true)
    }

    fn all_responses_equal(&self) -> Result<bool, Errors> {
        //Si alguna respuesta difiere de otra
        let mut responses: Vec<Vec<u8>> = Vec::new();
        for ip in self.responses_bytes.keys() {
            responses.push(self.cast_to_protocol_row(ip)?)
        }
        if responses.is_empty() {
            return Ok(true);
        }
        let first_response = &responses[0];
        let all_equal = responses.iter().all(|response| response == first_response);
        Ok(all_equal)
    }

    fn some_row_is_deleted(&self) -> Result<bool, Errors> {
        //Si alguna respuesta tiene un row que se debe borrar
        for ip in self.responses_bytes.keys() {
            let rows = self.read_rows(ip)?;
            for row in rows {
                if row.is_deleted() {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    pub fn get_better_response(&mut self) -> Result<(), Errors> {
        let first_ip = self.get_first_ip()?;
        let rows = self.aggregate_rows(first_ip)?;
        let (keyspace, table) = self.get_keyspace_table(first_ip)?;
        let column = self.get_columns(first_ip)?;
        self.store_better_response(rows, &keyspace, &table, &column)?;
        Ok(())
    }
    
    fn get_first_ip(&self) -> Result<&str, Errors> {
        self.responses_bytes
            .keys()
            .next()
            .map(|ip| ip.as_str())
            .ok_or_else(|| Errors::ServerError("No response found".to_string()))
    }
    
    fn aggregate_rows(&self, first_ip: &str) -> Result<Vec<Row>, Errors> {
        let mut rows = self.read_rows(first_ip)?;
        for ip in self.responses_bytes.keys() {
            let next_response = self.read_rows(ip)?;
            rows = RowComparer::compare_response(rows, next_response);
        }
        Ok(rows)
    }
    
    fn store_better_response(
        &mut self,
        rows: Vec<Row>,
        keyspace: &str,
        table: &str,
        column: &[String],
    ) -> Result<(), Errors> {
        let betters = Response::rows(rows, keyspace, table, &column.to_vec())?;
        let (best_rows, best_meta_data) = split_bytes(&betters)?;
        self.responses_bytes.insert(BEST.to_owned(), best_rows);
        self.meta_data_bytes.insert(BEST.to_owned(), best_meta_data);
        Ok(())
    }

    pub fn read_rows(&self, ip: &str) -> Result<Vec<Row>, Errors> {
        let protocol = self.responses_bytes.get(ip).ok_or_else(|| {
            Errors::ServerError(format!("Key {} not found in responses_bytes", ip))
        })?;
        RowResponse::read_rows(protocol.to_vec())
    }

    fn get_row_response(&self, ip: &str) -> Result<DataResponse, Errors> {
        let bytes = self.meta_data_bytes.get(ip).ok_or_else(|| {
            Errors::ServerError(format!("Key {} not found in meta_data_bytes", ip))
        })?;
        RowResponse::read_meta_data_response(bytes.to_vec())
    }

    pub fn get_keyspace_table(&self, ip: &str) -> Result<(String, String), Errors> {
        let data_response = self.get_row_response(ip)?;
        Ok((
            data_response.keyspace().to_string(),
            data_response.table().to_string(),
        ))
    }  

    pub fn get_pks_headers(&self, ip: &str) -> Result<HashMap<String, DataType>, Errors> {
        let data_response = self.get_row_response(ip)?;
        Ok(data_response.headers_pks().clone())
    }

    fn get_columns(&self, ip: &str) -> Result<Vec<String>, Errors> {
        let data_response = self.get_row_response(ip)?;
        Ok(data_response.colums())
    }

    pub fn cast_to_protocol_row(&self, ip: &str) -> Result<Vec<u8>, Errors> {
        let rows = self.read_rows(ip)?;
        let (keyspace, table) = self.get_keyspace_table(ip)?;
        let columns = self.get_columns(ip)?;
        Response::protocol_row(rows, &keyspace, &table, columns)
    }

    
}


#[cfg(test)]
mod tests {
    use crate::{data_access::column::Column, parsers::tokens::literal::Literal, utils::types_to_bytes::TypesToBytes};

    use super::*;
    use std::collections::HashMap;

    // Mocks necesarios
    fn mock_response_data() -> Vec<u8> {
        let rows = vec![
            Row::new(
                vec![
                    Column::new(&"column1".to_string(), &Literal::new("value1".to_string(), DataType::Text)),
                    Column::new(&"column2".to_string(), &Literal::new("value2".to_string(), DataType::Text)),
                ],
                vec!["pk1".to_string(), "pk2".to_string()],
            ),
            Row::new(
                vec![
                    Column::new(&"column3".to_string(), &Literal::new("value3".to_string(), DataType::Text)),
                    Column::new(&"column4".to_string(), &Literal::new("value4".to_string(), DataType::Text)),
                ],
                vec!["pk3".to_string(), "pk4".to_string()],
            ),
        ];
        let mut encoder = TypesToBytes::default(); 
        Response::write_rows(&rows, &mut encoder).expect("Failed to create mock response data");
        encoder.into_bytes()
    }

    fn mock_meta_data() -> Vec<u8> {
        let keyspace = "test_keyspace";
        let table = "test_table";
        let columns = vec!["column1".to_string(), "column2".to_string()];
        let pks = vec![
            ("pk1".to_string(), DataType::Text),
            ("pk2".to_string(), DataType::Int),
        ];
    
        let mut encoder = TypesToBytes::default();
    
        encoder.write_string(keyspace).unwrap();
        encoder.write_string(table).unwrap();
        encoder.write_short(pks.len() as u16).unwrap();
        for (pk, type_) in pks {
            encoder.write_string(&pk).unwrap();
            let data_type_id = data_type_to_byte(type_);
            encoder.write_i16(data_type_id).unwrap();
        }
        encoder.write_short(columns.len() as u16).unwrap();
        for name in columns {
            encoder.write_string(&name).unwrap();
        }
    
        encoder.into_bytes()
    }

    fn rows() -> Vec<u8> {
        let mut response_data = mock_response_data(); // Datos de las filas
        let meta_data = mock_meta_data(); // Datos de los metadatos
    
        let division_offset = response_data.len(); // Offset de división
        response_data.extend(meta_data); // Combina filas y metadatos
        response_data.extend(&division_offset.to_be_bytes()); // Agrega el offset como un entero de 4 bytes
    
        response_data
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

    #[test]
    fn test_new_response_manager() {
        let mut responses = HashMap::new();
        responses.insert(NodeIp::new_from_single_string("127.0.0.1:8080").unwrap(), rows());
        let manager = ResponseManager::new(&responses);
        assert!(manager.is_ok());
        let manager = manager.unwrap();
        assert_eq!(manager.responses_bytes.len(), 1);
        assert!(!manager.meta_data_bytes.is_empty());
    }

    #[test]
    fn test_get_ips() {
        let mut responses = HashMap::new();
        responses.insert(NodeIp::new_from_single_string("127.0.0.1:8080").unwrap(), rows());
        let manager = ResponseManager::new(&responses).unwrap();
        let ips = manager.get_ips();
        assert_eq!(ips, vec!["127.0.0.1:8080".to_string()]);
    }

    #[test]
    fn test_some_row_is_deleted() {
        let mut responses = HashMap::new();
        responses.insert(NodeIp::new_from_single_string("127.0.0.1:8080").unwrap(), rows());
        let manager = ResponseManager::new(&responses).unwrap();
        // Aquí debes simular un row eliminado para hacer que la función retorne verdadero
        assert!(!manager.some_row_is_deleted().unwrap());
    }

    #[test]
    fn test_get_keyspace_table() {
        let mut responses = HashMap::new();
        responses.insert(NodeIp::new_from_single_string("127.0.0.1:8080").unwrap(), rows());
        let manager = ResponseManager::new(&responses).unwrap();
        let result = manager.get_keyspace_table("127.0.0.1:8080");
        assert!(result.is_ok());
        let (keyspace, table) = result.unwrap();
        assert_eq!(keyspace, "test_keyspace"); // Cambiar por un valor esperado
        assert_eq!(table, "test_table");     // Cambiar por un valor esperado
    }
}

