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
