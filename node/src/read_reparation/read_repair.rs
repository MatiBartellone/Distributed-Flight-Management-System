use std::collections::HashMap;

use super::repair_row::RepairRow;
use super::row_response::RowResponse;
use crate::data_access::column::Column;
use crate::utils::types::bytes_cursor::BytesCursor;
use crate::utils::types::node_ip::NodeIp;
use crate::utils::types::timestamp::Timestamp;
use crate::{
    data_access::row::Row,
    parsers::{
        query_parser::query_parser,
        tokens::{data_type::DataType, token::Token},
    },
    query_delegation::query_delegator::QueryDelegator,
    utils::{constants::BEST, errors::Errors, response::Response},
};

pub struct ReadRepair {
    responses_bytes: HashMap<String, Vec<u8>>,
    meta_data_bytes: HashMap<String, Vec<u8>>,
    repair_row: RepairRow,
}

impl ReadRepair {
    pub fn new(responses: &HashMap<NodeIp, Vec<u8>>) -> Result<Self, Errors> {
        let mut responses_bytes = HashMap::new();
        let mut meta_data_bytes = HashMap::new();

        for (ip, response) in responses {
            let (response_node, meta_data_response) = ReadRepair::split_bytes(response)?;
            responses_bytes.insert(ip.get_string_ip(), response_node);
            meta_data_bytes.insert(ip.get_string_ip(), meta_data_response);
        }

        Ok(Self {
            responses_bytes,
            meta_data_bytes,
            repair_row: RepairRow::new(),
        })
    }

    pub fn get_response(&mut self) -> Result<Vec<u8>, Errors> {
        if self.repair_innecesary()? {
            return self.get_first_response();
        }
        self.get_better_response()?;
        self.repair()?;
        self.cast_to_protocol_row(BEST)
    }

    //Verificacion

    fn get_first_response(&self) -> Result<Vec<u8>, Errors> {
        if let Some((ip, _)) = self.responses_bytes.iter().next() {
            self.cast_to_protocol_row(ip)
        } else {
            Err(Errors::ServerError("There is not responses".to_string()))
        }
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

    fn repair_innecesary(&self) -> Result<bool, Errors> {
        if !self.all_responses_equal()? {
            return Ok(false);
        }
        if self.some_row_is_deleted()? {
            return Ok(false);
        }
        Ok(true)
    }


    //Obtener BEST

    fn get_better_response(&mut self) -> Result<(), Errors> {
        let mut ips = self.responses_bytes.keys();
        let first_ip = ips
            .next()
            .ok_or_else(|| Errors::ServerError("No response found".to_string()))?;

        let mut rows = self.read_rows(first_ip)?;
        for ip in ips {
            let next_response = self.read_rows(ip)?;
            rows = compare_response(rows, next_response);
        }
        let (keyspace, table) = self.get_keyspace_table(first_ip)?;
        let column = self.get_columns(first_ip)?;
        let betters = Response::rows(rows, &keyspace, &table, &column)?;
        let (best_rows, best_meta_data) = ReadRepair::split_bytes(&betters)?;
        self.responses_bytes.insert(BEST.to_owned(), best_rows);
        self.meta_data_bytes.insert(BEST.to_owned(), best_meta_data);
        self.set_repair_row()?;
        Ok(())
    }

    fn set_repair_row(&mut self) -> Result<(), Errors> {
        let (keyspace, table) = self.get_keyspace_table(BEST)?;
        let pks = self.get_pks_headers(BEST)?;
        self.repair_row
            .initializer(keyspace, table, pks.keys().cloned().collect());
        Ok(())
    }


    //REPARACION
    
    fn repair(&self) -> Result<(), Errors> {
        let better_response = self.cast_to_protocol_row(BEST)?;
        for ip in self.responses_bytes.keys() {
            let response = self.cast_to_protocol_row(ip)?;
            if response != better_response {
                self.repair_node(ip)?;
            }
        }
        Ok(())
    }

    fn repair_node(&self, ip: &str) -> Result<(), Errors> {
        let node_rows = self.read_rows(ip)?;
        let mut best = to_hash_rows(self.read_rows(BEST)?);

        self.process_existing_rows(ip, &node_rows, &mut best)?;
        self.process_remaining_rows(ip, &best)?;

        Ok(())
    }

    fn process_existing_rows(
        &self,
        ip: &str,
        node_rows: &[Row],
        best: &mut HashMap<Vec<String>, Row>,
    ) -> Result<(), Errors> {
        for node_row in node_rows {
            if let Some(best_row) = best.remove(&node_row.primary_key) {
                let (change_row, query) =
                    self.repair_row.repair_row(best_row.clone(), node_row.clone())?;
                if change_row {
                    ReadRepair::send_reparation(query, ip)?;
                }
            }
        }
        Ok(())
    }

    fn process_remaining_rows(
        &self,
        ip: &str,
        best: &HashMap<Vec<String>, Row>,
    ) -> Result<(), Errors> {
        for row in best.values() {
            if !row.is_deleted() {
                let query = self.repair_row.create_insert(row)?;
                ReadRepair::send_reparation(query, ip)?;
            }
        }
        Ok(())
    }

    fn send_reparation(query: Vec<Token>, ip: &str) -> Result<(), Errors> {
        let query_parsed = query_parser(query)?;
        let node_ip = NodeIp::new_from_single_string(ip)?;
        QueryDelegator::send_to_node(node_ip, query_parsed)?;
        Ok(())
    }


    //Auxiliares 

    fn read_rows(&self, ip: &str) -> Result<Vec<Row>, Errors> {
        let protocol = self.responses_bytes.get(ip).ok_or_else(|| {
            Errors::ServerError(format!("Key {} not found in responses_bytes", ip))
        })?;
        RowResponse::read_rows(protocol.to_vec())
    }

    fn get_keyspace_table(&self, ip: &str) -> Result<(String, String), Errors> {
        let bytes = self.meta_data_bytes.get(ip).ok_or_else(|| {
            Errors::ServerError(format!("Key {} not found in meta_data_bytes", ip))
        })?;
        let data_response = RowResponse::read_meta_data_response(bytes.to_vec())?;
        Ok((
            data_response.keyspace().to_string(),
            data_response.table().to_string(),
        ))
    }

    fn get_pks_headers(&self, ip: &str) -> Result<HashMap<String, DataType>, Errors> {
        let bytes = self.meta_data_bytes.get(ip).ok_or_else(|| {
            Errors::ServerError(format!("Key {} not found in meta_data_bytes", ip))
        })?;
        let data_response = RowResponse::read_meta_data_response(bytes.to_vec())?;
        Ok(data_response.headers_pks().clone())
    }

    fn get_columns(&self, ip: &str) -> Result<Vec<String>, Errors> {
        let bytes = self.meta_data_bytes.get(ip).ok_or_else(|| {
            Errors::ServerError(format!("Key {} not found in meta_data_bytes", ip))
        })?;
        let data_response = RowResponse::read_meta_data_response(bytes.to_vec())?;
        Ok(data_response.colums())
    }

    fn cast_to_protocol_row(&self, ip: &str) -> Result<Vec<u8>, Errors> {
        let rows = self.read_rows(ip)?;
        let (keyspace, table) = self.get_keyspace_table(ip)?;
        let columns = self.get_columns(ip)?;
        Response::protocol_row(rows, &keyspace, &table, columns)
    }

    fn split_bytes(data: &[u8]) -> Result<(Vec<u8>, Vec<u8>), Errors> {
        let division_offset = &data[data.len() - 4..];
        let mut cursor = BytesCursor::new(division_offset);
        let division = cursor.read_int()? as usize;
        let data_section = data[..division].to_vec();
        let timestamps_section = data[division..data.len() - 4].to_vec();
        Ok((data_section, timestamps_section))
    }
}

//Entre dos respuestas, crea una que tenga los mejores time stamp para cada columna de cada row
fn compare_response(original: Vec<Row>, next: Vec<Row>) -> Vec<Row> {
    let mut original_map = to_hash_rows(original);
    let mut res: Vec<Row> = Vec::new();
    for next_row in &next {
        if let Some(ori_row) = original_map.get(&next_row.primary_key) {
            let row = compare_row(ori_row, next_row);
            res.push(row);
            original_map.remove(&next_row.primary_key);
        } else {
            res.push(next_row.clone())
        }
    }
    for rows_remaining in original_map.values() {
        res.push(rows_remaining.clone())
    }
    res
}

//Crea una row quedanse con las columnas que tengan mejor time stamp
fn compare_row(original: &Row, new: &Row) -> Row {
    let mut best_columns: Vec<Column> = Vec::new();
    let new_map = to_hash_columns(new.columns.clone());
    //Se queda con las mejores columnas
    for col_ori in &original.columns {
        if let Some(col_new) = new_map.get(&col_ori.column_name) {
            if col_ori
                .timestamp
                .is_older_than(Timestamp::new_from_timestamp(&col_new.timestamp))
            {
                best_columns.push(Column::new_from_column(col_new));
            } else {
                best_columns.push(Column::new_from_column(col_ori));
            }
        }
    }
    let mut res = Row::new(best_columns, original.primary_key.clone());
    //Se queda con el deleted de mejor timestump
    if original.timestamp().is_older_than(new.timestamp()) {
        res.set_timestamp(new.timestamp());
        res.deleted = new.is_deleted();
    }
    res
}

fn to_hash_rows(rows: Vec<Row>) -> HashMap<Vec<String>, Row> {
    let mut hash: HashMap<Vec<String>, Row> = HashMap::new();
    for row in rows {
        hash.insert(row.primary_key.clone(), row);
    }
    hash
}

fn to_hash_columns(columns: Vec<Column>) -> HashMap<String, Column> {
    let mut hash: HashMap<String, Column> = HashMap::new();
    for column in columns {
        hash.insert(column.column_name.clone(), column);
    }
    hash
}

#[cfg(test)]
mod tests {
    use std::net::IpAddr;

    use crate::parsers::tokens::literal::Literal;

    use super::*;

    fn create_test_column(name: &str, value: &str, timestamp: i64) -> Column {
        Column {
            column_name: name.to_string(),
            value: Literal::new(value.to_string(), DataType::Text),
            timestamp: Timestamp::new_from_i64(timestamp),
        }
    }

    fn create_test_row(primary_key: Vec<&str>, columns: Vec<Column>) -> Row {
        Row::new(columns, primary_key.into_iter().map(String::from).collect())
    }

    #[test]
    fn test_new() {
        let ip: IpAddr = "127.0.0.1".parse().unwrap();
        let port = 8080;
        let node_ip = NodeIp::new(ip, port);

        let mut responses = HashMap::new();
        responses.insert(node_ip, vec![1, 2, 3, 4, 0, 0, 0, 4]);

        let result = ReadRepair::new(&responses);
        assert!(result.is_ok());

        let read_repair = result.unwrap();
        assert_eq!(read_repair.responses_bytes.len(), 1);
        assert_eq!(read_repair.meta_data_bytes.len(), 1);
    }

    #[test]
    fn test_split_bytes() {
        let data = vec![1, 2, 3, 4, 5, 6, 7, 8, 0, 0, 0, 4];

        let result = ReadRepair::split_bytes(&data);
        assert!(result.is_ok());
        let (data_section, timestamps_section) = result.unwrap();
        assert_eq!(data_section, vec![1, 2, 3, 4]); // Sección inicial
        assert_eq!(timestamps_section, vec![5, 6, 7, 8]); // Sección intermedia
    }

    #[test]
    fn test_compare_row() {
        let original_row = create_test_row(
            vec!["pk1"],
            vec![
                create_test_column("col1", "value1", 100),
                create_test_column("col2", "value2", 200),
            ],
        );

        let new_row = create_test_row(
            vec!["pk1"],
            vec![
                create_test_column("col1", "new_value1", 300),
                create_test_column("col2", "value2", 150),
            ],
        );

        let result = compare_row(&original_row, &new_row);

        assert_eq!(result.primary_key, vec!["pk1"]);
        assert_eq!(result.columns.len(), 2);
        assert_eq!(
            result.columns[0].value,
            Literal::new("new_value1".to_string(), DataType::Text)
        );
        assert_eq!(
            result.columns[1].value,
            Literal::new("value2".to_string(), DataType::Text)
        );
    }

    #[test]
    fn test_compare_response() {
        let original_rows = vec![
            create_test_row(vec!["pk1"], vec![create_test_column("col1", "value1", 100)]),
            create_test_row(vec!["pk2"], vec![create_test_column("col1", "value2", 200)]),
        ];

        let new_rows = vec![
            create_test_row(
                vec!["pk1"],
                vec![create_test_column("col1", "new_value1", 300)],
            ),
            create_test_row(vec!["pk3"], vec![create_test_column("col1", "value3", 150)]),
        ];

        let result = compare_response(original_rows, new_rows);
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].primary_key, vec!["pk1"]);
        assert_eq!(
            result[0].columns[0].value,
            Literal::new("new_value1".to_string(), DataType::Text)
        );
        assert_eq!(result[1].primary_key, vec!["pk3"]);
        assert_eq!(result[2].primary_key, vec!["pk2"]);
    }

    #[test]
    fn test_to_hash_rows() {
        let rows = vec![
            create_test_row(vec!["pk1"], vec![create_test_column("col1", "value1", 100)]),
            create_test_row(vec!["pk2"], vec![create_test_column("col1", "value2", 200)]),
        ];

        let hash = to_hash_rows(rows);

        assert!(hash.contains_key(&vec!["pk1".to_string()]));
        assert!(hash.contains_key(&vec!["pk2".to_string()]));
    }

    #[test]
    fn test_to_hash_columns() {
        let columns = vec![
            create_test_column("col1", "value1", 100),
            create_test_column("col2", "value2", 200),
        ];

        let hash = to_hash_columns(columns);

        assert!(hash.contains_key("col1"));
        assert!(hash.contains_key("col2"));
    }
}
