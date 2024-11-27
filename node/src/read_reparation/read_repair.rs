use std::collections::HashMap;

use crate::{utils::{errors::Errors, bytes_cursor::BytesCursor, response::Response, constants::BEST, node_ip::NodeIp, token_conversor::{create_identifier_token, create_reserved_token, create_iterate_list_token, create_comparison_operation_token, create_token_from_literal, create_logical_operation_token, create_token_literal}}, data_access::row::Row, parsers::{tokens::{token::Token, data_type::DataType}, query_parser::query_parser}, query_delegation::query_delegator::QueryDelegator};
use crate::parsers::tokens::terms::ComparisonOperators::Equal;
use crate::parsers::tokens::terms::LogicalOperators::And;
use super::row_response::RowResponse;



pub struct ReadRepair {
    responses_bytes: HashMap<String, Vec<u8>>,
    meta_data_bytes: HashMap<String, Vec<u8>>,
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


    fn repair(&self) -> Result<(), Errors> {
        let better_response = self.cast_to_protocol_row(BEST)?;
        for (ip, _) in &self.responses_bytes {
            let response = self.cast_to_protocol_row(ip)?;
            if response != better_response {
                self.repair_node(ip)?;
            }
        }
        Ok(())
    }

    fn repair_node(&self, ip: &str) -> Result<(), Errors> {
        let node_rows = self.read_rows(ip)?;
        let best = self.read_rows(BEST)?;
        let (keyspace, table) = self.get_keyspace_table(BEST)?;
        let mut change_row = false;
        for (row, better_row) in node_rows.iter().zip(best){
            let mut query : Vec<Token> = Vec::new();
            query.push(create_reserved_token("UPDATE"));
            query.push(create_identifier_token(&format!("{}.{}", keyspace, table)));
            query.push(Token::Reserved("SET".to_owned()));
            for (column, column_better) in row.columns.iter().zip(better_row.columns) {
                if column.value.value != column_better.value.value {
                    query.push(create_iterate_list_token(vec![
                        create_identifier_token(&column_better.column_name),
                        create_comparison_operation_token(Equal),
                        create_token_from_literal(column_better.value),
                    ]));
                    change_row = true
                }
            } 
            if change_row {
                let pks = self.get_pks_headers(BEST)?;
                query.push(create_identifier_token("WHERE"));
                let mut sub_where: Vec<Token> = Vec::new();
                for (i, (pks_header, pk_value)) in pks.iter().zip(row.primary_key.clone()).enumerate() {
                    sub_where.push(create_identifier_token(pks_header.0));
                    sub_where.push(create_comparison_operation_token(Equal));
                    sub_where.push(create_token_literal(&pk_value, pks_header.1.clone()));
                    if i < pks.len() - 1 {
                        sub_where.push(create_logical_operation_token(And))
                    }
                }
                query.push(create_iterate_list_token(sub_where));
                dbg!(&query);
                let query_parsed = query_parser(query)?;
                QueryDelegator::send_to_node(NodeIp::new_from_single_string(ip)?, query_parsed)?;
                change_row = false;
            }
        }
        Ok(())
    }
    

    fn get_better_response(&mut self) -> Result<(), Errors> {
        let mut ips = self.responses_bytes.keys();
        let first_ip = ips.next().ok_or_else(|| Errors::ServerError("No response found".to_string()))?;
        let mut rows = self.read_rows(first_ip)?;
        for ip in ips {
            let next_response = self.read_rows(ip)?;
            compare_response(&mut rows, next_response);
        }
        let (keyspace, table) = self.get_keyspace_table(first_ip)?;
        let betters = Response::rows(rows, &keyspace, &table)?;
        let (best_row, best_meta_data) = ReadRepair::split_bytes(&betters)?;
        self.responses_bytes.insert(BEST.to_owned(), best_row);
        self.meta_data_bytes.insert(BEST.to_owned(), best_meta_data);
        Ok(())
    }    

    fn read_rows(&self, ip: &str) -> Result<Vec<Row>, Errors> {
        let protocol = self.responses_bytes.get(ip).ok_or_else(|| Errors::ServerError(format!("Key {} not found in responses_bytes", ip)))?;
        RowResponse::read_rows(protocol.to_vec())
    }

    fn get_keyspace_table(&self, ip: &str) -> Result<(String, String), Errors> {
        let bytes = self.meta_data_bytes.get(ip).ok_or_else(|| Errors::ServerError(format!("Key {} not found in meta_data_bytes", ip)))?;
        let data_response = RowResponse::read_meta_data_response(bytes.to_vec())?;
        Ok((data_response.keyspace().to_string(), data_response.table().to_string()))
    }

    fn get_pks_headers(&self, ip: &str) -> Result<HashMap<String, DataType>, Errors> {
        let bytes = self.meta_data_bytes.get(ip).ok_or_else(|| Errors::ServerError(format!("Key {} not found in meta_data_bytes", ip)))?;
        let data_response = RowResponse::read_meta_data_response(bytes.to_vec())?;
        Ok(data_response.headers_pks().clone())
    }

    fn cast_to_protocol_row(&self, ip: &str) -> Result<Vec<u8>, Errors> {
        let rows = self.read_rows(ip)?;
        let (keyspace, table) = self.get_keyspace_table(ip)?;
        Response::protocol_row(rows, &keyspace, &table)
    }


    fn get_first_response(&self) -> Result<Vec<u8>, Errors> {
        if let Some((ip, _)) = self.responses_bytes.iter().next() {
            self.cast_to_protocol_row(ip)
        } else {
            Err(Errors::ServerError("There is not responses".to_string())) 
        }
    }

    fn repair_innecesary(&self) -> Result<bool, Errors> {
        let mut responses: Vec<Vec<u8>> = Vec::new();
        for (ip, _) in &self.responses_bytes {
            responses.push(self.cast_to_protocol_row(&ip)?)
        }
        if responses.is_empty() {
            return Ok(true); 
        }
        let first_response = &responses[0];
        let all_equal = responses.iter().all(|response| response == first_response);
        Ok(all_equal)
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




fn compare_response(original: &mut [Row], new: Vec<Row>) {
    for (ori_row, new_row) in original.iter_mut().zip(new) {
        compare_row(ori_row, new_row);
    }
}

fn compare_row(original: &mut Row, new: Row) {
    for (col_ori, col_new) in original.columns.iter_mut().zip(new.columns) {
        if col_ori.time_stamp < col_new.time_stamp {
            *col_ori = col_new;
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::data_access::row::Column;
    use crate::parsers::tokens::data_type::DataType;
    use crate::parsers::tokens::literal::Literal;

    use super::*;
    use std::collections::HashMap;
    use std::net::IpAddr;

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
    fn test_get_first_response() {
        let mut read_repair = ReadRepair {
            responses_bytes: HashMap::new(),
            meta_data_bytes: HashMap::new(),
        };

        // Agregar una respuesta
        read_repair.responses_bytes.insert("127.0.0.1".to_string(), vec![0, 1, 2, 3]);

        let response = read_repair.get_first_response();
        assert!(response.is_ok());
        assert_eq!(response.unwrap(), vec![0, 1, 2, 3]);
    }

    #[test]
    fn test_split_bytes() {
        let data = vec![
            1, 2, 3, 4,   
            5, 6, 7, 8,   
            0, 0, 0, 4    
        ];

        let result = ReadRepair::split_bytes(&data);
        assert!(result.is_ok());
        let (data_section, timestamps_section) = result.unwrap();
        assert_eq!(data_section, vec![1, 2, 3, 4]); // Sección inicial
        assert_eq!(timestamps_section, vec![5, 6, 7, 8]); // Sección intermedia
    }

    #[test]
    fn test_compare_row() {
        // Crear columnas
        let original_column = Column::new(
            &"col1".to_string(),
            &Literal {
                value: "old".to_string(),
                data_type: DataType::Text, // Ajusta este tipo si es necesario
            },
            1,
        );

        let new_column = Column::new(
            &"col1".to_string(),
            &Literal {
                value: "new".to_string(),
                data_type: DataType::Text, // Ajusta este tipo si es necesario
            },
            2,
        );

        // Crear filas (rows)
        let mut original = Row::new(vec![original_column], vec!["pk1".to_string()]);
        let new = Row::new(vec![new_column], vec!["pk1".to_string()]);

        // Comparar y verificar
        compare_row(&mut original, new);
        assert_eq!(original.columns[0].value.value, "new");
        assert_eq!(original.columns[0].time_stamp, 2);
    }
}

