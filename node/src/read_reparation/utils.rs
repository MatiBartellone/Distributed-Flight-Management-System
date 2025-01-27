use std::collections::HashMap;

use crate::{
    data_access::{column::Column, row::Row},
    utils::{errors::Errors, types::bytes_cursor::BytesCursor},
};

pub fn split_bytes(data: &[u8]) -> Result<(Vec<u8>, Vec<u8>), Errors> {
    let division_offset = &data[data.len() - 4..];
    let mut cursor = BytesCursor::new(division_offset);
    let division = cursor.read_int()? as usize;
    let data_section = data[..division].to_vec();
    let timestamps_section = data[division..data.len() - 4].to_vec();
    Ok((data_section, timestamps_section))
}

pub fn to_hash_columns(columns: Vec<Column>) -> HashMap<String, Column> {
    let mut hash: HashMap<String, Column> = HashMap::new();
    for column in columns {
        hash.insert(column.column_name.clone(), column);
    }
    hash
}

pub fn to_hash_rows(rows: Vec<Row>) -> HashMap<Vec<String>, Row> {
    let mut hash: HashMap<Vec<String>, Row> = HashMap::new();
    for row in rows {
        hash.insert(row.primary_key.clone(), row);
    }
    hash
}

#[cfg(test)]
mod tests {
    use crate::{
        data_access::{column::Column, row::Row},
        parsers::tokens::{data_type::DataType, literal::Literal},
        read_reparation::utils::{split_bytes, to_hash_columns, to_hash_rows},
        utils::types::timestamp::Timestamp,
    };

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
    fn test_split_bytes() {
        let data = vec![1, 2, 3, 4, 5, 6, 7, 8, 0, 0, 0, 4];

        let result = split_bytes(&data);
        assert!(result.is_ok());
        let (data_section, timestamps_section) = result.unwrap();
        assert_eq!(data_section, vec![1, 2, 3, 4]); // Sección inicial
        assert_eq!(timestamps_section, vec![5, 6, 7, 8]); // Sección intermedia
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
