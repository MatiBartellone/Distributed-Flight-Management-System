
use crate::{data_access::{row::Row, column::Column}, utils::types::timestamp::Timestamp};

use super::utils::{to_hash_rows, to_hash_columns};

pub struct RowComparer;

impl RowComparer {

    pub fn compare_response(original: Vec<Row>, next: Vec<Row>) -> Vec<Row> {
        let mut original_map = to_hash_rows(original);
        let mut res: Vec<Row> = Vec::new();

        for next_row in &next {
            if let Some(ori_row) = original_map.get(&next_row.primary_key) {
                let row = Self::compare_row(ori_row, next_row);
                res.push(row);
                original_map.remove(&next_row.primary_key);
            } else {
                res.push(next_row.clone());
            }
        }

        for rows_remaining in original_map.values() {
            res.push(rows_remaining.clone());
        }

        res
    }

    pub fn compare_row(original: &Row, new: &Row) -> Row {
        let mut best_columns: Vec<Column> = Vec::new();
        let new_map = to_hash_columns(new.columns.clone());

        for col_ori in &original.columns {
            if let Some(col_new) = new_map.get(&col_ori.column_name) {
                if col_ori.timestamp.is_older_than(Timestamp::new_from_timestamp(&col_new.timestamp))
                {
                    best_columns.push(Column::new_from_column(col_new));
                } else {
                    best_columns.push(Column::new_from_column(col_ori));
                }
            }
        }

        let mut res = Row::new(best_columns, original.primary_key.clone());
        if original.timestamp().is_older_than(new.timestamp()) {
            res.set_timestamp(new.timestamp());
            res.deleted = new.is_deleted();
        }
        res
    }
}


#[cfg(test)]
mod tests {
    use crate::{data_access::{row::Row, column::Column}, parsers::tokens::{data_type::DataType, literal::Literal}, utils::types::timestamp::Timestamp, read_reparation::row_comparer::RowComparer};


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

        let result = RowComparer::compare_row(&original_row, &new_row);

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

        let result = RowComparer::compare_response(original_rows, new_rows);
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].primary_key, vec!["pk1"]);
        assert_eq!(
            result[0].columns[0].value,
            Literal::new("new_value1".to_string(), DataType::Text)
        );
        assert_eq!(result[1].primary_key, vec!["pk3"]);
        assert_eq!(result[2].primary_key, vec!["pk2"]);
    }
}

