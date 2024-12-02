
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
        if original.is_deleted() != new.is_deleted() {
            if original.timestamp().is_newer_than(new.timestamp()) {
                return  original.clone();
            } else {
                return new.clone();
            }
        }
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
        } else {
            res.set_timestamp(original.timestamp());
            res.deleted = original.is_deleted();
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
    
    fn create_special_row(primary_key: Vec<&str>, columns: Vec<Column>, deletd: bool, timestamp: i64) -> Row {
        let mut row = Row::new(columns, primary_key.into_iter().map(String::from).collect());
        row.deleted = deletd;
        row.set_timestamp(Timestamp::new_from_i64(timestamp));
        row
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

    #[test]
    fn test_better_3_responses() {
        let response_1 = vec![
            create_special_row(vec!["pk1"], vec![create_test_column("col1", "value1", 100)], true, 100),
            create_special_row(vec!["pk3"], vec![create_test_column("col1", "value3", 110)], false, 110), //Best option
        ];

        let response_2 = vec![
            create_special_row(vec!["pk1"], vec![create_test_column("col1", "value1", 100)], false, 110), //Best option
            create_special_row(vec!["pk2"], vec![create_test_column("col1", "value2", 100)], true, 100),
        ];

        let response_3 = vec![
            create_special_row(vec!["pk2"], vec![create_test_column("col1", "value2", 100)], false, 110), //Best Option
            create_special_row(vec!["pk3"], vec![create_test_column("col1", "value2", 100)], true, 100),
        ];

        let response = RowComparer::compare_response(response_1, response_2);
        let best = RowComparer::compare_response(response, response_3);

        let expected = vec![
            create_special_row(vec!["pk2"], vec![create_test_column("col1", "value2", 100)], false, 110),
            create_special_row(vec!["pk3"], vec![create_test_column("col1", "value3", 110)], false, 110), 
            create_special_row(vec!["pk1"], vec![create_test_column("col1", "value1", 100)], false, 110),
        ];

        assert_eq!(expected, best)
    }


    #[test]
    fn test_better_3_responses_with_reparation() {
        let response_1 = vec![
            create_special_row(
                vec!["pk1"], 
                vec![
                    create_test_column("col1", "value1a", 1),
                    create_test_column("col2", "value1a", 2),
                    create_test_column("col3", "value1a", 3),
                ],
                true, 
                100),
            create_special_row(
                vec!["pk3"], 
                vec![
                    create_test_column("col1", "value3a", 1),
                    create_test_column("col2", "value3a", 2),
                    create_test_column("col3", "value3a", 3),
                ],
                false, 
                110),
        ];

        let response_2 = vec![
            create_special_row(
                vec!["pk1"], 
                vec![
                    create_test_column("col1", "value1b", 4),
                    create_test_column("col2", "value1b", 5),
                    create_test_column("col3", "value1b", 6),
                ],
                false, 
                110),
            create_special_row(
                vec!["pk2"], 
                vec![
                    create_test_column("col1", "value2b", 4),
                    create_test_column("col2", "value2b", 5),
                    create_test_column("col3", "value2b", 6),
                ],
                true, 
                100),
        ];

        let response_3 = vec![
            create_special_row(
                vec!["pk2"], 
                vec![
                    create_test_column("col1", "value2c", 7),
                    create_test_column("col2", "value2c", 8),
                    create_test_column("col3", "value2c", 9),
                ],
                false, 
                110),
            create_special_row(
                vec!["pk3"], 
                vec![
                    create_test_column("col1", "value3c", 7),
                    create_test_column("col2", "value3c", 8),
                    create_test_column("col3", "value3c", 9),
                ],
                true, 
                100),
        ];

        let response = RowComparer::compare_response(response_1, response_2);
        let best = RowComparer::compare_response(response, response_3);

        let expected = vec![
            create_special_row(
                vec!["pk2"], 
                vec![
                    create_test_column("col1", "value2c", 7),
                    create_test_column("col2", "value2c", 8),
                    create_test_column("col3", "value2c", 9),
                ],
                false, 
                110),
            create_special_row(
                vec!["pk3"], 
                vec![
                    create_test_column("col1", "value3a", 1),
                    create_test_column("col2", "value3a", 2),
                    create_test_column("col3", "value3a", 3),
                ],
                false, 
                110), 
            create_special_row(
                vec!["pk1"], 
                vec![
                    create_test_column("col1", "value1b", 4),
                    create_test_column("col2", "value1b", 5),
                    create_test_column("col3", "value1b", 6),
                ],
                false, 
                110),
        ];

        assert_eq!(expected, best)
    }


    #[test]
    fn test_better_6_responses_with_reparation() {
        let response_1 = vec![
            create_special_row(
                vec!["pk1"], 
                vec![
                    create_test_column("col1", "value1a", 1),
                    create_test_column("col2", "value1a", 2),
                    create_test_column("col3", "value1a", 3),
                ],
                true, 
                100),
            create_special_row(
                vec!["pk3"], 
                vec![
                    create_test_column("col1", "value3_1a", 1),
                    create_test_column("col2", "value3_1a", 20),
                    create_test_column("col3", "value3_1a", 1),
                ],
                false, 
                110),
        ];

        let response_2 = vec![
            create_special_row(
                vec!["pk1"], 
                vec![
                    create_test_column("col1", "value1_2b", 1),
                    create_test_column("col2", "value1_2b", 20),
                    create_test_column("col3", "value1_2b", 20),
                ],
                false, 
                110),
            create_special_row(
                vec!["pk2"], 
                vec![
                    create_test_column("col1", "value2b", 4),
                    create_test_column("col2", "value2b", 5),
                    create_test_column("col3", "value2b", 6),
                ],
                true, 
                100),
        ];

        let response_3 = vec![
            create_special_row(
                vec!["pk2"], 
                vec![
                    create_test_column("col1", "value2_3c", 1),
                    create_test_column("col2", "value2_3c", 1),
                    create_test_column("col3", "value2_3c", 20),
                ],
                false, 
                110),
            create_special_row(
                vec!["pk3"], 
                vec![
                    create_test_column("col1", "value3c", 7),
                    create_test_column("col2", "value3c", 8),
                    create_test_column("col3", "value3c", 9),
                ],
                true, 
                100),
        ];

        let response_4 = vec![
            create_special_row(
                vec!["pk1"], 
                vec![
                    create_test_column("col1", "value1a", 1),
                    create_test_column("col2", "value1a", 2),
                    create_test_column("col3", "value1a", 3),
                ],
                true, 
                100),
            create_special_row(
                vec!["pk3"], 
                vec![
                    create_test_column("col1", "value3_4a", 20),
                    create_test_column("col2", "value3_4a", 1),
                    create_test_column("col3", "value3_4a", 20),
                ],
                false, 
                110),
        ];

        let response_5 = vec![
            create_special_row(
                vec!["pk1"], 
                vec![
                    create_test_column("col1", "value1_5b", 20),
                    create_test_column("col2", "value1_5b", 1),
                    create_test_column("col3", "value1_5b", 1),
                ],
                false, 
                110),
            create_special_row(
                vec!["pk2"], 
                vec![
                    create_test_column("col1", "value2b", 4),
                    create_test_column("col2", "value2b", 5),
                    create_test_column("col3", "value2b", 6),
                ],
                true, 
                100),
        ];

        let response_6 = vec![
            create_special_row(
                vec!["pk2"], 
                vec![
                    create_test_column("col1", "value2_6c", 20),
                    create_test_column("col2", "value2_6c", 20),
                    create_test_column("col3", "value2_6c", 1),
                ],
                false, 
                110),
            create_special_row(
                vec!["pk3"], 
                vec![
                    create_test_column("col1", "value3c", 7),
                    create_test_column("col2", "value3c", 8),
                    create_test_column("col3", "value3c", 9),
                ],
                true, 
                100),
        ];

        let aux0 = RowComparer::compare_response(response_1, response_2);
        let aux1 = RowComparer::compare_response(aux0, response_3);
        let aux2 = RowComparer::compare_response(aux1, response_4);
        let aux3 = RowComparer::compare_response(aux2, response_5);
        let aux4 = RowComparer::compare_response(aux3, response_6);

        let expected = vec![
            create_special_row(
                vec!["pk2"], 
                vec![
                    create_test_column("col1", "value2_6c", 20),
                    create_test_column("col2", "value2_6c", 20),
                    create_test_column("col3", "value2_3c", 20),
                ],
                false, 
                110),
            create_special_row(
                vec!["pk3"], 
                vec![
                    create_test_column("col1", "value3_4a", 20),
                    create_test_column("col2", "value3_1a", 20),
                    create_test_column("col3", "value3_4a", 20),
                ],
                false, 
                110), 
            create_special_row(
                vec!["pk1"], 
                vec![
                    create_test_column("col1", "value1_5b", 20),
                    create_test_column("col2", "value1_2b", 20),
                    create_test_column("col3", "value1_2b", 20),
                ],
                false, 
                110),
        ];

        assert_eq!(expected, aux4)
    }
}

