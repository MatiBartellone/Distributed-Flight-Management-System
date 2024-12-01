#[cfg(test)]
mod tests {
    use crate::utils::test_functions::{add_one_finished, check_and_run_teardown, get_query_result, get_rows_select, setup};

    #[test]
    fn select_test_one_row() {
        setup();
        get_query_result("INSERT INTO test.sel (id, name, age, height) VALUES (1, 'Mati', 11, 43)").unwrap();
        let result = get_query_result("select * from test.sel WHERE id = 1");
        assert!(result.is_ok());
        let rows = get_rows_select(result.unwrap());
        assert_eq!(rows.len(), 1);
        let row = rows.get(0).unwrap();
        let row_hash = row.get_row_hash();
        assert_eq!(row_hash.get("id").unwrap().value, "1");
        assert_eq!(row_hash.get("name").unwrap().value, "Mati");
        assert_eq!(row_hash.get("age").unwrap().value, "11");
        assert_eq!(row_hash.get("height").unwrap().value, "43");
        add_one_finished();
        check_and_run_teardown();
    }
}
