#[cfg(test)]
mod tests {
    use crate::utils::test_functions::{add_one_finished, check_and_run_teardown, get_query_result, get_rows_select, setup};

    #[test]
    fn insert_test() {
        setup();
        let result = get_query_result("INSERT INTO test.tb1 (id, name) VALUES (1, 'Mati')");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 1i32.to_be_bytes().to_vec());
        let select_result = get_query_result("SELECT * FROM test.tb1 WHERE id = 1");
        assert!(select_result.is_ok());
        let rows = get_rows_select(select_result.unwrap());
        assert_eq!(rows.len(), 1);
        let row = rows.get(0).unwrap();
        let row_hash = row.get_row_hash();
        assert!(row_hash.get("id").is_some());
        assert_eq!(row_hash.get("id").unwrap().value, "1");
        assert!(row_hash.get("name").is_some());
        assert_eq!(row_hash.get("name").unwrap().value, "Mati");
        add_one_finished();
        check_and_run_teardown();
    }

    #[test]
    fn insert_test2() {
        setup();
        let result = get_query_result("INSERT INTO test.tb1 (id, name) VALUES (2, 'Thiago')");
        assert!(result.is_ok());
        let select_result = get_query_result("SELECT * FROM test.tb1 WHERE id = 2");
        assert!(select_result.is_ok());
        let rows = get_rows_select(select_result.unwrap());
        assert_eq!(rows.len(), 1);
        let row = rows.get(0).unwrap();
        let row_hash = row.get_row_hash();
        assert!(row_hash.get("id").is_some());
        assert_eq!(row_hash.get("id").unwrap().value, "2");
        assert!(row_hash.get("name").is_some());
        assert_eq!(row_hash.get("name").unwrap().value, "Thiago");
        add_one_finished();
        check_and_run_teardown();
    }

    #[test]
    fn insert_test_pk_twice_changes_data() {
        setup();
        let result =
            get_query_result("INSERT INTO test.tb1 (id, name, second) VALUES (3, 'Thiago', 'Pacheco')");
        assert!(result.is_ok());
        let result = get_query_result(
            "INSERT INTO test.tb1 (id, name, second) VALUES (3, 'Thiago', 'Bartellone')",
        );
        assert!(result.is_ok());
        let select_result = get_query_result("SELECT * FROM test.tb1 WHERE id = 3");
        assert!(select_result.is_ok());
        let rows = get_rows_select(select_result.unwrap());
        assert_eq!(rows.len(), 1);
        let row = rows.get(0).unwrap();
        let row_hash = row.get_row_hash();
        assert!(row_hash.get("id").is_some());
        assert_eq!(row_hash.get("id").unwrap().value, "3");
        assert!(row_hash.get("name").is_some());
        assert_eq!(row_hash.get("name").unwrap().value, "Thiago");
        assert!(row_hash.get("second").is_some());
        assert_eq!(row_hash.get("second").unwrap().value, "Bartellone");
        add_one_finished();
        check_and_run_teardown();
    }

    #[test]
    fn insert_test_multiple_rows() {
        setup();
        let result1 = get_query_result("INSERT INTO test.tb1 (id, name) VALUES (6, 'Mati')");
        let result2 = get_query_result("INSERT INTO test.tb1 (id, name) VALUES (7, 'Ivan')");
        let result3 = get_query_result("INSERT INTO test.tb1 (id, name) VALUES (8, 'Thiago')");
        assert!(result1.is_ok());
        assert!(result2.is_ok());
        assert!(result3.is_ok());
        let select_result = get_query_result("SELECT * FROM test.tb1 WHERE id = 6");
        assert!(select_result.is_ok());
        assert_eq!(get_rows_select(select_result.unwrap()).len(), 1);
        let select_result = get_query_result("SELECT * FROM test.tb1 WHERE id = 7");
        assert!(select_result.is_ok());
        assert_eq!(get_rows_select(select_result.unwrap()).len(), 1);
        let select_result = get_query_result("SELECT * FROM test.tb1 WHERE id = 8");
        assert!(select_result.is_ok());
        assert_eq!(get_rows_select(select_result.unwrap()).len(), 1);
        add_one_finished();
        check_and_run_teardown();
    }

    #[test]
    fn insert_test_multiple_rows_same_partition() {
        setup();
        let result1 = get_query_result("INSERT INTO test.tb1 (id, name) VALUES (9, 'Mati')");
        let result2 = get_query_result("INSERT INTO test.tb1 (id, name) VALUES (9, 'Ivan')");
        let result3 = get_query_result("INSERT INTO test.tb1 (id, name) VALUES (9, 'Thiago')");
        assert!(result1.is_ok());
        assert!(result2.is_ok());
        assert!(result3.is_ok());
        let select_result = get_query_result("SELECT * FROM test.tb1 WHERE id = 9");
        assert!(select_result.is_ok());
        let rows = get_rows_select(select_result.unwrap());
        assert_eq!(rows.len(), 3);
        let row = rows.get(0).unwrap();
        let row_hash = row.get_row_hash();
        assert_eq!(row_hash.get("id").unwrap().value, "9");
        assert_eq!(row_hash.get("name").unwrap().value, "Mati");
        let row = rows.get(1).unwrap();
        let row_hash = row.get_row_hash();
        assert_eq!(row_hash.get("id").unwrap().value, "9");
        assert_eq!(row_hash.get("name").unwrap().value, "Ivan");
        let row = rows.get(2).unwrap();
        let row_hash = row.get_row_hash();
        assert_eq!(row_hash.get("id").unwrap().value, "9");
        assert_eq!(row_hash.get("name").unwrap().value, "Thiago");
        add_one_finished();
        check_and_run_teardown();
    }

    #[test]
    fn insert_test_only_one_value() {
        setup();
        let result = get_query_result("INSERT INTO test.tb1 (id, name) VALUES (4, 'Emi')");
        assert!(result.is_ok());
        add_one_finished();
        check_and_run_teardown();
    }

    #[test]
    fn insert_test_insert_pk_twice() {
        setup();
        let result = get_query_result("INSERT INTO test.tb1 (id, name) VALUES (5, 'Ivan')");
        assert!(result.is_ok());
        let result = get_query_result("INSERT INTO test.tb1 (id, name) VALUES (5, 'Ivan')");
        assert!(result.is_ok());
        add_one_finished();
        check_and_run_teardown();
    }

    #[test]
    fn insert_test_error_column_nonexistant() {
        setup();
        let result = get_query_result("INSERT INTO test.tb1 (id, last_name) VALUES (2, 'Mati')");
        assert!(result.is_err());
        add_one_finished();
        check_and_run_teardown();
    }

    #[test]
    fn insert_test_error_no_pk() {
        setup();
        let result = get_query_result("INSERT INTO test.tb1 (name) VALUES ('Ivan')");
        assert!(result.is_err());
        add_one_finished();
        check_and_run_teardown();
    }

    #[test]
    fn insert_test_error_more_values_than_headers() {
        setup();
        let result = get_query_result("INSERT INTO test.tb1 (id) VALUES (2, 'Mati')");
        assert!(result.is_err());
        add_one_finished();
        check_and_run_teardown();
    }

    #[test]
    fn insert_test_error_mismatched_types() {
        setup();
        let result = get_query_result("INSERT INTO test.tb1 (id, name) VALUES ('Mati', 2)");
        assert!(result.is_err());
        add_one_finished();
        check_and_run_teardown();
    }
}