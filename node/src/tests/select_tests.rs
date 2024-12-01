#[cfg(test)]
mod tests {
    use crate::utils::test_functions::{
        add_one_finished, check_and_run_teardown, get_query_result, get_rows_select, setup,
    };

    #[test]
    fn select_test_one_row() {
        setup();
        get_query_result("INSERT INTO test.sel (id, name, age, height) VALUES (1, 'Mati', 11, 43)")
            .unwrap();

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

    #[test]
    fn select_test_multiple_rows() {
        setup();
        get_query_result("INSERT INTO test.sel (id, name, age, height) VALUES (2, 'Mati', 11, 43)")
            .unwrap();
        get_query_result("INSERT INTO test.sel (id, name, age, height) VALUES (2, 'Ivan', 22, 55)")
            .unwrap();
        get_query_result(
            "INSERT INTO test.sel (id, name, age, height) VALUES (2, 'Thiago', 33, 67)",
        )
        .unwrap();

        let result = get_query_result("select * from test.sel WHERE id = 2");
        assert!(result.is_ok());
        let rows = get_rows_select(result.unwrap());
        assert_eq!(rows.len(), 3);

        let row_1 = rows.get(0).unwrap();
        assert_eq!(row_1.get_row_hash().get("name").unwrap().value, "Mati");
        assert_eq!(row_1.get_row_hash().get("age").unwrap().value, "11");
        assert_eq!(row_1.get_row_hash().get("height").unwrap().value, "43");

        let row_2 = rows.get(1).unwrap();
        assert_eq!(row_2.get_row_hash().get("name").unwrap().value, "Ivan");
        assert_eq!(row_2.get_row_hash().get("age").unwrap().value, "22");
        assert_eq!(row_2.get_row_hash().get("height").unwrap().value, "55");

        let row_3 = rows.get(2).unwrap();
        assert_eq!(row_3.get_row_hash().get("name").unwrap().value, "Thiago");
        assert_eq!(row_3.get_row_hash().get("age").unwrap().value, "33");
        assert_eq!(row_3.get_row_hash().get("height").unwrap().value, "67");

        add_one_finished();
        check_and_run_teardown();
    }

    #[test]
    fn select_test_multiple_filters() {
        setup();
        get_query_result("INSERT INTO test.sel (id, name, age, height) VALUES (3, 'Mati', 11, 43)")
            .unwrap();
        get_query_result("INSERT INTO test.sel (id, name, age, height) VALUES (3, 'Ivan', 22, 55)")
            .unwrap();
        get_query_result(
            "INSERT INTO test.sel (id, name, age, height) VALUES (3, 'Thiago', 33, 67)",
        )
        .unwrap();

        let result =
            get_query_result("select * from test.sel WHERE id = 3 AND age > 20 AND height < 60");
        assert!(result.is_ok());
        let rows = get_rows_select(result.unwrap());
        assert_eq!(rows.len(), 1);

        let row = rows.get(0).unwrap();
        let row_hash = row.get_row_hash();
        assert_eq!(row_hash.get("id").unwrap().value, "3");
        assert_eq!(row_hash.get("name").unwrap().value, "Ivan");
        assert_eq!(row_hash.get("age").unwrap().value, "22");
        assert_eq!(row_hash.get("height").unwrap().value, "55");

        add_one_finished();
        check_and_run_teardown();
    }

    #[test]
    fn select_test_no_results() {
        setup();
        get_query_result("INSERT INTO test.sel (id, name, age, height) VALUES (4, 'Mati', 11, 43)")
            .unwrap();

        let result = get_query_result("select * from test.sel WHERE id = 99");
        assert!(result.is_ok());
        let rows = get_rows_select(result.unwrap());
        assert_eq!(rows.len(), 0);

        add_one_finished();
        check_and_run_teardown();
    }

    #[test]
    fn select_test_order_by() {
        setup();
        get_query_result("INSERT INTO test.sel (id, name, age, height) VALUES (5, 'Mati', 11, 43)")
            .unwrap();
        get_query_result(
            "INSERT INTO test.sel (id, name, age, height) VALUES (5, 'Thiago', 22, 55)",
        )
        .unwrap();
        get_query_result("INSERT INTO test.sel (id, name, age, height) VALUES (5, 'Ivan', 33, 67)")
            .unwrap();

        let result = get_query_result("select * from test.sel WHERE id = 5 ORDER BY age DESC");
        assert!(result.is_ok());
        let rows = get_rows_select(result.unwrap());
        assert_eq!(rows.len(), 3);

        assert_eq!(rows[0].get_row_hash().get("age").unwrap().value, "33");
        assert_eq!(rows[1].get_row_hash().get("age").unwrap().value, "22");
        assert_eq!(rows[2].get_row_hash().get("age").unwrap().value, "11");

        add_one_finished();
        check_and_run_teardown();
    }

    #[test]
    fn select_test_order_by_string_column() {
        setup();
        get_query_result("INSERT INTO test.sel (id, name, age) VALUES (6, 'Thiago', 11)").unwrap();
        get_query_result("INSERT INTO test.sel (id, name, age) VALUES (6, 'Ivan', 22)").unwrap();
        get_query_result("INSERT INTO test.sel (id, name, age) VALUES (6, 'Mati', 33)").unwrap();

        let result = get_query_result("select * from test.sel WHERE id = 6 ORDER BY name ASC");
        assert!(result.is_ok());
        let rows = get_rows_select(result.unwrap());
        assert_eq!(rows.len(), 3);

        assert_eq!(rows[0].get_row_hash().get("name").unwrap().value, "Ivan");
        assert_eq!(rows[1].get_row_hash().get("name").unwrap().value, "Mati");
        assert_eq!(rows[2].get_row_hash().get("name").unwrap().value, "Thiago");

        add_one_finished();
        check_and_run_teardown();
    }

    #[test]
    fn select_test_order_by_multiple_columns() {
        setup();
        get_query_result("INSERT INTO test.sel (id, name, age, height) VALUES (7, 'Mati', 22, 1)")
            .unwrap();
        get_query_result("INSERT INTO test.sel (id, name, age, height) VALUES (7, 'Ivan', 33, 1)")
            .unwrap();
        get_query_result(
            "INSERT INTO test.sel (id, name, age, height) VALUES (7, 'Thiago', 33, 2)",
        )
        .unwrap();

        let result =
            get_query_result("select * from test.sel WHERE id = 7 ORDER BY height ASC, age DESC");
        assert!(result.is_ok());
        let rows = get_rows_select(result.unwrap());
        assert_eq!(rows.len(), 3);

        assert_eq!(rows[0].get_row_hash().get("name").unwrap().value, "Ivan");
        assert_eq!(rows[1].get_row_hash().get("name").unwrap().value, "Mati");
        assert_eq!(rows[2].get_row_hash().get("name").unwrap().value, "Thiago");

        add_one_finished();
        check_and_run_teardown();
    }

    #[test]
    fn select_test_where_invalid_type_comparison() {
        setup();
        get_query_result("INSERT INTO test.sel (id, name, age) VALUES (10, 'Thiago', 33)").unwrap();

        let result = get_query_result("select * from test.sel WHERE age = 'thirty-three'");
        assert!(result.is_err());

        add_one_finished();
        check_and_run_teardown();
    }

    #[test]
    fn select_test_where_nonexistent_column() {
        setup();
        get_query_result("INSERT INTO test.sel (id, name, age) VALUES (11, 'Ivan', 33)").unwrap();

        let result =
            get_query_result("select * from test.sel WHERE id = 11 AND non_existent_column = 10");
        assert!(result.is_err());

        add_one_finished();
        check_and_run_teardown();
    }

    #[test]
    fn select_test_error_nonexistent_columns() {
        setup();
        get_query_result("INSERT INTO test.sel (id, name, age) VALUES (12, 'Mati', 33)").unwrap();

        let result = get_query_result(
            "select non_existent_column, another_nonexistent_column from test.sel",
        );
        assert!(result.is_err());

        add_one_finished();
        check_and_run_teardown();
    }

    #[test]
    fn select_test_no_rows_after_filter() {
        setup();
        get_query_result("INSERT INTO test.sel (id, name, age) VALUES (13, 'Mati', 33)").unwrap();

        let result = get_query_result("select * from test.sel WHERE id = 13 AND age > 100");
        assert!(result.is_ok());
        let rows = get_rows_select(result.unwrap());
        assert_eq!(rows.len(), 0);

        add_one_finished();
        check_and_run_teardown();
    }

    #[test]
    fn select_test_order_by_nonexistent_column() {
        setup();
        get_query_result("INSERT INTO test.sel (id, name, age) VALUES (14, 'Mati', 33)").unwrap();

        let result = get_query_result(
            "select * from test.sel WHERE id = 14 ORDER BY non_existent_column ASC",
        );
        assert!(result.is_err());

        add_one_finished();
        check_and_run_teardown();
    }

    #[test]
    fn select_test_error_no_partition_in_where() {
        setup();
        get_query_result("INSERT INTO test.sel (id, name, age) VALUES (14, 'Mati', 33)").unwrap();

        let result = get_query_result("select * from test.sel WHERE age = 33");
        assert!(result.is_err());

        add_one_finished();
        check_and_run_teardown();
    }

    #[test]
    fn select_test_error_no_where() {
        setup();
        get_query_result("INSERT INTO test.sel (id, name, age) VALUES (14, 'Mati', 33)").unwrap();

        let result = get_query_result("select * from test.sel");
        assert!(result.is_err());

        add_one_finished();
        check_and_run_teardown();
    }
}
