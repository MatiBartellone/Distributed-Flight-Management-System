#[cfg(test)]
mod tests {
    use crate::parsers::tokens::literal::Literal;
    use crate::utils::test_functions::{
        add_one_finished, check_and_run_teardown, get_query_result, get_rows_select, setup,
    };
    use std::collections::HashMap;
    #[test]
    fn update_test_simple_one_value() {
        setup();
        get_query_result("INSERT INTO test.upd (id, name, age, height) VALUES (1, 'Mati', 1, 2)")
            .unwrap();
        let result = get_query_result("UPDATE test.upd SET age = 5 WHERE id = 1");
        assert!(result.is_ok());
        let row_hash = get_one_row_hash("SELECT * FROM test.upd WHERE id = 1");
        assert_eq!(row_hash.get("age").unwrap().value, "5");
        add_one_finished();
        check_and_run_teardown();
    }

    #[test]
    fn update_test_simple_two_values() {
        setup();
        get_query_result("INSERT INTO test.upd (id, name, age, height) VALUES (2, 'Mati', 1, 2)")
            .unwrap();
        let result = get_query_result("UPDATE test.upd SET age = 5, height = 7 WHERE id = 2");
        assert!(result.is_ok());
        let row_hash = get_one_row_hash("SELECT * FROM test.upd WHERE id = 2");
        assert_eq!(row_hash.get("age").unwrap().value, "5");
        assert_eq!(row_hash.get("height").unwrap().value, "7");
        add_one_finished();
        check_and_run_teardown();
    }

    #[test]
    fn update_test_two_updates_two_values() {
        setup();
        get_query_result("INSERT INTO test.upd (id, name, age, height) VALUES (3, 'Mati', 1, 2)")
            .unwrap();
        let result = get_query_result("UPDATE test.upd SET age = 5 WHERE id = 3");
        assert!(result.is_ok());
        let result = get_query_result("UPDATE test.upd SET height = 7 WHERE id = 3");
        assert!(result.is_ok());
        let row_hash = get_one_row_hash("SELECT * FROM test.upd WHERE id = 3");
        assert_eq!(row_hash.get("age").unwrap().value, "5");
        assert_eq!(row_hash.get("height").unwrap().value, "7");
        add_one_finished();
        check_and_run_teardown();
    }

    #[test]
    fn update_test_two_updates_one_value() {
        setup();
        get_query_result("INSERT INTO test.upd (id, name, age, height) VALUES (4, 'Mati', 1, 2)")
            .unwrap();
        let result = get_query_result("UPDATE test.upd SET age = 5 WHERE id = 4");
        assert!(result.is_ok());
        let result = get_query_result("UPDATE test.upd SET age = 7 WHERE id = 4");
        assert!(result.is_ok());
        let row_hash = get_one_row_hash("SELECT * FROM test.upd WHERE id = 4");
        assert_eq!(row_hash.get("age").unwrap().value, "7");
        add_one_finished();
        check_and_run_teardown();
    }

    #[test]
    fn update_test_column_one_value() {
        setup();
        get_query_result("INSERT INTO test.upd (id, name, age, height) VALUES (5, 'Mati', 17, 42)")
            .unwrap();
        let result = get_query_result("UPDATE test.upd SET age = height WHERE id = 5");
        assert!(result.is_ok());
        let row_hash = get_one_row_hash("SELECT * FROM test.upd WHERE id = 5");
        assert_eq!(row_hash.get("age").unwrap().value, "42");
        add_one_finished();
        check_and_run_teardown();
    }

    #[test]
    fn update_test_column_two_values() {
        setup();
        get_query_result("INSERT INTO test.upd (id, name, age, height) VALUES (6, 'Mati', 12, 14)")
            .unwrap();
        let result = get_query_result("UPDATE test.upd SET age = height, height = id WHERE id = 6");
        assert!(result.is_ok());
        let row_hash = get_one_row_hash("SELECT * FROM test.upd WHERE id = 6");
        assert_eq!(row_hash.get("age").unwrap().value, "14");
        assert_eq!(row_hash.get("height").unwrap().value, "6");
        add_one_finished();
        check_and_run_teardown();
    }

    #[test]
    fn update_test_column_two_updates_one_value() {
        setup();
        get_query_result("INSERT INTO test.upd (id, name, age, height) VALUES (7, 'Mati', 67, 12)")
            .unwrap();
        let result = get_query_result("UPDATE test.upd SET height = age WHERE id = 7");
        assert!(result.is_ok());
        let result = get_query_result("UPDATE test.upd SET height = id WHERE id = 7");
        assert!(result.is_ok());
        let row_hash = get_one_row_hash("SELECT * FROM test.upd WHERE id = 7");
        assert_eq!(row_hash.get("height").unwrap().value, "7");
        add_one_finished();
        check_and_run_teardown();
    }

    #[test]
    fn update_test_column_two_updates_two_value() {
        setup();
        get_query_result("INSERT INTO test.upd (id, name, age, height) VALUES (8, 'Mati', 67, 12)")
            .unwrap();
        let result = get_query_result("UPDATE test.upd SET height = id WHERE id = 8");
        assert!(result.is_ok());
        let result = get_query_result("UPDATE test.upd SET age = height WHERE id = 8");
        assert!(result.is_ok());
        let row_hash = get_one_row_hash("SELECT * FROM test.upd WHERE id = 8");
        assert_eq!(row_hash.get("age").unwrap().value, "8");
        assert_eq!(row_hash.get("height").unwrap().value, "8");
        add_one_finished();
        check_and_run_teardown();
    }

    #[test]
    fn update_test_arithmetic_one_value_1() {
        setup();
        get_query_result("INSERT INTO test.upd (id, name, age, height) VALUES (9, 'Mati', 43, 23)")
            .unwrap();
        let result = get_query_result("UPDATE test.upd SET age = height + 5 WHERE id = 9");
        assert!(result.is_ok());
        let row_hash = get_one_row_hash("SELECT * FROM test.upd WHERE id = 9");
        assert_eq!(row_hash.get("age").unwrap().value, "28");
        add_one_finished();
        check_and_run_teardown();
    }

    #[test]
    fn update_test_arithmetic_one_value_2() {
        setup();
        get_query_result(
            "INSERT INTO test.upd (id, name, age, height) VALUES (14, 'Mati', 43, 23)",
        )
        .unwrap();
        let result = get_query_result("UPDATE test.upd SET age = age + 7 WHERE id = 14");
        assert!(result.is_ok());
        let row_hash = get_one_row_hash("SELECT * FROM test.upd WHERE id = 14");
        assert_eq!(row_hash.get("age").unwrap().value, "50");
        add_one_finished();
        check_and_run_teardown();
    }

    #[test]
    fn update_test_arithmetic_one_value_sub() {
        setup();
        get_query_result(
            "INSERT INTO test.upd (id, name, age, height) VALUES (15, 'Mati', 43, 23)",
        )
        .unwrap();
        let result = get_query_result("UPDATE test.upd SET age = age - 7 WHERE id = 15");
        assert!(result.is_ok());
        let row_hash = get_one_row_hash("SELECT * FROM test.upd WHERE id = 15");
        assert_eq!(row_hash.get("age").unwrap().value, "36");
        add_one_finished();
        check_and_run_teardown();
    }

    #[test]
    fn update_test_arithmetic_one_value_mul() {
        setup();
        get_query_result(
            "INSERT INTO test.upd (id, name, age, height) VALUES (16, 'Mati', 43, 23)",
        )
        .unwrap();
        let result = get_query_result("UPDATE test.upd SET age = age * 10 WHERE id = 16");
        assert!(result.is_ok());
        let row_hash = get_one_row_hash("SELECT * FROM test.upd WHERE id = 16");
        assert_eq!(row_hash.get("age").unwrap().value, "430");
        add_one_finished();
        check_and_run_teardown();
    }

    #[test]
    fn update_test_arithmetic_one_value_div() {
        setup();
        get_query_result(
            "INSERT INTO test.upd (id, name, age, height) VALUES (17, 'Mati', 33, 23)",
        )
        .unwrap();
        let result = get_query_result("UPDATE test.upd SET age = age / 3 WHERE id = 17");
        assert!(result.is_ok());
        let row_hash = get_one_row_hash("SELECT * FROM test.upd WHERE id = 17");
        assert_eq!(row_hash.get("age").unwrap().value, "11");
        add_one_finished();
        check_and_run_teardown();
    }

    #[test]
    fn update_test_arithmetic_two_values() {
        setup();
        get_query_result("INSERT INTO test.upd (id, name, age, height) VALUES (10, 'Mati', 1, 2)")
            .unwrap();
        let result = get_query_result(
            "UPDATE test.upd SET age = id + 12, height = height + 56 WHERE id = 10",
        );
        assert!(result.is_ok());
        let row_hash = get_one_row_hash("SELECT * FROM test.upd WHERE id = 10");
        assert_eq!(row_hash.get("age").unwrap().value, "22");
        assert_eq!(row_hash.get("height").unwrap().value, "58");
        add_one_finished();
        check_and_run_teardown();
    }

    #[test]
    fn update_test_error_missmatched_type() {
        setup();
        get_query_result("INSERT INTO test.upd (id, name, age, height) VALUES (11, 'Mati', 1, 2)")
            .unwrap();
        let result = get_query_result("UPDATE test.upd SET age = 'Thiago' WHERE id = 11");
        assert!(result.is_err());
        add_one_finished();
        check_and_run_teardown();
    }

    #[test]
    fn update_test_error_nonexistant_column() {
        setup();
        get_query_result("INSERT INTO test.upd (id, name, age, height) VALUES (12, 'Mati', 1, 2)")
            .unwrap();
        let result = get_query_result("UPDATE test.upd SET second = 'Thiago' WHERE id = 12");
        assert!(result.is_err());
        add_one_finished();
        check_and_run_teardown();
    }

    #[test]
    fn update_test_simple_one_value_multiple_rows() {
        setup();
        get_query_result(
            "INSERT INTO test.upd (id, name, age, height) VALUES (13, 'Mati', 143, 32)",
        )
        .unwrap();
        get_query_result(
            "INSERT INTO test.upd (id, name, age, height) VALUES (13, 'Thiago', 232, 21)",
        )
        .unwrap();
        get_query_result(
            "INSERT INTO test.upd (id, name, age, height) VALUES (13, 'Ivan', 54, 123)",
        )
        .unwrap();
        let result = get_query_result("UPDATE test.upd SET age = 5 WHERE id = 13");
        assert!(result.is_ok());
        let select_result = get_query_result("SELECT * FROM test.upd WHERE id = 13");
        assert!(select_result.is_ok());
        let rows = get_rows_select(select_result.unwrap());
        assert_eq!(rows.len(), 3);
        let row_hash = rows.get(0).unwrap().get_row_hash();
        assert_eq!(row_hash.get("age").unwrap().value, "5");
        let row_hash = rows.get(1).unwrap().get_row_hash();
        assert_eq!(row_hash.get("age").unwrap().value, "5");
        let row_hash = rows.get(2).unwrap().get_row_hash();
        assert_eq!(row_hash.get("age").unwrap().value, "5");
        add_one_finished();
        check_and_run_teardown();
    }

    fn get_one_row_hash(select: &str) -> HashMap<String, Literal> {
        let select_result = get_query_result(select);
        assert!(select_result.is_ok());
        let rows = get_rows_select(select_result.unwrap());
        assert_eq!(rows.len(), 1);
        rows.get(0).unwrap().get_row_hash()
    }

    #[test]
    fn select_test_error_no_partition_in_where() {
        setup();
        get_query_result("INSERT INTO test.upd (id, name, age) VALUES (14, 'Mati', 33)").unwrap();

        let result = get_query_result("select * from test.upd WHERE age = 33");
        assert!(result.is_err());

        add_one_finished();
        check_and_run_teardown();
    }

    #[test]
    fn select_test_error_no_where() {
        setup();
        get_query_result("INSERT INTO test.upd (id, name, age) VALUES (15, 'Mati', 33)").unwrap();

        let result = get_query_result("select * from test.upd");
        assert!(result.is_err());

        add_one_finished();
        check_and_run_teardown();
    }
}
