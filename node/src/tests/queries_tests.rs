#[cfg(test)]
mod tests {
    use crate::meta_data::meta_data_handler::{use_client_meta_data, use_keyspace_meta_data};
    use crate::parsers::query_parser::{query_lexer, query_parser};
    use crate::utils::constants::CLIENT_METADATA_PATH;
    use crate::utils::test_functions::{add_one_finished, check_and_run_teardown, get_query_result, setup};
    use crate::utils::types::bytes_cursor::BytesCursor;

    #[test]
    fn use_valid_keyspace() {
        setup();
        use_client_meta_data(|handler| handler.add_new_client(CLIENT_METADATA_PATH.to_string())).unwrap();
        let result = get_query_result("USE test");
        dbg!(&result);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), vec![0, 0, 0, 3, 0, 4, 116, 101, 115, 116]);
        add_one_finished();
        check_and_run_teardown();
    }

    #[test]
    fn use_empty_keyspace() {
        setup();
        let result = get_query_result("USE ");
        assert!(result.is_err());
        add_one_finished();
        check_and_run_teardown();
    }

    #[test]
    fn use_valid_keyspace_with_query() {
        setup();
        use_client_meta_data(|handler| handler.add_new_client(CLIENT_METADATA_PATH.to_string())).unwrap();
        let result = get_query_result("USE test");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), vec![0, 0, 0, 3, 0, 4, 116, 101, 115, 116]); //3 4 test
        let query = "INSERT INTO que (id) VALUES (1)";
        let query = query.to_string();
        let tokens = query_lexer(query).unwrap();
        let mut query = query_parser(tokens).unwrap();
        query.set_table().unwrap();
        assert!(query.run().is_ok());
        add_one_finished();
        check_and_run_teardown();
    }

    #[test]
    fn create_valid_keyspace() {
        setup();
        let result = get_query_result("CREATE KEYSPACE valid WITH replication = {'replication_factor' : 1}");
        assert!(result.is_ok());

        let mut cursor = BytesCursor::new(result.unwrap().as_slice());
        assert_eq!(cursor.read_int().unwrap(), 5);
        assert_eq!(cursor.read_string().unwrap(), "CREATED");
        assert_eq!(cursor.read_string().unwrap(), "KEYSPACE");
        assert_eq!(cursor.read_string().unwrap(), "valid");
        assert!(use_keyspace_meta_data(|handler| handler.exists_keyspace("valid")).unwrap());

        let result = get_query_result("DROP KEYSPACE valid");
        assert!(result.is_ok());
        add_one_finished();
        check_and_run_teardown();
    }

    #[test]
    fn create_existing_keyspace() {
        setup();

        let result = get_query_result("CREATE KEYSPACE valid2 WITH replication = {'replication_factor' : 1}");
        assert!(result.is_ok());

        let result = get_query_result("CREATE KEYSPACE valid2 WITH replication = {'replication_factor' : 1}");
        assert!(result.is_err());

        let result = get_query_result("DROP KEYSPACE valid2");
        assert!(result.is_ok());
        add_one_finished();
        check_and_run_teardown();
    }

    #[test]
    fn create_invalid_keyspace_name() {
        setup();
        let result = get_query_result("CREATE KEYSPACE invalid");
        assert!(result.is_err());

        add_one_finished();
        check_and_run_teardown();
    }

    #[test]
    fn create_empty_keyspace_name() {
        setup();
        let result = get_query_result("CREATE KEYSPACE ");
        assert!(result.is_err());
        add_one_finished();
        check_and_run_teardown();
    }

    #[test]
    fn create_valid_table() {
        setup();

        // Crear la tabla
        let result = get_query_result("CREATE TABLE test.test1 (id int, name text, age int, PRIMARY KEY (id))");
        assert!(result.is_ok());

        let mut cursor = BytesCursor::new(result.unwrap().as_slice());
        assert_eq!(cursor.read_int().unwrap(), 5);
        assert_eq!(cursor.read_string().unwrap(), "CREATED");
        assert_eq!(cursor.read_string().unwrap(), "TABLE");
        assert_eq!(cursor.read_string().unwrap(), "test.test1");

        let result = get_query_result("INSERT INTO test.test1 (id) VALUES (1)");
        assert!(result.is_ok());


        add_one_finished();
        check_and_run_teardown();
    }

    #[test]
    fn create_existing_table() {
        setup();

        let result = get_query_result("CREATE TABLE test.test2 (id int, value text, PRIMARY KEY (id))");
        assert!(result.is_ok());

        let result = get_query_result("CREATE TABLE test.test2 (id int, value text, PRIMARY KEY (id))");
        assert!(result.is_err());

        let result = get_query_result("INSERT INTO test.test2 (id) VALUES (1)");
        assert!(result.is_ok());

        add_one_finished();
        check_and_run_teardown();
    }

    #[test]
    fn create_table_without_primary_key() {
        setup();

        let result = get_query_result("CREATE TABLE test.test3 (id int, name text, age INT)");
        assert!(result.is_err());

        add_one_finished();
        check_and_run_teardown();
    }

    #[test]
    fn create_table_duplicate_columns() {
        setup();

        let result = get_query_result("CREATE TABLE test.test4 (id int, id text, PRIMARY KEY (id))");
        assert!(result.is_err());

        add_one_finished();
        check_and_run_teardown();
    }

    #[test]
    fn drop_existing_keyspace() {
        setup();

        let create_result = get_query_result("CREATE KEYSPACE test_drop_keyspace WITH replication = {'replication_factor' : 1}");
        assert!(create_result.is_ok());

        let result = get_query_result("DROP KEYSPACE test_drop_keyspace");
        assert!(result.is_ok());

        let mut cursor = BytesCursor::new(result.unwrap().as_slice());
        assert_eq!(cursor.read_int().unwrap(), 5);
        assert_eq!(cursor.read_string().unwrap(), "DROPPED");
        assert_eq!(cursor.read_string().unwrap(), "KEYSPACE");
        assert_eq!(cursor.read_string().unwrap(), "test_drop_keyspace");

        assert!(!use_keyspace_meta_data(|handler| handler.exists_keyspace("valid")).unwrap());

        add_one_finished();
        check_and_run_teardown();
    }

    #[test]
    fn drop_non_existing_keyspace() {
        setup();

        let result = get_query_result("DROP KEYSPACE nonexistent_keyspace");
        assert!(result.is_err());

        add_one_finished();
        check_and_run_teardown();
    }

    #[test]
    fn drop_existing_table() {
        setup();

        let create_result = get_query_result("CREATE TABLE test.drop_table (id int, name text, age int, PRIMARY KEY (id))");
        assert!(create_result.is_ok());

        let result = get_query_result("DROP TABLE test.drop_table");
        assert!(result.is_ok());

        let mut cursor = BytesCursor::new(result.unwrap().as_slice());
        assert_eq!(cursor.read_int().unwrap(), 5);
        assert_eq!(cursor.read_string().unwrap(), "DROPPED");
        assert_eq!(cursor.read_string().unwrap(), "TABLE");
        assert_eq!(cursor.read_string().unwrap(), "test.drop_table");

        let result = get_query_result("INSERT INTO test.drop_table (id) VALUES (1)");
        assert!(result.is_err());

        add_one_finished();
        check_and_run_teardown();
    }

    #[test]
    fn drop_non_existing_table() {
        setup();

        let result = get_query_result("DROP TABLE nonexistent_table");
        assert!(result.is_err());

        add_one_finished();
        check_and_run_teardown();
    }


}