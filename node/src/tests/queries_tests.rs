#[cfg(test)]
mod tests {
    use crate::meta_data::meta_data_handler::use_client_meta_data;
    use crate::parsers::query_parser::{query_lexer, query_parser};
    use crate::utils::constants::CLIENT_METADATA_PATH;
    use crate::utils::test_functions::{add_one_finished, check_and_run_teardown, get_query_result, setup};

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
        assert_eq!(result.unwrap(), vec![0, 0, 0, 3, 0, 4, 116, 101, 115, 116]);
        let query = "INSERT INTO que (id) VALUES (1)";
        let query = query.to_string();
        let tokens = query_lexer(query).unwrap();
        let mut query = query_parser(tokens).unwrap();
        query.set_table().unwrap();
        assert!(query.run().is_ok());
        add_one_finished();
        check_and_run_teardown();
    }
}