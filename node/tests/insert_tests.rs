
use node::parsers::query_parser::{query_lexer, query_parser};
use std::sync::atomic::{AtomicUsize, Ordering};
use node::utils::test_functions::{check_and_run_teardown, get_query_result, get_rows_select, setup, GlobalState};

#[test]
fn insert_test() {
    let global_state = GlobalState::new();
    setup(&global_state); // Pasamos el estado a la configuración
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
    check_and_run_teardown();
}

#[test]
fn insert_test2() {
    let global_state = GlobalState::new();
    setup(&global_state); // Pasamos el estado a la configuración
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
    check_and_run_teardown();
}

#[test]
fn insert_test_pk_twice_changes_data() {
    let global_state = GlobalState::new();
    setup(&global_state); // Pasamos el estado a la configuración
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
    check_and_run_teardown();
}

#[test]
fn insert_test_multiple_rows() {
    let global_state = GlobalState::new();
    setup(&global_state); // Pasamos el estado a la configuración
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
    check_and_run_teardown();
}

#[test]
fn insert_test_multiple_rows_same_partition() {
    let global_state = GlobalState::new();
    setup(&global_state); // Pasamos el estado a la configuración
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
    check_and_run_teardown();
}

#[test]
fn insert_test_only_one_value() {
    let global_state = GlobalState::new();
    setup(&global_state); // Pasamos el estado a la configuración
    let result = get_query_result("INSERT INTO test.tb1 (id, name) VALUES (4, 'Emi')");
    assert!(result.is_ok());
    check_and_run_teardown();
}

#[test]
fn insert_test_insert_pk_twice() {
    let global_state = GlobalState::new();
    setup(&global_state); // Pasamos el estado a la configuración
    let result = get_query_result("INSERT INTO test.tb1 (id, name) VALUES (5, 'Ivan')");
    assert!(result.is_ok());
    let result = get_query_result("INSERT INTO test.tb1 (id, name) VALUES (5, 'Ivan')");
    assert!(result.is_ok());
    check_and_run_teardown();
}

#[test]
fn insert_test_error_column_nonexistant() {
    let global_state = GlobalState::new();
    setup(&global_state); // Pasamos el estado a la configuración
    let result = get_query_result("INSERT INTO test.tb1 (id, last_name) VALUES (2, 'Mati')");
    assert!(result.is_err());
    check_and_run_teardown();
}

#[test]
fn insert_test_error_no_pk() {
    let global_state = GlobalState::new();
    setup(&global_state); // Pasamos el estado a la configuración
    let result = get_query_result("INSERT INTO test.tb1 (name) VALUES ('Ivan')");
    assert!(result.is_err());
    check_and_run_teardown();
}

#[test]
fn insert_test_error_more_values_than_headers() {
    let global_state = GlobalState::new();
    setup(&global_state); // Pasamos el estado a la configuración
    let result = get_query_result("INSERT INTO test.tb1 (id) VALUES (2, 'Mati')");
    assert!(result.is_err());
    check_and_run_teardown();
}

#[test]
fn insert_test_error_mismatched_types() {
    let global_state = GlobalState::new();
    setup(&global_state); // Pasamos el estado a la configuración
    let result = get_query_result("INSERT INTO test.tb1 (id, name) VALUES ('Mati', 2)");
    assert!(result.is_err());
    check_and_run_teardown();
}
