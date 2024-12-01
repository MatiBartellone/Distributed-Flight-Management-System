use std::thread;
use std::time::Duration;
use node::utils::test_functions::{add_one_finished, check_and_run_teardown, get_query_result, get_rows_select, setup, teardown};

#[test]
fn delete_test_one_simple_delete() {
    setup();
    get_query_result("INSERT INTO test.del (id, name) VALUES (1, 'Mati')").unwrap();
    let select_result = get_query_result("SELECT * FROM test.del WHERE id = 1");
    assert!(select_result.is_ok());
    let rows = get_rows_select(select_result.unwrap());
    assert_eq!(rows.len(), 1);
    let row = rows.get(0).unwrap();
    assert!(!row.is_deleted());
    let result = get_query_result("DELETE FROM test.del WHERE id = 1");
    assert!(result.is_ok());
    let select_result = get_query_result("SELECT * FROM test.del WHERE id = 1");
    assert!(select_result.is_ok());
    let rows = get_rows_select(select_result.unwrap());
    assert_eq!(rows.len(), 1);
    let row = rows.get(0).unwrap();
    assert!(row.is_deleted());
    add_one_finished();
    check_and_run_teardown();
}
#[test]
fn delete_test_two_simple_delete_erases_line() {
    setup();
    get_query_result("INSERT INTO test.del (id, name) VALUES (2, 'Mati')").unwrap();
    let result1 = get_query_result("DELETE FROM test.del WHERE id = 2");
    let result2 = get_query_result("DELETE FROM test.del WHERE id = 2");
    assert!(result1.is_ok());
    assert!(result2.is_ok());
    let select_result = get_query_result("SELECT * FROM test.del WHERE id = 2");
    let rows = get_rows_select(select_result.unwrap());
    assert_eq!(rows.len(), 0);
    add_one_finished();
    check_and_run_teardown();
}

#[test]
fn delete_test_one_simple_delete_multiple_lines() {
    setup();
    get_query_result("INSERT INTO test.del (id, name) VALUES (3, 'Mati')").unwrap();
    get_query_result("INSERT INTO test.del (id, name) VALUES (3, 'Thiago')").unwrap();
    get_query_result("INSERT INTO test.del (id, name) VALUES (3, 'Ivan')").unwrap();
    let select_result = get_query_result("SELECT * FROM test.del WHERE id = 3");
    let rows = get_rows_select(select_result.unwrap());
    assert!(!rows.get(0).unwrap().is_deleted());
    assert!(!rows.get(1).unwrap().is_deleted());
    assert!(!rows.get(2).unwrap().is_deleted());
    let result = get_query_result("DELETE FROM test.del WHERE id = 3");
    assert!(result.is_ok());
    let select_result = get_query_result("SELECT * FROM test.del WHERE id = 3");
    let rows = get_rows_select(select_result.unwrap());
    assert!(rows.get(0).unwrap().is_deleted());
    assert!(rows.get(1).unwrap().is_deleted());
    assert!(rows.get(2).unwrap().is_deleted());
    add_one_finished();
    check_and_run_teardown();
}

#[test]
fn delete_test_two_simple_delete_erases_multiple_lines() {
    setup();
    get_query_result("INSERT INTO test.del (id, name) VALUES (4, 'Mati')").unwrap();
    get_query_result("INSERT INTO test.del (id, name) VALUES (4, 'Thiago')").unwrap();
    get_query_result("INSERT INTO test.del (id, name) VALUES (4, 'Ivan')").unwrap();
    let result1 = get_query_result("DELETE FROM test.del WHERE id = 4");
    let result2 = get_query_result("DELETE FROM test.del WHERE id = 4");
    assert!(result1.is_ok());
    assert!(result2.is_ok());
    let select_result = get_query_result("SELECT * FROM test.del WHERE id = 4");
    let rows = get_rows_select(select_result.unwrap());
    assert_eq!(rows.len(), 0);
    add_one_finished();
    check_and_run_teardown();
}

#[test]
fn delete_test_inexistant_line() {
    setup();
    let result = get_query_result("DELETE FROM test.del WHERE id = 5");
    assert!(result.is_ok());
    let select_result = get_query_result("SELECT * FROM test.del WHERE id = 4");
    let rows = get_rows_select(select_result.unwrap());
    assert_eq!(rows.len(), 0);
    add_one_finished();
    check_and_run_teardown();
}