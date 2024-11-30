use std::fs::File;
use std::io::Write;
use std::thread;
use node::data_access::data_access_handler::DataAccessHandler;
use node::meta_data::meta_data_handler::MetaDataHandler;
use node::parsers::query_parser::{query_lexer, query_parser};
use node::utils::errors::Errors;
use node::utils::types::node_ip::NodeIp;
use std::sync::{Once, Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread::sleep;
use std::time::Duration;
use node::data_access::row::Row;
use node::utils::functions::deserialize_from_str;
use node::utils::types::bytes_cursor::BytesCursor;

static INIT: Once = Once::new();
static FINISHED: AtomicUsize = AtomicUsize::new(0);

#[derive(Clone)]
struct GlobalState {
    data: Arc<Mutex<i32>>,
}

impl GlobalState {
    fn new() -> Self {
        GlobalState {
            data: Arc::new(Mutex::new(42)),
        }
    }
}

fn setup(state: &GlobalState) {
    INIT.call_once(|| {
        let ip = NodeIp::new_from_single_string("127.0.0.1:9090").unwrap();
        store_ip(&ip).unwrap();
        let data_access_ip = ip.clone();
        let metadata_ip = ip.clone();
        thread::spawn(move || {
            DataAccessHandler::start_listening(data_access_ip).unwrap();
        });
        thread::spawn(move || {
            MetaDataHandler::start_listening(metadata_ip).unwrap();
        });
        sleep(Duration::from_secs(1));
        run_query("CREATE KEYSPACE test WITH replication = {'replication_factor' : 1}");
        run_query("CREATE TABLE test.tb1 (id int, name text, second text, PRIMARY KEY(id, name))");

        // Aquí puedes usar el estado global de forma segura
        let mut data = state.data.lock().unwrap();
        *data = 42; // Puedes actualizar el estado global de forma segura
    });
}

fn teardown() {
    let query = "DROP KEYSPACE test".to_string();
    let tokens = query_lexer(query).unwrap();
    let query = query_parser(tokens).unwrap();
    query.run().unwrap();
}

// Chequear si esta es la última prueba y ejecutar el "teardown" global.
fn check_and_run_teardown() {
    // Si todas las pruebas se completaron, ejecutamos el teardown
    if FINISHED.load(Ordering::SeqCst) == 11 { // Ajusta este número según el número total de pruebas
        teardown();
    }
}

#[test]
fn insert_test() {
    let global_state = GlobalState::new();
    setup(&global_state);  // Pasamos el estado a la configuración
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
    FINISHED.fetch_add(1, Ordering::SeqCst);
    check_and_run_teardown();
}

#[test]
fn insert_test2() {
    let global_state = GlobalState::new();
    setup(&global_state);  // Pasamos el estado a la configuración
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
    FINISHED.fetch_add(1, Ordering::SeqCst);
    check_and_run_teardown();
}

#[test]
fn insert_test_pk_twice_changes_data() {
    let global_state = GlobalState::new();
    setup(&global_state);  // Pasamos el estado a la configuración
    let result = get_query_result("INSERT INTO test.tb1 (id, name, second) VALUES (3, 'Thiago', 'Pacheco')");
    assert!(result.is_ok());
    let result = get_query_result("INSERT INTO test.tb1 (id, name, second) VALUES (3, 'Thiago', 'Bartellone')");
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
    FINISHED.fetch_add(1, Ordering::SeqCst);
    check_and_run_teardown();
}

#[test]
fn insert_test_multiple_rows() {
    let global_state = GlobalState::new();
    setup(&global_state);  // Pasamos el estado a la configuración
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
    FINISHED.fetch_add(1, Ordering::SeqCst);
    check_and_run_teardown();
}

#[test]
fn insert_test_multiple_rows_same_partition() {
    let global_state = GlobalState::new();
    setup(&global_state);  // Pasamos el estado a la configuración
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
    FINISHED.fetch_add(1, Ordering::SeqCst);
    check_and_run_teardown();
}

#[test]
fn insert_test_only_one_value() {
    let global_state = GlobalState::new();
    setup(&global_state);  // Pasamos el estado a la configuración
    let result = get_query_result("INSERT INTO test.tb1 (id, name) VALUES (4, 'Emi')");
    assert!(result.is_ok());
    FINISHED.fetch_add(1, Ordering::SeqCst);
    check_and_run_teardown();
}

#[test]
fn insert_test_insert_pk_twice() {
    let global_state = GlobalState::new();
    setup(&global_state);  // Pasamos el estado a la configuración
    let result = get_query_result("INSERT INTO test.tb1 (id, name) VALUES (5, 'Ivan')");
    assert!(result.is_ok());
    let result = get_query_result("INSERT INTO test.tb1 (id, name) VALUES (5, 'Ivan')");
    assert!(result.is_ok());
    FINISHED.fetch_add(1, Ordering::SeqCst);
    check_and_run_teardown();
}

#[test]
fn insert_test_error_column_nonexistant() {
    let global_state = GlobalState::new();
    setup(&global_state);  // Pasamos el estado a la configuración
    let result = get_query_result("INSERT INTO test.tb1 (id, last_name) VALUES (2, 'Mati')");
    assert!(result.is_err());
    FINISHED.fetch_add(1, Ordering::SeqCst);
    check_and_run_teardown();
}

#[test]
fn insert_test_error_no_pk() {
    let global_state = GlobalState::new();
    setup(&global_state);  // Pasamos el estado a la configuración
    let result = get_query_result("INSERT INTO test.tb1 (name) VALUES ('Ivan')");
    assert!(result.is_err());
    FINISHED.fetch_add(1, Ordering::SeqCst);
    check_and_run_teardown();
}

#[test]
fn insert_test_error_more_values_than_headers() {
    let global_state = GlobalState::new();
    setup(&global_state);  // Pasamos el estado a la configuración
    let result = get_query_result("INSERT INTO test.tb1 (id) VALUES (2, 'Mati')");
    assert!(result.is_err());
    FINISHED.fetch_add(1, Ordering::SeqCst);
    check_and_run_teardown();
}

#[test]
fn insert_test_error_mismatched_types() {
    let global_state = GlobalState::new();
    setup(&global_state);  // Pasamos el estado a la configuración
    let result = get_query_result("INSERT INTO test.tb1 (id, name) VALUES ('Mati', 2)");
    assert!(result.is_err());
    FINISHED.fetch_add(1, Ordering::SeqCst);
    check_and_run_teardown();
}

fn store_ip(ip: &NodeIp) -> Result<(), Errors> {
    let mut file = File::create("src/utils/ip.txt").expect("Error creating file");
    file.write_all(ip.get_string_ip().as_bytes())
        .expect("Error writing to file");
    Ok(())
}

fn get_query_result(query: &str) -> Result<Vec<u8>, Errors> {
    let query = query.to_string();
    let tokens = query_lexer(query).unwrap();
    let query = query_parser(tokens).unwrap();
    query.run()
}

fn run_query(query: &str) {
    let query = query.to_string();
    let tokens = query_lexer(query).unwrap();
    let query = query_parser(tokens).unwrap();
    query.run().unwrap();
}

fn get_rows_select(result: Vec<u8>) -> Vec<Row> {
    let mut cursor = BytesCursor::new(result.as_slice());
    assert_eq!(cursor.read_int().unwrap(), 2);
    let rows_len = cursor.read_short().unwrap();
    let mut rows = Vec::new();
    for _ in 0..rows_len{
        rows.push(deserialize_from_str(&cursor.read_string().unwrap()).unwrap());
    }
    rows
}