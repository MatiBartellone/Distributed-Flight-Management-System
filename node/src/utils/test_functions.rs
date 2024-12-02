use crate::data_access::data_access_handler::DataAccessHandler;
use crate::data_access::row::Row;
use crate::meta_data::meta_data_handler::MetaDataHandler;
use crate::parsers::query_parser::{query_lexer, query_parser};
use crate::utils::errors::Errors;
use crate::utils::functions::deserialize_from_str;
use crate::utils::types::bytes_cursor::BytesCursor;
use crate::utils::types::node_ip::NodeIp;
use std::fs::File;
use std::io::Write;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Once;
use std::thread;
use std::thread::sleep;
use std::time::Duration;

static INIT: Once = Once::new();
static FINISHED: AtomicUsize = AtomicUsize::new(0);
const INTEGRATION_TESTS_QUANTITY: usize = 66;

pub fn add_one_finished() {
    FINISHED.fetch_add(1, Ordering::SeqCst);
}

// Chequear si esta es la última prueba y ejecutar el "teardown" global.
pub fn check_and_run_teardown() {
    // Si todas las pruebas se completaron, ejecutamos el teardown
    let finished = FINISHED.load(Ordering::SeqCst);
    if finished >= INTEGRATION_TESTS_QUANTITY {
        teardown();
    }
}

pub fn setup() {
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

        if get_query_result("CREATE KEYSPACE test WITH replication = {'replication_factor' : 1}").is_ok() {
            get_query_result("CREATE TABLE test.tb1 (id int, name text, second text, PRIMARY KEY(id, name))").unwrap();
            get_query_result("CREATE TABLE test.del (id int, name text, second text, PRIMARY KEY(id, name))").unwrap();
            get_query_result("CREATE TABLE test.upd (id int, name text, age int, height int, PRIMARY KEY(id, name))").unwrap();
            get_query_result("CREATE TABLE test.sel (id int, name text, age int, height int, PRIMARY KEY(id, name))").unwrap();
            get_query_result("CREATE TABLE test.que (id int, name text, age int, height int, PRIMARY KEY(id))").unwrap();
        }

    });
}

pub fn teardown() {
    let query = "DROP KEYSPACE test".to_string();
    let tokens = query_lexer(query).unwrap();
    let query = query_parser(tokens).unwrap();
    query.run().unwrap();
}

pub fn store_ip(ip: &NodeIp) -> Result<(), Errors> {
    let mut file = File::create("src/utils/ip.txt").expect("Error creating file");
    file.write_all(ip.get_string_ip().as_bytes())
        .expect("Error writing to file");
    Ok(())
}

pub fn get_query_result(query: &str) -> Result<Vec<u8>, Errors> {
    let query = query.to_string();
    let tokens = query_lexer(query)?;
    let query = query_parser(tokens)?;
    query.run()
}

pub fn get_rows_select(result: Vec<u8>) -> Vec<Row> {
    let mut cursor = BytesCursor::new(result.as_slice());
    assert_eq!(cursor.read_int().unwrap(), 2);
    let rows_len = cursor.read_short().unwrap();
    let mut rows = Vec::new();
    for _ in 0..rows_len {
        rows.push(deserialize_from_str(&cursor.read_string().unwrap()).unwrap());
    }
    rows
}
