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
use std::sync::{Once, Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::thread::sleep;
use std::time::Duration;

static INIT: Once = Once::new();
static FINISHED: AtomicUsize = AtomicUsize::new(0);
const INTEGRATION_TESTS_QUANTITY: usize = 16;

#[derive(Clone)]
pub struct GlobalState {
    data: Arc<Mutex<i32>>,
}

impl GlobalState {
    pub fn new() -> Self {
        GlobalState {
            data: Arc::new(Mutex::new(42)),
        }
    }
}

pub fn setup(state: &GlobalState) {
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
        run_query("CREATE TABLE test.del (id int, name text, second text, PRIMARY KEY(id, name))");

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
pub fn check_and_run_teardown() {
    FINISHED.fetch_add(1, Ordering::SeqCst);
    // Si todas las pruebas se completaron, ejecutamos el teardown
    if FINISHED.load(Ordering::SeqCst) == INTEGRATION_TESTS_QUANTITY {
        // Ajusta este número según el número total de pruebas
        teardown();
    }
}

pub fn store_ip(ip: &NodeIp) -> Result<(), Errors> {
    let mut file = File::create("src/utils/ip.txt").expect("Error creating file");
    file.write_all(ip.get_string_ip().as_bytes())
        .expect("Error writing to file");
    Ok(())
}

pub fn get_query_result(query: &str) -> Result<Vec<u8>, Errors> {
    let query = query.to_string();
    let tokens = query_lexer(query).unwrap();
    let query = query_parser(tokens).unwrap();
    query.run()
}

pub fn run_query(query: &str) {
    let query = query.to_string();
    let tokens = query_lexer(query).unwrap();
    let query = query_parser(tokens).unwrap();
    query.run();
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
