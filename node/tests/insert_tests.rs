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

static TEST_COUNT: AtomicUsize = AtomicUsize::new(0);
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
        run_query("CREATE KEYSPACE test WITH replication = {'replication_factor' : 1}");
        run_query("CREATE TABLE test.tb1 (id int PRIMARY KEY, name text)");

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
    if FINISHED.load(Ordering::SeqCst) == 2 { // Ajusta este número según el número total de pruebas
        teardown();
    }
}

#[test]
fn insert_test() {
    let global_state = GlobalState::new();
    setup(&global_state);  // Pasamos el estado a la configuración
    TEST_COUNT.fetch_add(1, Ordering::SeqCst);  // Incrementamos el contador de pruebas
    let query = "INSERT INTO test.tb1 (id, name) VALUES (1, 'Mati')".to_string();
    let tokens = query_lexer(query).unwrap();
    let query = query_parser(tokens).unwrap();
    let result = query.run();
    if let Err(e) = result {
        panic!("{:?}", e);
    }
    assert!(result.is_ok());

    // Marcamos la prueba como completada
    FINISHED.fetch_add(1, Ordering::SeqCst);
    check_and_run_teardown();
}

#[test]
fn insert_test2() {
    let global_state = GlobalState::new();
    setup(&global_state);  // Pasamos el estado a la configuración
    TEST_COUNT.fetch_add(1, Ordering::SeqCst);  // Incrementamos el contador de pruebas
    let query = "INSERT INTO test.tb1 (id, name) VALUES (2, 'Mati')".to_string();
    let tokens = query_lexer(query).unwrap();
    let query = query_parser(tokens).unwrap();
    let result = query.run();
    if let Err(e) = result {
        panic!("{:?}", e);
    }
    assert!(result.is_ok());

    // Marcamos la prueba como completada
    FINISHED.fetch_add(1, Ordering::SeqCst);
    check_and_run_teardown();
}

fn store_ip(ip: &NodeIp) -> Result<(), Errors> {
    let mut file = File::create("src/utils/ip.txt").expect("Error creating file");
    file.write_all(ip.get_string_ip().as_bytes())
        .expect("Error writing to file");
    Ok(())
}

fn run_query(query: &str) {
    let query = query.to_string();
    let tokens = query_lexer(query).unwrap();
    let query = query_parser(tokens).unwrap();
    query.run().unwrap();
}
