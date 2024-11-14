use std::
    io::{self, Write}
;

use flight_simulator::{cassandra_comunication::flight_simulator_client::FlightSimulatorClient, thread_pool::thread_pool::ThreadPool};

fn main() {
    let node = get_input("FULL IP (ip:port): ");
    let client = match inicializate_simulator(&node) {
        Ok(cliente) => cliente,
        Err(e) => {
            println!("{}", e);
            return;
        }
    };
    let airport_code = get_input("Enter the airport code:");
    let thread_pool = ThreadPool::new(8);
    client.restart_flights(&airport_code, &thread_pool);
    client.flight_updates_loop(&airport_code, 10.0, 1000, &thread_pool);
}

fn inicializate_simulator(node: &str) -> Result<FlightSimulatorClient, String> {
    let simulator_client = FlightSimulatorClient::new(node)?;
    simulator_client.inicializate()?;
    simulator_client.use_aviation_keyspace()?;
    Ok(simulator_client)
}

// Gets the user input with a message
fn get_input(message: &str) -> String {
    println!("{}", message);
    io::stdout().flush().unwrap();
    let mut node = String::new();
    io::stdin().read_line(&mut node).expect("Error reading");
    node.trim().to_string()
}