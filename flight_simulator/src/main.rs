use std::
    io::{self, Write}
;

use flight_simulator::cassandra_comunication::{cassandra_client::CassandraClient, flight_simulator_client::FlightSimulatorClient, thread_pool_client::ThreadPoolClient};

fn main() {
    let clients = match inicializate_clients() {
        Ok(clients) => clients,
        Err(e) => {
            println!("{}", e);
            return;
        }
    };
    let airport_code = get_input("Enter the airport code:");
    let thread_pool = ThreadPoolClient::new(clients);
    let simulator = FlightSimulatorClient;
    simulator.restart_flights(&airport_code, &thread_pool);
    simulator.flight_updates_loop(&airport_code, 10.0, 1000, &thread_pool);
}

fn inicializate_clients() -> Result<Vec<CassandraClient>, String> {
    let cant_clients = get_input("Enter the number of clients: ").parse::<usize>().unwrap();
    
    let simulator = FlightSimulatorClient;
    let mut clients = Vec::new();
    for _ in 0..cant_clients {
        let node = get_input("FULL IP (ip:port): ");
        let client = CassandraClient::new(&node)?;
        client.inicializate()?;
        simulator.use_aviation_keyspace(&client)?;
        clients.push(client);
    }
    Ok(clients)
}

// Gets the user input with a message
fn get_input(message: &str) -> String {
    println!("{}", message);
    io::stdout().flush().unwrap();
    let mut node = String::new();
    io::stdin().read_line(&mut node).expect("Error reading");
    node.trim().to_string()
}