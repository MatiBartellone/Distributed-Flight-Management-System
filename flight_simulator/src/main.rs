use flight_simulator::{cassandra_comunication::{cassandra_client::CassandraClient, flight_simulator_client::FlightSimulatorClient, thread_pool_client::ThreadPoolClient}, utils::system_functions::get_user_data};

fn main() {
    let clients = match inicializate_clients() {
        Ok(clients) => clients,
        Err(e) => {
            println!("{}", e);
            return;
        }
    };
    let airport_code = get_user_data("Enter the airport code:");
    let thread_pool = ThreadPoolClient::new(clients);
    let simulator = FlightSimulatorClient;
    simulator.restart_flights(&airport_code, &thread_pool);
    simulator.flight_updates_loop(&airport_code, 10.0, 1000, &thread_pool);
}

fn inicializate_clients() -> Result<Vec<CassandraClient>, String> {
    let cant_clients = get_user_data("Enter the number of clients: ").parse::<usize>()
        .or_else(|_| Err("Error parsing the number of clients".to_string()))?;
    
    let simulator = FlightSimulatorClient;
    let mut clients = Vec::new();
    for _ in 0..cant_clients {
        let node = get_user_data("FULL IP (ip:port): ");
        let client = CassandraClient::new(&node)?;
        client.inicializate()?;
        simulator.use_aviation_keyspace(&client)?;
        clients.push(client);
    }
    Ok(clients)
}