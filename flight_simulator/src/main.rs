use std::{io::{self, Write}, thread, time::Duration};

use flight_simulator::cassandra_client::CassandraClient;

fn main() {
    let node = get_input("FULL IP (ip:port): ");
    let mut cassandra = match inicializate_cassandra(&node) {
        Ok(cliente) => cliente,
        Err(e) => {
            println!("{}", e);
            return;
        }
    };
    let airport_code = get_input("Enter the airport code:");
    restart_flights(&mut cassandra, &airport_code);
    flight_updates_loop(&mut cassandra, &airport_code, 10.0, 1000);
}

fn inicializate_cassandra(node: &str) -> Result<CassandraClient, String> {
    let mut cassandra_client = CassandraClient::new(node)?;
    cassandra_client.inicializate()?;
    cassandra_client.use_aviation_keyspace()?;
    Ok(cassandra_client)
}

// Gets the user input with a message
fn get_input(message: &str) -> String {
    println!("{}", message);
    io::stdout().flush().unwrap();
    let mut node = String::new();
    io::stdin()
        .read_line(&mut node)
        .expect("Error reading");
    node.trim().to_string()
}

// Restarts all the flights in the airport
fn restart_flights(cassandra_client: &mut CassandraClient, airport_code: &str) {
    let flights = cassandra_client.get_flights(airport_code);
    for mut flight in flights {
        flight.restart();
        _ = cassandra_client.update_flight(flight);
    }
}

// Update the progress of the flights
fn flight_updates_loop(cassandra_client: &mut CassandraClient, airport_code: &str, step: f32, interval: u64) {
    loop {
        let flights = cassandra_client.get_flights(airport_code);
        for mut flight in flights {
            flight.update_progress(step);
            _ = cassandra_client.update_flight(flight);
        }
        thread::sleep(Duration::from_millis(interval));
    }
}