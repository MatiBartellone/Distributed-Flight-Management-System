use std::{
    io::{self, Write},
    thread,
    time::Duration,
};

use flight_simulator::{cassandra_comunication::flight_simulator_client::FlightSimulatorClient, thread_pool};

fn main() {
    let node = get_input("FULL IP (ip:port): ");
    let mut cassandra = match inicializate_simulator(&node) {
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

fn inicializate_simulator(node: &str) -> Result<FlightSimulatorClient, String> {
    let mut simulator_client = FlightSimulatorClient::new(node)?;
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

// Restarts all the flights in the airport
fn restart_flights(simulator_client: &mut FlightSimulatorClient, airport_code: &str) {
    let flights = simulator_client.get_flights(airport_code);
    
    let thread_pool = ThreadPool::new(8);
    for mut flight in flights {
        thread_pool.execute(move |_| {
            // let position = simulator_client.get_airport_position(&flight.info.departure_airport);
            flight.restart((0.0, 0.0));
            _ = simulator_client.update_flight(&flight);
        });
    }
    thread_pool.wait();
}

// Update the progress of the flights
fn flight_updates_loop(
    simulator_client: &mut FlightSimulatorClient,
    airport_code: &str,
    step: f32,
    interval: u64,
) {
    let thread_pool = ThreadPool::new(8);
    loop {
        let flights = simulator_client.get_flights(airport_code);
        for mut flight in flights {
            thread_pool.execute(move |_| {
                flight.update_progress(position);
                _ = simulator_client.update_flight(&flight);
            });
        }
        thread_pool.wait();
        thread::sleep(Duration::from_millis(interval));
    }
}
