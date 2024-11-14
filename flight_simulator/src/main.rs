use std::{
    collections::HashSet, io::{self, Write}, sync::mpsc, thread, time::Duration
};

use flight_simulator::{cassandra_comunication::flight_simulator_client::FlightSimulatorClient, flight_implementation::flight::Flight, thread_pool::thread_pool::ThreadPool};

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
    let thread_pool = ThreadPool::new(8);
    restart_flights(&mut cassandra, &airport_code, &thread_pool);
    flight_updates_loop(&mut cassandra, &airport_code, 10.0, 1000, &thread_pool);
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

fn get_codes(
    simulator_client: &mut FlightSimulatorClient,
    thread_pool: &ThreadPool,
    airport_code: &str,
) -> HashSet<String> {
    let (tx, rx) = mpsc::channel();
    let airport_code = airport_code.to_string();
    thread_pool.execute(move |frame_id| {
        if let Some(flight_codes) = simulator_client.get_flight_codes_by_airport(&airport_code, &frame_id) {
            tx.send(flight_codes).expect("Error sending the flight codes");
        }
    });

    thread_pool.wait();
    rx.recv().unwrap()
}

fn get_flights(
    simulator_client: &mut FlightSimulatorClient,
    airport_code: &str,
    thread_pool: &ThreadPool,
) -> Vec<Flight> {
    let flight_codes = get_codes(simulator_client, thread_pool, airport_code);

    let (tx, rx) = mpsc::channel();
    for code in flight_codes {
        thread_pool.execute(move |frame_id| {
            if let Some(flight) = simulator_client.get_flight(&code, &frame_id) {
                tx.send(flight).expect("Error sending the flight");
            }
        });
    }

    thread_pool.wait();
    rx.into_iter().collect()
}

// Restarts all the flights in the airport
fn restart_flights(
    simulator_client: &mut FlightSimulatorClient,
    airport_code: &str,
    thread_pool: &ThreadPool,
) {
    let flights = get_flights(simulator_client, airport_code, thread_pool);

    for mut flight in flights {
        thread_pool.execute(move |frame_id| {
            flight.restart((0.0, 0.0));
            _ = simulator_client.update_flight(&flight, &frame_id);
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
    thread_pool: &ThreadPool,
) {
    let flights = get_flights(simulator_client, airport_code, thread_pool);
    loop {
        for mut flight in flights {
            thread_pool.execute(move |frame_id| {
                flight.update_progress(step);
                _ = simulator_client.update_flight(&flight, &frame_id);
            });
        }
        thread_pool.wait();
        thread::sleep(Duration::from_millis(interval));
    }
}
