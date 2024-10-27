use std::{io, thread, time::Duration};

use flight_simulator::cassandra_client::CassandraClient;

fn main() {
    let mut cassandra_client = match CassandraClient::new("127.0.0.1", 4000){
        Ok(cliente) => cliente,
        Err(_) => {
            println!("Error conectando al servidor");
            return;
        }
    };
    cassandra_client.inicializate();


    println!("\nIngrese el código del aeropuerto:");
    let mut airport_code = String::new();
    let _ = io::stdin().read_line(&mut airport_code);
    let airport_code = airport_code.trim();

    // Reinicia todos los vuelos de ese aeropuerto
    let flights = cassandra_client.get_flights(airport_code);
    for mut flight in flights {
        flight.restart();
        cassandra_client.update_flight(flight);
    }

    // Avanza los vuelos a la velocidad que le pasen
    loop {
        let flights = cassandra_client.get_flights(airport_code);
        for mut flight in flights{
            flight.update_progress(10.0);
            cassandra_client.update_flight(flight);
        }
        thread::sleep(Duration::from_millis(1000));
    }
}
