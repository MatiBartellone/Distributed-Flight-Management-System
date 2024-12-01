use std::collections::HashMap;

use simulator::{cassandra_comunication::{cassandra_client::CassandraClient, simulator::Simulator, thread_pool_client::ThreadPoolClient}, flight_implementation::{airport::Airport, flight::{Flight, FlightStatus, FlightTracking}, flight_state::FlightState}, utils::system_functions::{clear_screen, get_user_data}};

fn main() {
    let clients = match inicializate_clients() {
        Ok(clients) => clients,
        Err(e) => {
            println!("{}", e);
            return;
        }
    };
    let thread_pool = ThreadPoolClient::new(clients);
    loop_option(&thread_pool);
}

fn loop_option(thread_pool: &ThreadPoolClient) {
    let simulator = Simulator;
    let codes = get_airports_codes();
    let airports = simulator.get_airports(codes, &thread_pool);
    let mut flights = HashMap::new();
    loop {
        println!("Choose an option:");
        println!("1. Add flights for an airport");
        println!("2. Add a single flight");
        println!("3. Start updating loop");
        println!("4. Exit");
        let option = get_user_data("--> ");
        match option.as_str() {
            "1" => add_flights_for_airport(&mut flights, &simulator, thread_pool),
            "2" => add_single_flight(&mut flights, &simulator, thread_pool),
            "3" => break,
            "4" => return,
            _ => println!("Invalid option"),
        }
        clear_screen();
    }
    clear_screen();
    let mut flights: Vec<Flight> = flights.into_values().collect();
    simulator.restart_flights(&mut flights, &airports, &thread_pool);
    flight_updates_loop(&simulator, flights, &airports, thread_pool);
}

fn flight_updates_loop(simulator: &Simulator, flights: Vec<Flight>, airports: &HashMap<String, Airport>, thread_pool: &ThreadPoolClient) {
    let step = get_user_data("Enter the step time:")
        .parse::<f32>()
        .unwrap_or(10.0);
    let interval = get_user_data("Enter the interval time:")
        .parse::<u64>()
        .unwrap_or(1000);
    simulator.flight_updates_loop(flights, airports, step, interval, thread_pool);
}

fn add_flights_for_airport(flights: &mut HashMap<String, Flight>, simulator: &Simulator, thread_pool: &ThreadPoolClient) {
    let airport_code = get_user_data("Enter the airport code:");
    let flights_for_airport = simulator.get_flights(&airport_code, thread_pool);
    for flight in flights_for_airport {
        flights.insert(flight.get_code(), flight);
    }
}

fn add_single_flight(flights: &mut HashMap<String, Flight>, simulator: &Simulator, thread_pool: &ThreadPoolClient) {
    let flight = get_flight_data();
    simulator.insert_single_flight(&flight, thread_pool);
    flights.insert(flight.get_code(), flight);
}

fn get_flight_data() -> Flight {
        println!("Enter flight status details");
        let code = get_user_data("Enter flight code:");
        let departure_airport = get_user_data("Enter departure airport code:");
        let arrival_airport = get_user_data("Enter arrival airport code:");
        let departure_time = get_user_data("Enter departure time (e.g., YYYY-MM-DD HH:MM):");
        let arrival_time = get_user_data("Enter arrival time (e.g., YYYY-MM-DD HH:MM):");

        let status = FlightStatus {
            code,
            status: FlightState::OnTime,
            departure_airport,
            arrival_airport,
            departure_time,
            arrival_time,
        };
        Flight::new(FlightTracking::default(), status)
}

fn inicializate_clients() -> Result<Vec<CassandraClient>, String> {
    let cant_clients = get_user_data("Enter the number of clients: ").parse::<usize>()
        .or_else(|_| Err("Error parsing the number of clients".to_string()))?;
    
    let simulator = Simulator;
    let mut clients = Vec::new();
    for _ in 0..cant_clients {
        let node = get_user_data("FULL IP (ip:port): ");
        let mut  client = CassandraClient::new(&node)?;
        client.inicializate()?;
        simulator.use_aviation_keyspace(&mut client)?;
        clients.push(client);
    }
    Ok(clients)
}

// List of the airports codes to use in the app
fn get_airports_codes() -> Vec<String> {
    vec![
        "EZE".to_string(), // Aeropuerto Internacional Ministro Pistarini (Argentina)
        "JFK".to_string(), // John F. Kennedy International Airport (EE. UU.)
        "SCL".to_string(), // Aeropuerto Internacional Comodoro Arturo Merino Benítez (Chile)
        "MIA".to_string(), // Aeropuerto Internacional de Miami (EE. UU.)
        "DFW".to_string(), // Dallas/Fort Worth International Airport (EE. UU.)
        "GRU".to_string(), // Aeroporto Internacional de São Paulo/Guarulhos (Brasil)
        "MAD".to_string(), // Aeropuerto Adolfo Suárez Madrid-Barajas (España)
        "CDG".to_string(), // Aeropuerto Charles de Gaulle (Francia)
        "LAX".to_string(), // Los Angeles International Airport (EE. UU.)
        "AMS".to_string(), // Luchthaven Schiphol (Países Bajos)
        "NRT".to_string(), // Narita International Airport (Japón)
        "LHR".to_string(), // Aeropuerto de Heathrow (Reino Unido)
        "FRA".to_string(), // Aeropuerto de Frankfurt (Alemania)
        "SYD".to_string(), // Sydney Kingsford Smith Airport (Australia)
        "SFO".to_string(), // San Francisco International Airport (EE. UU.)
        "BOG".to_string(), // Aeropuerto Internacional El Dorado (Colombia)
        "MEX".to_string(), // Aeropuerto Internacional de la Ciudad de México (México)
        "YYC".to_string(), // Aeropuerto Internacional de Calgary (Canadá)
        "OSL".to_string(), // Aeropuerto de Oslo-Gardermoen (Noruega)
        "DEL".to_string(), // Aeropuerto Internacional Indira Gandhi (India)
        "PEK".to_string(), // Aeropuerto Internacional de Pekín-Capital (China)
        "SVO".to_string(), // Aeropuerto Internacional Sheremétievo (Rusia)
        "RUH".to_string(), // Aeropuerto Internacional Rey Khalid (Arabia Saudita)
        "CGK".to_string(), // Aeropuerto Internacional Soekarno-Hatta (Indonesia)
        "JNB".to_string(), // Aeropuerto Internacional O. R. Tambo (Sudáfrica)
        "BKO".to_string(), // Aeropuerto Internacional Modibo Keïta (Mali)
        "CAI".to_string(), // Aeropuerto Internacional de El Cairo (Egipto)
    ]
}