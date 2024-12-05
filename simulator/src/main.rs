use std::collections::{HashMap, HashSet};

use simulator::{cassandra_comunication::{cassandra_client::CassandraClient, simulator::Simulator, thread_pool_client::ThreadPoolClient}, flight_implementation::{airport::Airport, flight::{Flight, FlightStatus, FlightTracking}}, utils::system_functions::{clear_screen, get_user_data}};

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
    let airports = simulator.get_airports(codes, thread_pool);
    let mut airport_codes = HashSet::new();
    let mut flights_inserted = HashMap::new();
    clear_screen();
    loop {
        println!("Choose an option:");
        println!("1. Add flights for an airport");
        println!("2. Add a single flight");
        println!("3. Start updating loop");
        println!("4. Exit");
        let option = get_user_data("--> ");
        clear_screen();
        match option.as_str() {
            "1" => add_flights_for_airport(&airports, &mut airport_codes),
            "2" => add_single_flight(&mut flights_inserted, &airports, &simulator, thread_pool),
            "3" => {
                flight_updates_loop(&simulator, &mut flights_inserted, &airports, &mut airport_codes, thread_pool);
                airport_codes.clear();
                flights_inserted.clear();
            },
            "4" => return,
            _ => println!("Invalid option"),
        }
    }
}

// Minimize the number of airports for the flights
fn minimize_airports_for_flights(flight_no_in_selected_airports: HashMap<String, &Flight>) -> HashMap<String, Vec<String>> {
    let mut uncovered_flights = flight_no_in_selected_airports;
    let mut flight_codes_by_airport: HashMap<String, Vec<String>> = HashMap::new();

    while !uncovered_flights.is_empty() {
        // Agroup flights by airport
        let mut airport_coverage: HashMap<String, Vec<&Flight>> = HashMap::new();
        for (_ , flight) in &uncovered_flights {
            airport_coverage
                .entry(flight.get_departure_airport().to_string())
                .or_insert_with(Vec::new)
                .push(flight);
            airport_coverage
                .entry(flight.get_arrival_airport().to_string())
                .or_insert_with(Vec::new)
                .push(flight);
        }

        // Select the airport with the most flights
        let best_airport = airport_coverage
            .iter()
            .max_by_key(|(_, flights)| flights.len())
            .map(|(airport, _)| airport.clone())
            .unwrap();

        // Get the flights covered by the airport
        let covered_flights = airport_coverage.remove(&best_airport).unwrap();
        let covered_flight_codes: Vec<String> =
            covered_flights.iter().map(|flight| flight.get_code()).collect();
        flight_codes_by_airport.insert(best_airport.to_string(), covered_flight_codes);

        // Eliminar los vuelos cubiertos de la lista de vuelos sin cubrir
        uncovered_flights.retain(|_, flight| {
            flight.get_departure_airport() != &best_airport
                && flight.get_arrival_airport() != &best_airport
        });
    }
    flight_codes_by_airport
}

fn separate_flights_by_airport(flights_inserted: &mut HashMap<String, Flight>, airport_codes: &mut HashSet<String>) -> HashMap<String, Vec<String>> {
    let mut flight_no_in_selected_airports = HashMap::new();
    for flight in flights_inserted.values() {
        if airport_codes.get(flight.get_departure_airport()).is_some() {
            continue;
        }
        if airport_codes.get(flight.get_arrival_airport()).is_some() {
            continue;
        }
        flight_no_in_selected_airports.insert(flight.get_code(), flight);
    }
    minimize_airports_for_flights(flight_no_in_selected_airports)
}

fn flight_updates_loop(simulator: &Simulator, flights_inserted: &mut HashMap<String, Flight>, airports: &HashMap<String, Airport>, airport_codes: &mut HashSet<String>, thread_pool: &ThreadPoolClient) {
    let step = get_user_data("Enter the step time: ")
        .parse::<f32>()
        .unwrap_or(1.0);
    let interval = get_user_data("Enter the interval time: ")
        .parse::<u64>()
        .unwrap_or(1000);
    let flight_codes_by_airport= separate_flights_by_airport(flights_inserted, airport_codes);
    let airport_codes: Vec<String> = airport_codes
        .iter()
        .map(|airport_code| airport_code.to_string())
        .collect();
    simulator.flight_updates_loop(airports, airport_codes, flight_codes_by_airport, step, interval, thread_pool);
}

fn add_flights_for_airport(airports: &HashMap<String, Airport>, airport_codes: &mut HashSet<String>){
    let airport_code = get_user_data("Enter the airport code: ");
    if !airports.contains_key(&airport_code) {
        println!("Invalid airport code: {}", airport_code);
        return;
    }
    airport_codes.insert(airport_code);
}

fn add_single_flight(flights_inserted: &mut HashMap<String, Flight>, airports: &HashMap<String, Airport>, simulator: &Simulator, thread_pool: &ThreadPoolClient) {
    let mut flight = get_flight_data();
    let position = match airports.get(flight.get_departure_airport()){
        Some(airport) => airport.position,
        None => {println!("Invalid airport code: {}", flight.get_departure_airport()); return;}
    };
    let arrival_position = match airports.get(flight.get_arrival_airport()){
        Some(airport) => airport.position,
        None => {println!("Invalid airport code: {}", flight.get_arrival_airport()); return;}
    };
    flight.set_arrival_position(arrival_position);
    flight.restart(position);
    simulator.insert_single_flight(&flight, thread_pool);
    flights_inserted.insert(flight.get_code(), flight);
}

fn get_flight_data() -> Flight {
        println!("Enter flight status details");
        let code = get_user_data("Enter flight code: ");
        let departure_airport = get_user_data("Enter departure airport code: ");
        let arrival_airport = get_user_data("Enter arrival airport code: ");
        let departure_time = get_user_data("Enter departure time (e.g., HH:MM:SS): ");
        let arrival_time = get_user_data("Enter arrival time (e.g., HH:MM:SS): ");

        let status = FlightStatus {
            code,
            departure_airport,
            arrival_airport,
            departure_time,
            arrival_time,
        };
        Flight::new(FlightTracking::default(), status)
}

fn authenticate_client(client: &mut CassandraClient) {
    loop {
        let user = get_user_data("Enter the user: ");
        let password = get_user_data("Enter the password: ");
        let Err(e) = client.authenticate(&user, &password) else {
            break;
        };
        println!("Error authenticating: {e}\n trying again...");
    }
}

fn inicializate_clients() -> Result<Vec<CassandraClient>, String> {
    let cant_clients = get_user_data("Enter the number of clients: ").parse::<usize>()
        .map_err(|_| "Error parsing the number of clients".to_string())?;
    
    let simulator = Simulator;
    let mut clients = Vec::new();
    for i in 0..cant_clients {
        clear_screen();
        println!("Client {}", i + 1);
        let node = get_user_data("FULL IP (ip:port): ");
        let mut client = CassandraClient::new(&node)?;
        client.start_up()?;
        authenticate_client(&mut client);
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