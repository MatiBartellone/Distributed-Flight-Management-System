use std::{collections::{HashMap, HashSet}, sync::{mpsc::{self}, Arc, Mutex}, thread, time::Duration};

use crate::flight_implementation::{airport::Airport, flight::{Flight, FlightStatus, FlightTracking}, flight_state::FlightState};

use super::{cassandra_client::{CassandraClient, STREAM}, thread_pool_client::ThreadPoolClient};

// Constantes globales para nombres de tablas y columnas en snake case
const KEYSPACE_AVIATION: &str = "aviation";
const TABLE_AIRPORTS: &str = "aviation.airports";
const TABLE_FLIGHTS_BY_AIRPORT: &str = "aviation.flights_by_airport";
const TABLE_FLIGHT_INFO: &str = "aviation.flight_info";

// Columnas
const COL_NAME: &str = "name";
const COL_POSITION_LAT: &str = "position_lat";
const COL_POSITION_LON: &str = "position_lon";
const COL_CODE: &str = "code";
const COL_AIRPORT_CODE: &str = "airport_code";
const COL_FLIGHT_CODE: &str = "flight_code";
const COL_STATUS: &str = "status";
const COL_ARRIVAL_AIRPORT: &str = "arrival_airport";
const COL_DEPARTURE_AIRPORT: &str = "departure_airport";
const COL_DEPARTURE_TIME: &str = "departure_time";
const COL_ARRIVAL_TIME: &str = "arrival_time";
const COL_ARRIVAL_POSITION_LAT: &str = "arrival_position_lat";
const COL_ARRIVAL_POSITION_LON: &str = "arrival_position_lon";
const COL_ALTITUDE: &str = "altitude";
const COL_SPEED: &str = "speed";
const COL_FUEL_LEVEL: &str = "fuel_level";

pub struct Simulator;

impl Simulator {
    /// Use the aviation keyspace in the cassandra database
    pub fn use_aviation_keyspace(&self, client: &mut CassandraClient) -> Result<(), String> {
        let frame_id = STREAM as usize;
        let query = format!("USE {};", KEYSPACE_AVIATION);
        client.execute_strong_query_without_response(&query, &frame_id)
    }

    /// Get the information of the airports
    pub fn get_airports(&self, airports_codes: Vec<String>, thread_pool: &ThreadPoolClient) ->HashMap<String, Airport> {
        let (tx, rx) = mpsc::channel();
        let tx = Arc::new(Mutex::new(tx));
        for code in airports_codes {
            let tx = Arc::clone(&tx);
            thread_pool.execute(move |frame_id, client| {
                if let Some(airport) = Self.get_airport(client, &code, &frame_id) {
                    if let Ok(tx) = tx.lock(){
                        let _ = tx.send(airport);
                    }
                }
            });
        }
        thread_pool.join();
        drop(tx);
        rx.into_iter()
            .map(|airport| (airport.code.to_string(), airport))
            .collect()
    }

    fn get_airport(&self, client: &mut CassandraClient, airport_code: &str, frame_id: &usize) -> Option<Airport> {
        let query = format!(
            "SELECT {}, {}, {}, {} FROM {} WHERE {} = '{}';",
            COL_NAME, COL_POSITION_LAT, COL_POSITION_LON, COL_CODE, 
            TABLE_AIRPORTS, COL_CODE, airport_code
        );
        let values = client.execute_strong_select_query(&query, frame_id).ok()?;
        self.values_to_airport(&values)
    }

    // Transforms the row to airport
    fn values_to_airport(&self, values: &[HashMap<String, String>]) -> Option<Airport> {
        let row = values.first()?;
        let name = row.get(COL_NAME)?.to_string();
        let code = row.get(COL_CODE)?.to_string();
        let position_lat = row.get(COL_POSITION_LAT)?.parse::<f64>().ok()?;
        let position_lon = row.get(COL_POSITION_LON)?.parse::<f64>().ok()?;

        Some(Airport {
            name,
            code,
            position: (position_lat, position_lon),
        })
    }

    fn get_codes(
        &self,
        airport_code: &str,
        thread_pool: &ThreadPoolClient
    ) -> HashSet<String> {
        let (tx, rx) = mpsc::channel();
        let airport_code = airport_code.to_string();
        thread_pool.execute(move |frame_id, client| {
            if let Some(flight_codes) = Self.get_flight_codes_by_airport(client, &airport_code, &frame_id) {
                tx.send(flight_codes).expect("Error sending the flight codes");
            } else {
                tx.send(HashSet::new()).expect("Error sending the flight codes");
            }
        });
    
        thread_pool.join();
        rx.recv().unwrap_or_default()
    }

    // Gets all de flights codes going or leaving the aiport
    fn get_flight_codes_by_airport(&self, client: &mut CassandraClient, airport_code: &str, frame_id: &usize) -> Option<HashSet<String>> {
        let query = format!(
            "SELECT {} FROM {} WHERE {} = '{}';",
            COL_FLIGHT_CODE, TABLE_FLIGHTS_BY_AIRPORT, COL_AIRPORT_CODE, airport_code
        );
        let values = client.execute_strong_select_query(&query, frame_id).ok()?;
        self.values_to_flight_codes(&values)
    }

    // Transforms the values to flight codes
    fn values_to_flight_codes(&self, flight_codes: &Vec<HashMap<String, String>>) -> Option<HashSet<String>> {
        let mut codes = HashSet::new();
        for row in flight_codes {
            if let Some(code) = row.get(COL_FLIGHT_CODE){
                codes.insert(code.to_string());
            }
        }
        Some(codes)
    }


    /// Insert a flight in the database
    pub fn insert_single_flight(&self, flight: &Flight, thread_pool: &ThreadPoolClient) {
        let flight_thread = flight.clone();
        thread_pool.execute(move |frame_id, client| {
            _ = Self.insert_flight(client, &flight_thread, &frame_id);
        });

        let code = flight.get_code().to_string();
        let departure_airport = flight.get_departure_airport().to_string();
        thread_pool.execute(move |frame_id, client| {
            _ = Self.insert_flight_code_to_airport(client, &departure_airport, &code, &frame_id);
        });

        let code = flight.get_code().to_string();
        let arrival_airport = flight.get_arrival_airport().to_string();
        thread_pool.execute(move |frame_id, client| {
            _ = Self.insert_flight_code_to_airport(client, &arrival_airport, &code, &frame_id);
        });
    }

    fn insert_flight(&self, client: &mut CassandraClient, flight: &Flight, frame_id: &usize) -> Result<(), String> {
        let query = format!(
            "INSERT INTO {TABLE_FLIGHT_INFO} ({COL_FLIGHT_CODE}, {COL_STATUS}, {COL_DEPARTURE_AIRPORT}, {COL_ARRIVAL_AIRPORT}, {COL_DEPARTURE_TIME}, {COL_ARRIVAL_TIME}, {COL_POSITION_LAT}, {COL_POSITION_LON}, {COL_ARRIVAL_POSITION_LAT}, {COL_ARRIVAL_POSITION_LON}, {COL_ALTITUDE}, {COL_SPEED}, {COL_FUEL_LEVEL}) VALUES ('{}', '{}', '{}', '{}', '{}', '{}', {:.1}, {:.1}, {:.1}, {:.1}, {:.1}, {:.1}, {:.1});",
            flight.get_code(), 
            flight.get_status(), 
            flight.get_departure_airport(), 
            flight.get_arrival_airport(), 
            flight.get_departure_time(), 
            flight.get_arrival_time(),
            flight.get_position().0, 
            flight.get_position().1, 
            flight.get_arrival_position().0, 
            flight.get_arrival_position().1, 
            flight.get_altitude(), 
            flight.get_speed(), 
            flight.get_fuel_level()
        );
        client.execute_strong_query_without_response(&query, frame_id)
    }

    fn insert_flight_code_to_airport(&self, client: &mut CassandraClient, airport_code: &str, flight_code: &str, frame_id: &usize) -> Result<(), String> {
        let query = format!(
            "INSERT INTO {TABLE_FLIGHTS_BY_AIRPORT} ({COL_AIRPORT_CODE}, {COL_FLIGHT_CODE}) VALUES ('{}', '{}');",
            airport_code, flight_code
        );
        client.execute_strong_query_without_response(&query, frame_id)
    }

    /// Get the information of the flights
    pub fn get_flights(
        &self,
        airport_code: &str,
        thread_pool: &ThreadPoolClient
    ) -> Vec<Flight> {
        let flight_codes = self.get_codes(airport_code, thread_pool);
    
        let (tx, rx) = mpsc::channel();
        let tx = Arc::new(Mutex::new(tx));
        for code in flight_codes {
            let tx = Arc::clone(&tx);
            thread_pool.execute(move |frame_id, client| {
                if let Some(flight) = Self.get_flight(client, &code, &frame_id) {
                    if let Ok(tx) = tx.lock(){
                        let _ = tx.send(flight);
                    }
                }
            });
        }
    
        thread_pool.join();
        drop(tx);
        rx.into_iter().collect()
    }

    /// Get the information of the flights by their codes
    fn get_flights_by_codes(
        &self,
        flight_codes: HashSet<String>,
        thread_pool: &ThreadPoolClient
    ) -> Vec<Flight> {
        let (tx, rx) = mpsc::channel();
        let tx = Arc::new(Mutex::new(tx));
        for code in flight_codes {
            let tx = Arc::clone(&tx);
            thread_pool.execute(move |frame_id, client| {
                if let Some(flight) = Self.get_flight(client, &code, &frame_id) {
                    if let Ok(tx) = tx.lock(){
                        let _ = tx.send(flight);
                    }
                }
            });
        }
    
        thread_pool.join();
        drop(tx);
        rx.into_iter().collect()
    }

    // Get the information of the flight
    fn get_flight(&self, client: &mut CassandraClient, flight_code: &str, frame_id: &usize) -> Option<Flight> {
        let flight_status = self.get_flight_status(client, flight_code, frame_id)?;
        let flight_tracking = self.get_flight_tracking(client, flight_code, frame_id)?;
        Some(Flight {
            info: flight_tracking,
            status: flight_status
        })
    }

    fn get_flight_status(&self, client: &mut CassandraClient, flight_code: &str, frame_id: &usize) -> Option<FlightStatus> {
        let query = format!(
            "SELECT {}, {}, {}, {}, {}, {} FROM {} WHERE {} = '{}';",
            COL_FLIGHT_CODE, COL_STATUS, COL_DEPARTURE_AIRPORT, COL_ARRIVAL_AIRPORT, COL_DEPARTURE_TIME, COL_ARRIVAL_TIME,
            TABLE_FLIGHT_INFO, COL_FLIGHT_CODE, flight_code
        );
        let values = client.execute_strong_select_query(&query, frame_id).ok()?;
        self.values_to_flight_status(&values)
    }
    
    fn values_to_flight_status(&self, values: &[HashMap<String, String>])-> Option<FlightStatus> {
        let strong_row = values.first()?;                
        let code = strong_row.get(COL_FLIGHT_CODE)?.to_string();
        let status_str = strong_row.get(COL_STATUS)?;
        let status = FlightState::new(status_str);
        let departure_airport = strong_row.get(COL_DEPARTURE_AIRPORT)?.to_string();
        let arrival_airport = strong_row.get(COL_ARRIVAL_AIRPORT)?.to_string();
        let departure_time = strong_row.get(COL_DEPARTURE_TIME)?.to_string();
        let arrival_time = strong_row.get(COL_ARRIVAL_TIME)?.to_string();

        Some(FlightStatus {
            code,
            status,
            departure_airport,
            arrival_airport,
            departure_time,
            arrival_time,
        })
    }

    fn get_flight_tracking(&self, client: &mut CassandraClient, flight_code: &str, frame_id: &usize) -> Option<FlightTracking> {
        let query = format!(
            "SELECT {}, {}, {}, {}, {}, {}, {} FROM {} WHERE {} = '{}';",
            COL_POSITION_LAT, COL_POSITION_LON, COL_ARRIVAL_POSITION_LAT, COL_ARRIVAL_POSITION_LON,
            COL_ALTITUDE, COL_SPEED, COL_FUEL_LEVEL,
            TABLE_FLIGHT_INFO, COL_FLIGHT_CODE, flight_code
        );
        let values = client.execute_weak_select_query(&query, frame_id).ok()?;
        self.values_to_flight_tracking(&values)
    }

    fn values_to_flight_tracking(&self, values: &[HashMap<String, String>])-> Option<FlightTracking> {
        let weak_row = values.first()?;
        let position_lat: f64 = weak_row.get(COL_POSITION_LAT)?.parse().ok()?;
        let position_lon: f64 = weak_row.get(COL_POSITION_LON)?.parse().ok()?;
        let arrival_position_lat: f64 = weak_row.get(COL_ARRIVAL_POSITION_LAT)?.parse().ok()?;
        let arrival_position_lon: f64 = weak_row.get(COL_ARRIVAL_POSITION_LON)?.parse().ok()?;
        let altitude: f64 = weak_row.get(COL_ALTITUDE)?.parse().ok()?;
        let speed: f32 = weak_row.get(COL_SPEED)?.parse().ok()?;
        let fuel_level: f32 = weak_row.get(COL_FUEL_LEVEL)?.parse().ok()?;

        Some(FlightTracking {
            position: (position_lat, position_lon),
            arrival_position: (arrival_position_lat, arrival_position_lon),
            altitude,
            speed,
            fuel_level,
        })
    }

    /// Update the flight information in the database with the new information
    pub fn update_flight(&self, client: &mut CassandraClient, flight: &Flight, frame_id: &usize) -> Result<(), String> {
        self.update_flight_status(client, flight, frame_id)?;
        self.update_flight_tracking(client, flight, frame_id)
    }

    fn update_flight_status(&self, client: &mut CassandraClient, flight: &Flight, frame_id: &usize) -> Result<(), String> {
        let query = format!(
            "UPDATE {TABLE_FLIGHT_INFO} SET {COL_STATUS} = '{}', {COL_DEPARTURE_AIRPORT} = '{}', {COL_ARRIVAL_AIRPORT} = '{}', \"{COL_DEPARTURE_TIME}\" = '{}', \"{COL_ARRIVAL_TIME}\" = '{}' WHERE \"{COL_FLIGHT_CODE}\" = '{}';",
            flight.get_status(),
            flight.get_departure_airport(),
            flight.get_arrival_airport(),
            flight.get_departure_time(),
            flight.get_arrival_time(),
            flight.get_code()
        );
        println!("Query:status: {}", query);
        client.execute_strong_query_without_response(&query, frame_id)
    }

    fn update_flight_tracking(&self, client: &mut CassandraClient, flight: &Flight, frame_id: &usize) -> Result<(), String> {
        let query = format!(
            "UPDATE {TABLE_FLIGHT_INFO} SET {COL_POSITION_LAT} = {:.1}, {COL_POSITION_LON} = {:.1}, {COL_ARRIVAL_POSITION_LAT} = {:.1}, {COL_ARRIVAL_POSITION_LON} = {:.1}, {COL_ALTITUDE} = {:.1}, {COL_SPEED} = {:.1}, {COL_FUEL_LEVEL} = {:.1} WHERE {COL_FLIGHT_CODE} = '{}';",
            flight.get_position().0,
            flight.get_position().1,
            flight.get_arrival_position().0,
            flight.get_arrival_position().1,
            flight.get_altitude(),
            flight.get_speed(),
            flight.get_fuel_level(),
            flight.get_code()
        );
        println!("Query:tracking: {}", query);
        client.execute_weak_query_without_response(&query, frame_id)
    }

    /// Restarts all the flights in the airport to the initial state
    pub fn restart_flights(&self, flights: &mut [Flight], airports: &HashMap<String, Airport>, thread_pool: &ThreadPoolClient) {
        for mut flight in flights.iter().cloned() {
            println!("Restarting flight: {:?}", flight);
            let position = match airports.get(flight.get_departure_airport()) {
                Some(airport) => airport.position,
                None => continue,
            };
            thread_pool.execute(move |frame_id, client| {
                flight.restart(position);
                _ = Self.update_flight(client, &flight, &frame_id);
                println!("Restarted flight: {:?}", flight);
            });
        }

        thread_pool.join();
    }

    /// Loop that updates the flights in the airport every interval of time
    pub fn flight_updates_loop(
        &self,
        flights: Vec<Flight>,
        airports: &HashMap<String, Airport>,
        step: f32,
        interval: u64,
        thread_pool: &ThreadPoolClient
    ) {
        let codes: HashSet<String> = flights.iter().map(|flight| flight.get_code().to_string()).collect();
        loop {
            for flight in self.get_flights_by_codes(codes.clone(), thread_pool) {
                let arrival_position = match  airports.get(flight.get_arrival_airport()) {
                    Some(airport) => airport.position,
                    None => continue,
                };
                let mut flight = flight.clone();
                thread_pool.execute(move |frame_id, client| {
                    flight.update_progress(arrival_position, step);
                    _ = Self.update_flight(client, &flight, &frame_id);
                });
            }
            thread_pool.join();
            thread::sleep(Duration::from_millis(interval));
        }
    }
}