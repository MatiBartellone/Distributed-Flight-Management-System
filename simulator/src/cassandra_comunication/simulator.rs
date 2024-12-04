use std::{collections::{HashMap, HashSet}, sync::{mpsc::{self}, Arc, Mutex}, thread, time::Duration};

use crate::{flight_implementation::{airport::Airport, flight::{Flight, FlightStatus, FlightTracking}, flight_state::FlightState}, utils::query_builder::QueryBuilder};

use super::{cassandra_client::{CassandraClient, STREAM}, thread_pool_client::ThreadPoolClient};

// Constantes globales para nombres de tablas y columnas en snake case
const KEYSPACE_AVIATION: &str = "aviation";
const TABLE_AIRPORTS: &str = "airports";
const TABLE_FLIGHTS_BY_AIRPORT: &str = "flights_by_airport";

// Columnas
const COL_AIRPORT_CODE: &str = "airport_code";
const COL_NAME: &str = "name";
const COL_POSITION_LAT: &str = "position_lat";
const COL_POSITION_LON: &str = "position_lon";

const COL_CODE: &str = "code";
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
        let query = QueryBuilder::new("SELECT", TABLE_AIRPORTS)
            .select(vec![COL_NAME, COL_POSITION_LAT, COL_POSITION_LON, COL_CODE])
            .where_condition(&format!("{} = '{}'", COL_CODE, airport_code), None)
            .build();
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
    
    /// Insert a flight in the database
    pub fn insert_single_flight(&self, flight: &Flight, thread_pool: &ThreadPoolClient) {
        let flight_thread = flight.clone();
        thread_pool.execute(move |frame_id, client| {
            let airport_code = flight_thread.get_departure_airport();
            _ = Self.insert_flight(client, &flight_thread, &airport_code, &frame_id);
        });
        let flight_thread = flight.clone();
        thread_pool.execute(move |frame_id, client| {
            let airport_code = flight_thread.get_arrival_airport();
            _ = Self.insert_flight(client, &flight_thread, &airport_code, &frame_id);
        });
    }

    fn insert_flight(&self, client: &mut CassandraClient, flight: &Flight, airport_code: &str, frame_id: &usize) -> Result<(), String> {
        let query = QueryBuilder::new("INSERT", TABLE_FLIGHTS_BY_AIRPORT)
            .insert(
                vec![
                    COL_AIRPORT_CODE,
                    COL_FLIGHT_CODE, 
                    COL_STATUS, 
                    COL_DEPARTURE_AIRPORT, 
                    COL_ARRIVAL_AIRPORT, 
                    COL_DEPARTURE_TIME, 
                    COL_ARRIVAL_TIME, 
                    COL_POSITION_LAT, 
                    COL_POSITION_LON, 
                    COL_ARRIVAL_POSITION_LAT, 
                    COL_ARRIVAL_POSITION_LON, 
                    COL_ALTITUDE, 
                    COL_SPEED, 
                    COL_FUEL_LEVEL
                ],
                vec![
                    airport_code, 
                    &flight.get_code(), 
                    &flight.get_status().to_string(),
                    &flight.get_departure_airport(), 
                    flight.get_arrival_airport(), 
                    flight.get_departure_time(), 
                    flight.get_arrival_time(), 
                    &flight.get_position().0.to_string(), 
                    &flight.get_position().1.to_string(), 
                    &flight.get_arrival_position().0.to_string(),
                    &flight.get_arrival_position().1.to_string(), 
                    &flight.get_altitude().to_string(), 
                    &flight.get_speed().to_string(), 
                    &flight.get_fuel_level().to_string()
                ])
            .build();
        client.execute_strong_query_without_response(&query, frame_id)
    }

    /// Get the information of the flights
    fn get_flights_by_airport(
        &self,
        airport_code: &str,
        thread_pool: &ThreadPoolClient
    ) -> Vec<Flight> {
        let code_status = airport_code.to_string();
        let status_receiver = thread_pool.execute(move |frame_id, client| {
            Self.get_flight_status(client, &code_status, &frame_id)
        });

        let code_tracking = airport_code.to_string();
        let tracking_receiver = thread_pool.execute(move |frame_id, client| {
            Self.get_flight_tracking(client, &code_tracking, &frame_id)
        });

        thread_pool.join();
        let status_values = status_receiver.into_iter().flatten().collect::<Vec<HashMap<String, String>>>();
        let tracking_values = tracking_receiver.into_iter().flatten().collect::<Vec<HashMap<String, String>>>();
        
        let mut flights = Vec::new();
        for (status, tracking) in status_values.iter().zip(tracking_values.iter()) {
            let Some(flight) = self.get_flight(status, tracking) else {continue};
            flights.push(flight);
        }
        flights

    }

    // Get the information of the flight
    fn get_flight(&self, status: &HashMap<String, String>, tracking: &HashMap<String, String>) -> Option<Flight> {
        let status = self.values_to_flight_status(status)?;
        let tracking = self.values_to_flight_tracking(tracking)?;
        Some(Flight {
            status,
            tracking,
        })
    }

    fn get_flight_status(&self, client: &mut CassandraClient, airport_code: &str, frame_id: &usize) ->Vec<HashMap<String, String>> {
        let query = QueryBuilder::new("SELECT", TABLE_FLIGHTS_BY_AIRPORT)
            .select(vec![COL_FLIGHT_CODE, COL_STATUS, COL_ARRIVAL_AIRPORT])
            .where_condition(&format!("{} = '{}'", COL_AIRPORT_CODE, airport_code), None)
            .order_by(COL_FLIGHT_CODE, None)
            .build();
        client.execute_strong_select_query(&query, frame_id)
            .unwrap_or_default()
    }
    
    fn values_to_flight_status(&self, values: &HashMap<String, String>)-> Option<FlightStatus> {  
        let code = values.get(COL_FLIGHT_CODE)?.to_string();
        let status_str = values.get(COL_STATUS)?;
        let status = FlightState::new(status_str);
        let departure_airport = values.get(COL_DEPARTURE_AIRPORT)?.to_string();
        let arrival_airport = values.get(COL_ARRIVAL_AIRPORT)?.to_string();
        let departure_time = values.get(COL_DEPARTURE_TIME)?.to_string();
        let arrival_time = values.get(COL_ARRIVAL_TIME)?.to_string();

        Some(FlightStatus {
            code,
            status,
            departure_airport,
            arrival_airport,
            departure_time,
            arrival_time,
        })
    }

    fn get_flight_tracking(&mut self, client: &mut CassandraClient, airport_code: &str, frame_id: &usize) -> Vec<HashMap<String, String>> {
        let query = QueryBuilder::new("SELECT", TABLE_FLIGHTS_BY_AIRPORT)
            .select(vec![COL_POSITION_LAT, COL_POSITION_LON, COL_ARRIVAL_POSITION_LAT, COL_ARRIVAL_POSITION_LON])
            .where_condition(&format!("{} = '{}'", COL_AIRPORT_CODE, airport_code), None)
            .order_by(COL_FLIGHT_CODE, None)
            .build();
        client.execute_weak_select_query(&query, frame_id)
            .unwrap_or_default()
    }

    fn values_to_flight_tracking(&self, values: &HashMap<String, String>)-> Option<FlightTracking> {
        let position_lat: f64 = values.get(COL_POSITION_LAT)?.parse().ok()?;
        let position_lon: f64 = values.get(COL_POSITION_LON)?.parse().ok()?;
        let arrival_position_lat: f64 = values.get(COL_ARRIVAL_POSITION_LAT)?.parse().ok()?;
        let arrival_position_lon: f64 = values.get(COL_ARRIVAL_POSITION_LON)?.parse().ok()?;
        let altitude: f64 = values.get(COL_ALTITUDE)?.parse().ok()?;
        let speed: f32 = values.get(COL_SPEED)?.parse().ok()?;
        let fuel_level: f32 = values.get(COL_FUEL_LEVEL)?.parse().ok()?;

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
        let query = QueryBuilder::new("UPDATE", TABLE_FLIGHTS_BY_AIRPORT)
            .update(
                vec![
                    (COL_STATUS, &flight.get_status().to_string()), 
                    (COL_DEPARTURE_AIRPORT, &flight.get_departure_airport()), 
                    (COL_ARRIVAL_AIRPORT, &flight.get_departure_airport()), 
                    (COL_DEPARTURE_TIME, &flight.get_departure_airport()), 
                    (COL_ARRIVAL_TIME, &flight.get_departure_airport()),
                ]
            )
            .where_condition(&format!("{} = '{}'", COL_FLIGHT_CODE, flight.get_code()), None)
            .build();
        client.execute_strong_query_without_response(&query, frame_id)
    }

    fn update_flight_tracking(&self, client: &mut CassandraClient, flight: &Flight, frame_id: &usize) -> Result<(), String> {
        let query = QueryBuilder::new("UPDATE", TABLE_FLIGHTS_BY_AIRPORT)
            .update(
                vec![
                    (COL_POSITION_LAT, &flight.get_position().0.to_string()), 
                    (COL_POSITION_LON, &flight.get_position().1.to_string()), 
                    (COL_ARRIVAL_POSITION_LAT, &flight.get_arrival_position().0.to_string()), 
                    (COL_ARRIVAL_POSITION_LON, &flight.get_arrival_position().1.to_string()), 
                    (COL_ALTITUDE, &flight.get_altitude().to_string()), 
                    (COL_SPEED, &flight.get_speed().to_string()), 
                    (COL_FUEL_LEVEL, &flight.get_fuel_level().to_string())
                ]
            )
            .where_condition(&format!("{} = '{}'", COL_FLIGHT_CODE, flight.get_code()), None)
            .build();
        client.execute_weak_query_without_response(&query, frame_id)
    }

    fn get_selected_flights(&self, airports: &HashMap<String, Airport>, flight_codes: &HashSet<String>, thread_pool: &ThreadPoolClient) -> Vec<Flight> {
        let mut flights = Vec::new();
        for airport in airports.values() {
            flights.extend(self.get_flights_by_airport(&airport.code, thread_pool));
        }
        // falta agregar los aviones individuales
        flights
    }

    /// Loop that updates the flights in the airport every interval of time
    pub fn flight_updates_loop(
        &self,
        airports: &HashMap<String, Airport>,
        flight_codes: HashSet<String>,
        step: f32,
        interval: u64,
        thread_pool: &ThreadPoolClient
    ) {
        loop {
            let flights = self.get_selected_flights(airports, &flight_codes, thread_pool);
            for mut flight in flights {
                let arrival_position = match  airports.get(flight.get_arrival_airport()) {
                    Some(airport) => airport.position,
                    None => continue,
                };
                
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