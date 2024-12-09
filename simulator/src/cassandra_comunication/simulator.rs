use std::{collections::HashMap, thread, time::Duration};

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
        let mut receivers = Vec::new();
        for airport_code in airports_codes {
            let airport_receiver = thread_pool.execute(move |frame_id, client| {
                Self.get_airport(client, &airport_code, &frame_id)
            });
            receivers.push(airport_receiver);
        }

        let mut airports = HashMap::new();
        for receiver in receivers {
            let Ok(received_airport) = receiver.recv() else {continue};
            let Some(airport) = received_airport else {continue};
            airports.insert(airport.code.to_string(), airport);
        }
        airports
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
            _ = Self.insert_flight(client, &flight_thread, airport_code, &frame_id);
        });
        let flight_thread = flight.clone();
        thread_pool.execute(move |frame_id, client| {
            let airport_code = flight_thread.get_arrival_airport();
            _ = Self.insert_flight(client, &flight_thread, airport_code, &frame_id);
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
                    &format!("'{}'", airport_code), 
                    &format!("'{}'", flight.get_code()), 
                    &format!("'{}'", flight.get_status()),
                    &format!("'{}'", flight.get_departure_airport()), 
                    &format!("'{}'", flight.get_arrival_airport()), 
                    &format!("'{}'", flight.get_departure_time()), 
                    &format!("'{}'", flight.get_arrival_time()), 
                    &format!("{:.1}", flight.get_position().0), 
                    &format!("{:.1}", flight.get_position().1), 
                    &format!("{:.1}", flight.get_arrival_position().0),
                    &format!("{:.1}", flight.get_arrival_position().1), 
                    &format!("{:.1}", flight.get_altitude()), 
                    &format!("{:.1}", flight.get_speed()), 
                    &format!("{:.1}", flight.get_fuel_level())
                ])
            .build();
        client.execute_strong_query_without_response(&query, frame_id)
    }

    fn get_flights_by_airports(&self, airports_codes: &Vec<String>, flight_codes_by_airport: &HashMap<String, Vec<String>>, thread_pool: &ThreadPoolClient) -> Vec<Flight> {
        let mut receivers = Vec::new();
        for airport_code in airports_codes {
            let airport_code = airport_code.to_string();
            let flight_codes_airport = flight_codes_by_airport.get(&airport_code).unwrap_or(&Vec::new()).to_vec();
            let flights_receiver = thread_pool.execute(move |frame_id, client| {
                Self.get_flights_by_airport(&airport_code, &flight_codes_airport, client, &frame_id)
            });
            receivers.push(flights_receiver);
        }

        let mut flights = Vec::new();
        for receiver in receivers {
            if let Ok(mut received_flights) = receiver.recv() {
                flights.append(&mut received_flights);
            }
        }
        flights
    }

    fn get_status_map(&self, status_values: Vec<HashMap<String, String>>) -> HashMap<String, HashMap<String, String>>  {
        status_values
            .into_iter()
            .filter_map(|status| {
                let code = status.get(COL_FLIGHT_CODE)?;
                Some((code.to_string(), status))
            })
            .collect()
    }
    
    fn get_tracking_map(&self, tracking_values: Vec<HashMap<String, String>>) -> Vec<(String, HashMap<String, String>)> {
        tracking_values
            .into_iter()
            .filter_map(|mut tracking| {
                tracking
                    .remove(COL_FLIGHT_CODE)
                    .map(|code| (code, tracking))
            })
            .collect()
    }

    /// Get the information of the flights
    fn get_flights_by_airport(&self, airport_code: &str, flight_codes_airport: &[String], client: &mut CassandraClient, frame_id: &usize) -> Vec<Flight> {
        let status_values = Self.get_flight_status(client, airport_code, flight_codes_airport, frame_id);
        let tracking_values= Self.get_flight_tracking(client, airport_code, flight_codes_airport, frame_id);
        
        let status_map = self.get_status_map(status_values);
        let tracking_map = self.get_tracking_map(tracking_values);

        let mut flights = Vec::new();
        for (flight_code, tracking) in tracking_map {
            if let Some(status) = status_map.get(&flight_code) {
                if let Some(flight) = self.get_flight(status, &tracking) {
                    flights.push(flight);
                }
            }
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

    fn get_flight_status(&self, client: &mut CassandraClient, airport_code: &str, flight_codes_airport: &[String], frame_id: &usize) ->Vec<HashMap<String, String>> {
        let mut query_builder = QueryBuilder::new("SELECT", TABLE_FLIGHTS_BY_AIRPORT)
            .select(vec![COL_FLIGHT_CODE, COL_DEPARTURE_AIRPORT, COL_ARRIVAL_AIRPORT, COL_DEPARTURE_TIME, COL_ARRIVAL_TIME])
            .where_condition(&format!("{} = '{}'", COL_AIRPORT_CODE, airport_code), Some("AND"));

        if !flight_codes_airport.is_empty() {
            let or_conditions: Vec<String> = flight_codes_airport
                .iter()
                .map(|code| format!("{} = '{}'", COL_FLIGHT_CODE, code))
                .collect();
            let or_clause = format!("({})", or_conditions.join(" OR "));
            query_builder = query_builder.where_condition(&or_clause, None);
        }
            
        let query = query_builder
            .order_by(COL_FLIGHT_CODE, None)
            .build();
        client.execute_strong_select_query(&query, frame_id)
            .unwrap_or_default()
    }
    
    fn values_to_flight_status(&self, values: &HashMap<String, String>)-> Option<FlightStatus> {
        let code = values.get(COL_FLIGHT_CODE)?.to_string();
        let departure_airport = values.get(COL_DEPARTURE_AIRPORT)?.to_string();
        let arrival_airport = values.get(COL_ARRIVAL_AIRPORT)?.to_string();
        let departure_time = values.get(COL_DEPARTURE_TIME)?.to_string();
        let arrival_time = values.get(COL_ARRIVAL_TIME)?.to_string();

        Some(FlightStatus {
            code,
            departure_airport,
            arrival_airport,
            departure_time,
            arrival_time,
        })
    }

    fn get_flight_tracking(&mut self, client: &mut CassandraClient, airport_code: &str, flight_codes_airport: &[String], frame_id: &usize) -> Vec<HashMap<String, String>> {
        let mut query_builder = QueryBuilder::new("SELECT", TABLE_FLIGHTS_BY_AIRPORT)
            .select(vec![COL_FLIGHT_CODE, COL_STATUS, COL_POSITION_LAT, COL_POSITION_LON, COL_ARRIVAL_POSITION_LAT, COL_ARRIVAL_POSITION_LON, COL_ALTITUDE, COL_SPEED, COL_FUEL_LEVEL])
            .where_condition(&format!("{} = '{}'", COL_AIRPORT_CODE, airport_code), Some("AND"));

        if !flight_codes_airport.is_empty() {
            let or_conditions: Vec<String> = flight_codes_airport
                .iter()
                .map(|code| format!("{} = '{}'", COL_FLIGHT_CODE, code))
                .collect();
            let or_clause = format!("({})", or_conditions.join(" OR "));
            query_builder = query_builder.where_condition(&or_clause, None);
        }
            
        let query = query_builder
            .order_by(COL_FLIGHT_CODE, None)
            .build();
        client.execute_weak_select_query(&query, frame_id)
            .unwrap_or_default()
    }

    fn values_to_flight_tracking(&self, values: &HashMap<String, String>)-> Option<FlightTracking> {
        let status_str = values.get(COL_STATUS)?;
        let status = FlightState::new(status_str);
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
            status,
            ..Default::default()
        })
    }

    /// Update the flight information in the database with the new information
    pub fn update_flight(&self, client: &mut CassandraClient, flight: &Flight, frame_id: &usize) -> Result<(), String> {
        self.update_flight_tracking(client, flight, flight.get_arrival_airport(), frame_id)?;
        self.update_flight_tracking(client, flight, flight.get_departure_airport(), frame_id)
    }

    fn update_flight_tracking(&self, client: &mut CassandraClient, flight: &Flight, airport_code: &str, frame_id: &usize) -> Result<(), String> {
        let query = QueryBuilder::new("UPDATE", TABLE_FLIGHTS_BY_AIRPORT)
            .update(
                vec![
                    (COL_STATUS, &format!("'{}'", flight.get_status())),
                    (COL_POSITION_LAT, &format!("{:.1}", flight.get_position().0)), 
                    (COL_POSITION_LON, &format!("{:.1}", flight.get_position().1)), 
                    (COL_ARRIVAL_POSITION_LAT, &format!("{:.1}", flight.get_arrival_position().0)), 
                    (COL_ARRIVAL_POSITION_LON, &format!("{:.1}", flight.get_arrival_position().1)), 
                    (COL_ALTITUDE, &format!("{:.1}", flight.get_altitude())), 
                    (COL_SPEED, &format!("{:.1}", flight.get_speed())), 
                    (COL_FUEL_LEVEL, &format!("{:.1}", flight.get_fuel_level()))
                ]
            )
            .where_condition(&format!("{} = '{}'", COL_FLIGHT_CODE, flight.get_code()), Some("AND"))
            .where_condition(&format!("{} = '{}'", COL_AIRPORT_CODE, airport_code), Some("AND"))
            .build();
        client.execute_weak_query_without_response(&query, frame_id)
    }

    fn get_selected_flights(&self, airports_codes: &Vec<String>, flight_codes_by_airport: &HashMap<String, Vec<String>>, thread_pool: &ThreadPoolClient) -> Vec<Flight> {
        let mut flights = self.get_flights_by_airports(airports_codes, &HashMap::new(), thread_pool);
        if flight_codes_by_airport.is_empty() {
            return flights;
        }
        let airport_flight_codes: Vec<String> = flight_codes_by_airport
            .keys()
            .map(|airport_code| airport_code.to_string())
            .collect();
        flights.extend(self.get_flights_by_airports(&airport_flight_codes, flight_codes_by_airport, thread_pool));
        flights
    }

    /// Loop that updates the flights in the airport every interval of time
    pub fn flight_updates_loop(
        &self,
        airport_codes: Vec<String>,
        flight_codes_by_airport: HashMap<String, Vec<String>>,
        step: f32,
        interval: u64,
        thread_pool: &ThreadPoolClient
    ) {
        loop {
            let flights = self.get_selected_flights(&airport_codes, &flight_codes_by_airport, thread_pool);
            let mut all_arrived = true;
            for mut flight  in flights.into_iter() {
                if flight.has_arrived() {
                    continue;
                }
                all_arrived = false;
                let _ = thread_pool.execute(move |frame_id, client| {
                    flight.update_progress(step);
                    let _ = Self.update_flight(client, &flight, &frame_id);
                    if flight.has_arrived() {
                        println!("Flight {} has arrived.", flight.get_code());
                    }
                });
            }
            thread_pool.join();

            if all_arrived {
                println!("All flights have arrived. Stopping updates.");
                break;
            }
            thread::sleep(Duration::from_millis(interval));
        }
        
    }
}