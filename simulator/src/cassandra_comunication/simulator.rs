use std::{collections::{HashMap, HashSet}, sync::{mpsc::{self}, Arc, Mutex}, thread, time::Duration};

use crate::flight_implementation::{airport::Airport, flight::{Flight, FlightStatus, FlightTracking}, flight_state::FlightState};

use super::{cassandra_client::{CassandraClient, STREAM}, thread_pool_client::ThreadPoolClient};

pub struct Simulator;

impl Simulator {
    /// Use the aviation keyspace in the cassandra database
    pub fn use_aviation_keyspace(&self, client: &mut CassandraClient) -> Result<(), String> {
        let frame_id = STREAM as usize;
        client.execute_strong_query_without_response("USE aviation;", &frame_id)
    }

    pub fn get_airports(&self, airports_codes: Vec<String>, thread_pool: &ThreadPoolClient) -> HashMap<String, Airport> {
        let (tx, rx) = mpsc::channel();
        let tx = Arc::new(Mutex::new(tx));
        for code in airports_codes {
            let simulator = Self; 
            let tx = Arc::clone(&tx);
            thread_pool.execute(move |frame_id, client| {
                if let Some(airport) = simulator.get_airport(client, &code, &frame_id) {
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

    /// Get the information of the airport
    pub fn get_airport(&self, client: &mut CassandraClient, airport_code: &str, frame_id: &usize) -> Option<Airport> {
        let query = format!(
            "SELECT name, \"positionLat\", \"positionLon\", code FROM aviation.airports WHERE code = '{}';",
            airport_code
        );
        let values = client.execute_strong_select_query(&query, frame_id).ok()?;
        self.values_to_airport(&values)
    }

    // Transforms the row to airport
    fn values_to_airport(&self, values: &Vec<HashMap<String, String>>) -> Option<Airport> {
        let row = values.get(0)?;
        let name = row.get("name")?.to_string();
        let code = row.get("code")?.to_string();
        let position_lat = row.get("positionLat")?.parse::<f64>().ok()?;
        let position_lon = row.get("positionLon")?.parse::<f64>().ok()?;

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
        let simulator = Self; 
        thread_pool.execute(move |frame_id, client| {
            if let Some(flight_codes) = simulator.get_flight_codes_by_airport(client, &airport_code, &frame_id) {
                tx.send(flight_codes).expect("Error sending the flight codes");
            } else {
                tx.send(HashSet::new()).expect("Error sending the flight codes");
            }
        });
    
        thread_pool.join();
        match rx.recv() {
            Ok(flight_codes) => flight_codes,
            Err(_) => HashSet::new()
        }
    }

    // Gets all de flights codes going or leaving the aiport
    fn get_flight_codes_by_airport(&self, client: &mut CassandraClient, airport_code: &str, frame_id: &usize) -> Option<HashSet<String>> {
        let query = format!(
            "SELECT \"flightCode\" FROM aviation.flightsByAirport WHERE \"airportCode\" = '{}'",
            airport_code
        );
        let values = client.execute_strong_select_query(&query, frame_id).ok()?;
        self.values_to_flight_codes(&values)
    }

    // Transforms the values to flight codes
    fn values_to_flight_codes(&self, flight_codes: &Vec<HashMap<String, String>>) -> Option<HashSet<String>> {
        let mut codes = HashSet::new();
        for row in flight_codes {
            if let Some(code) = row.get("flightCode"){
                codes.insert(code.to_string());
            }
        }
        Some(codes)
    }

    /// Insert a flight in the database
    pub fn insert_single_flight(&self, flight: &Flight, thread_pool: &ThreadPoolClient) {
        let simulator = Self;
        let flight = flight.clone();
        thread_pool.execute(move |frame_id, client| {
            _ = simulator.insert_flight(client, &flight, &frame_id);
        });
    }

    fn insert_flight(&self, client: &mut CassandraClient, flight: &Flight, frame_id: &usize) -> Result<(), String> {
        let query = format!(
            "INSERT INTO aviation.flightInfo (\"flightCode\", status, \"departureAirport\", \"arrivalAirport\", \"departureTime\", \"arrivalTime\", \"positionLat\", \"positionLon\", \"arrivalPositionLat\", \"arrivalPositionLon\", altitude, speed, \"fuelLevel\") VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}');",
            flight.get_code(), 
            flight.get_status().to_string(), 
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
            let simulator = Self; 
            let tx = Arc::clone(&tx);
            thread_pool.execute(move |frame_id, client| {
                if let Some(flight) = simulator.get_flight(client, &code, &frame_id) {
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
            "SELECT \"flightCode, status, \"departureAirport\", \"arrivalAirport\", \"departureTime\", \"arrivalTime\" FROM aviation.flightInfo WHERE \"flightCode\" = '{}';",
            flight_code
        );
        let values = client.execute_strong_select_query(&query, frame_id).ok()?;
        self.values_to_flight_status(&values)
    }
    
    fn values_to_flight_status(&self, values: &Vec<HashMap<String, String>>)-> Option<FlightStatus> {
        let strong_row = values.get(0)?;
        let code = strong_row.get("flightCode")?.to_string();
        let status_str = strong_row.get("status")?;
        let status = FlightState::new(status_str);
        let departure_airport = strong_row.get("departureAirport")?.to_string();
        let arrival_airport = strong_row.get("arrivalAirport")?.to_string();
        let departure_time = strong_row.get("departureTime")?.to_string();
        let arrival_time = strong_row.get("arrivalTime")?.to_string();

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
            "SELECT \"positionLat\", \"positionLon\", \"arrivalPositionLat\", \"arrivalPositionLon\", altitude, speed, \"fuelLevel\" FROM aviation.flightInfo WHERE \"flightCode\" = '{}'",
            flight_code
        );
        let values = client.execute_weak_select_query(&query, frame_id).ok()?;
        self.values_to_flight_tracking(&values)
    }

    fn values_to_flight_tracking(&self, values: &Vec<HashMap<String, String>>)-> Option<FlightTracking> {
        let weak_row = values.get(0)?;

        let position_lat: f64 = weak_row.get("positionLat")?.parse().ok()?;
        let position_lon: f64 = weak_row.get("positionLon")?.parse().ok()?;
        let arrival_position_lat: f64 = weak_row.get("arrivalPositionLat")?.parse().ok()?;
        let arrival_position_lon: f64 = weak_row.get("arrivalPositionLon")?.parse().ok()?;
        let altitude: f64 = weak_row.get("altitude")?.parse().ok()?;
        let speed: f32 = weak_row.get("speed")?.parse().ok()?;
        let fuel_level: f32 = weak_row.get("fuelLevel")?.parse().ok()?;

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
            "UPDATE aviation.flightInfo SET status = '{}', \"departureAirport\" = '{}', \"arrivalAirport\" = '{}', \"departureTime\" = '{}', \"arrivalTime\" = '{}' WHERE \"flightCode\" = '{}';",
            flight.get_status().to_string(), flight.get_departure_airport(), flight.get_arrival_airport(), flight.get_departure_time(), flight.get_arrival_time(),
            flight.get_code()
        );
        client.execute_strong_query_without_response(&query, frame_id)
    }

    fn update_flight_tracking(&self, client: &mut CassandraClient, flight: &Flight, frame_id: &usize) -> Result<(), String> {
        let query = format!(
            "UPDATE aviation.flightInfo SET \"positionLat\" = '{}', \"positionLon\" = '{}', \"arrivalPositionLat\" = '{}', \"arrivalPositionLon\" = '{}', altitude = '{}', speed = '{}', \"fuelLevel\" = '{}' WHERE \"flightCode\" = '{}';",
            flight.get_position().0, flight.get_position().1, flight.get_arrival_position().0, flight.get_arrival_position().1, flight.get_altitude(), flight.get_speed(), flight.get_fuel_level(),
            flight.get_code()
        );
        client.execute_weak_query_without_response(&query, frame_id)
    }

    /// Restarts all the flights in the airport to the initial state
    pub fn restart_flights(&self, flights: &mut Vec<Flight>, airports: &HashMap<String, Airport>, thread_pool: &ThreadPoolClient) {
        for mut flight in flights.clone() {
            let simulator = Self;
            let position = match airports.get(flight.get_departure_airport()) {
                Some(airport) => airport.position,
                None => continue,
            };
            thread_pool.execute(move |frame_id, client| {
                flight.restart(position);
                _ = simulator.update_flight(client, &flight, &frame_id);
            });
        }

        thread_pool.join();
    }

    /// Loop that updates the flights in the airport every interval of time
    pub fn flight_updates_loop(
        &self,
        mut flights: Vec<Flight>,
        airports: &HashMap<String, Airport>,
        step: f32,
        interval: u64,
        thread_pool: &ThreadPoolClient
    ) {
        loop {
            for flight in flights.iter_mut() {
                let arrival_position = match  airports.get(flight.get_arrival_airport()) {
                    Some(airport) => airport.position,
                    None => continue,
                };

                let simulator = Self;
                let mut flight = flight.clone();
                thread_pool.execute(move |frame_id, client| {
                    flight.update_progress(arrival_position, step);
                    _ = simulator.update_flight(client, &flight, &frame_id);
                });
            }
            thread_pool.join();
            thread::sleep(Duration::from_millis(interval));
        }
    }
}