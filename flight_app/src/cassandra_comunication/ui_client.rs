use std::{collections::{HashMap, HashSet}, sync::{mpsc::{self}, Arc, Mutex}};

use crate::{airport_implementation::airport::Airport, flight_implementation::{flight::Flight, flight_selected::{FlightSelected, FlightStatus, FlightTracking}, flight_state::FlightState}};

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
const COL_FUEL_LEVEL: &str = "fuel_evel";

pub struct UIClient;

impl UIClient {
    /// Use the aviation keyspace in the cassandra database
    pub fn use_aviation_keyspace(&self, client: &mut CassandraClient) -> Result<(), String> {
        let frame_id = STREAM as usize;
        let query = format!("USE {};", KEYSPACE_AVIATION);
        client.execute_strong_query_without_response(&query, &frame_id)
    }

    /// Get the information of the airports
    pub fn get_airports(&self, airports_codes: Vec<String>, thread_pool: &ThreadPoolClient) -> Vec<Airport> {
        let (tx, rx) = mpsc::channel();
        let tx = Arc::new(Mutex::new(tx));
        for code in airports_codes {
            let ui_client = Self; 
            let tx = Arc::clone(&tx);
            thread_pool.execute(move |frame_id, client| {
                if let Some(airport) = ui_client.get_airport(client, &code, &frame_id) {
                    if let Ok(tx) = tx.lock(){
                        let _ = tx.send(airport);
                    }
                }
            });
        }
        thread_pool.join();
        drop(tx);
        rx.into_iter().collect()
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
    fn values_to_airport(&self, values: &Vec<HashMap<String, String>>) -> Option<Airport> {
        let row = values.get(0)?;
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
        let ui_client = Self; 
        thread_pool.execute(move |frame_id, client| {
            if let Some(flight_codes) = ui_client.get_flight_codes_by_airport(client, &airport_code, &frame_id) {
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

    /// Get the basic information of the flights
    pub fn get_flights(
        &self,
        airport_code: &str,
        thread_pool: &ThreadPoolClient
    ) -> Vec<Flight> {
        let flight_codes = self.get_codes(airport_code, thread_pool);
    
        let (tx, rx) = mpsc::channel();
        let tx = Arc::new(Mutex::new(tx));
        for code in flight_codes {
            let ui_client = Self; 
            let tx = Arc::clone(&tx);
            thread_pool.execute(move |frame_id, client| {
                if let Some(flight) = ui_client.get_flight(client, &code, &frame_id) {
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

    fn get_flight(&self, client: &mut CassandraClient, flight_code: &str, frame_id: &usize) -> Option<Flight> {
        let mut flight = Flight::default();
        flight.code = flight_code.to_string();
        self.get_flight_status(client, &mut flight, frame_id)?;
        self.get_flight_tracking(client, &mut flight, frame_id)?;
        Some(flight)
    }

    fn get_flight_status(&self, client: &mut CassandraClient, flight: &mut Flight, frame_id: &usize) -> Option<()> {
        let query = format!(
            "SELECT {}, {}, {} FROM {} WHERE {} = '{}';",
            COL_FLIGHT_CODE, COL_STATUS, COL_ARRIVAL_AIRPORT,
            TABLE_FLIGHT_INFO, COL_FLIGHT_CODE, flight.code
        );
        let values = client.execute_strong_select_query(&query, frame_id).ok()?;
        self.values_to_flight_status(&values, flight)
    }

    fn values_to_flight_status(&self, values: &Vec<HashMap<String, String>>, flight: &mut Flight)-> Option<()> {
        let strong_row = values.get(0)?;

        flight.code = strong_row
            .get(COL_FLIGHT_CODE)?
            .to_string();
            
        let status_str = strong_row.get(COL_STATUS)?;
        flight.status = FlightState::new(status_str);
        
        flight.arrival_airport = strong_row
            .get(COL_ARRIVAL_AIRPORT)?
            .to_string();

        Some(())
    }

    fn get_flight_tracking(&self, client: &mut CassandraClient, flight: &mut Flight, frame_id: &usize) -> Option<()> {
        let query = format!(
            "SELECT {}, {}, {}, {} FROM {} WHERE {} = '{}';",
            COL_POSITION_LAT, COL_POSITION_LON, COL_ARRIVAL_POSITION_LAT, COL_ARRIVAL_POSITION_LON,
            TABLE_FLIGHT_INFO, COL_FLIGHT_CODE, flight.code
        );
        let values = client.execute_weak_select_query(&query, frame_id).ok()?;
        self.values_to_flight_tracking(&values, flight)
    }

    fn values_to_flight_tracking(&self, values: &Vec<HashMap<String, String>>, flight: &mut Flight)-> Option<()> {
        let weak_row = values.get(0)?;

        flight.position.0 = weak_row
            .get(COL_POSITION_LAT)?
            .parse().ok()?;
        
        flight.position.1 = weak_row
            .get(COL_POSITION_LON)?
            .parse().ok()?;

        flight.arrival_position.0 = weak_row
            .get(COL_ARRIVAL_POSITION_LAT)?
            .parse().ok()?;
        
        flight.arrival_position.1 = weak_row
            .get(COL_ARRIVAL_POSITION_LON)?
            .parse().ok()?;
        
        Some(())
    }

    // Get the complete information of the flight
    pub fn get_flight_selected(&self, flight_code: &str, thread_pool: &ThreadPoolClient) -> Option<FlightSelected> {
        let (tx, rx) = mpsc::channel();
        let flight_code = flight_code.to_string();
        let ui_client = Self; 
        thread_pool.execute(move |frame_id, client| {
            let flight_status = ui_client.get_flight_selected_status(client, &flight_code, &frame_id);
            let flight_tracking = ui_client.get_flight_selected_tracking(client, &flight_code, &frame_id);
            if let (Some(status), Some(tracking)) = (flight_status, flight_tracking) {
                tx.send(Some(FlightSelected {
                    status,
                    info: tracking,
                })).expect("Failed to send flight data");
            } else {
                tx.send(None).expect("Failed to send None for missing flight data");
            }
        });
    
        thread_pool.join();
        rx.recv().unwrap()
    }

    fn get_flight_selected_status(&self, client: &mut CassandraClient, flight_code: &str, frame_id: &usize) -> Option<FlightStatus> {
        let query = format!(
            "SELECT {}, {}, {}, {}, {}, {} FROM {} WHERE {} = '{}';",
            COL_FLIGHT_CODE, COL_STATUS, COL_DEPARTURE_AIRPORT, COL_ARRIVAL_AIRPORT, COL_DEPARTURE_TIME, COL_ARRIVAL_TIME,
            TABLE_FLIGHT_INFO, COL_FLIGHT_CODE, flight_code
        );
        let values = client.execute_strong_select_query(&query, frame_id).ok()?;
        self.values_to_flight_selected_status(&values)
    }
    
    fn values_to_flight_selected_status(&self, values: &Vec<HashMap<String, String>>)-> Option<FlightStatus> {
        let strong_row = values.get(0)?;                
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

    fn get_flight_selected_tracking(&self, client: &mut CassandraClient, flight_code: &str, frame_id: &usize) -> Option<FlightTracking> {
        let query = format!(
            "SELECT {}, {}, {}, {}, {}, {}, {} FROM {} WHERE {} = '{}';",
            COL_POSITION_LAT, COL_POSITION_LON, COL_ARRIVAL_POSITION_LAT, COL_ARRIVAL_POSITION_LON,
            COL_ALTITUDE, COL_SPEED, COL_FUEL_LEVEL,
            TABLE_FLIGHT_INFO, COL_FLIGHT_CODE, flight_code
        );
        let values = client.execute_weak_select_query(&query, frame_id).ok()?;
        self.values_to_flight_selected_tracking(&values)
    }

    fn values_to_flight_selected_tracking(&self, values: &Vec<HashMap<String, String>>)-> Option<FlightTracking> {
        let weak_row = values.get(0)?;
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
}