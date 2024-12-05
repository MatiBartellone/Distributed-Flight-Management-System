use std::collections::HashMap;

use crate::{airport_implementation::airport::Airport, flight_implementation::{flight::Flight, flight_selected::{FlightSelected, FlightStatus, FlightTracking}, flight_state::FlightState}, utils::query_builder::QueryBuilder};

use super::cassandra_client::{CassandraClient, STREAM};

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

const FRAME_ID : usize = STREAM as usize;

pub struct UIClient{
    client: CassandraClient,
}

impl UIClient {
    pub fn new(client: CassandraClient) -> Self {
        Self {
            client,
        }
    }
    /// Use the aviation keyspace in the cassandra database
    pub fn use_aviation_keyspace(&mut self) -> Result<(), String> {
        let frame_id = STREAM as usize;
        let query = format!("USE {};", KEYSPACE_AVIATION);
        self.client.execute_strong_query_without_response(&query, &frame_id)
    }

    /// Get the information of the airports
    pub fn get_airports(&mut self, airports_codes: Vec<String>) -> HashMap<String, Airport> {
        let mut airports = HashMap::new();
        for airport_code in airports_codes {
            if let Some(airport) = self.get_airport(&airport_code, &FRAME_ID) {
                airports.insert(airport_code, airport);
            }
        }
        airports
    }

    fn get_airport(&mut self, airport_code: &str, frame_id: &usize) -> Option<Airport> {
        let query = QueryBuilder::new("SELECT", TABLE_AIRPORTS)
            .select(vec![COL_NAME, COL_POSITION_LAT, COL_POSITION_LON, COL_CODE])
            .where_condition(&format!("{} = '{}'", COL_CODE, airport_code), None)
            .build();
        let values = self.client.execute_strong_select_query(&query, frame_id).ok()?;
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

    fn get_status_map(&self, status_values: Vec<HashMap<String, String>>) -> HashMap<String, HashMap<String, String>>  {
        status_values
            .into_iter()
            .filter_map(|status| {
                if let Some(code) = status.get(COL_FLIGHT_CODE) {
                    Some((code.to_string(), status))
                } else {
                    None
                }
            })
            .collect()
    }
    
    fn get_tracking_map(&self, tracking_values: Vec<HashMap<String, String>>) -> Vec<(String, HashMap<String, String>)> {
        tracking_values
            .into_iter()
            .filter_map(|mut tracking| {
                if let Some(code) = tracking.remove(COL_FLIGHT_CODE) {
                    Some((code, tracking))
                } else {
                    None
                }
            })
            .collect()
    }

    /// Get the basic information of the flights
    pub fn get_flights(&mut self, airport_code: &str) -> Vec<Flight> {
        let status_values = self.get_flight_status(airport_code, &FRAME_ID);
        let tracking_values = self.get_flight_tracking(airport_code, &FRAME_ID);

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

    fn get_flight(&self, status: &HashMap<String, String>, tracking: &HashMap<String, String>) -> Option<Flight> {
        let mut flight = Flight::default();
        self.values_to_flight_status(status, &mut flight)?;
        self.values_to_flight_tracking(tracking, &mut flight)?;
        Some(flight)
    }

    fn get_flight_status(&mut self, airport_code: &str, frame_id: &usize) -> Vec<HashMap<String, String>> {
        let query = QueryBuilder::new("SELECT", TABLE_FLIGHTS_BY_AIRPORT)
            .select(vec![COL_FLIGHT_CODE, COL_STATUS, COL_ARRIVAL_AIRPORT])
            .where_condition(&format!("{} = '{}'", COL_AIRPORT_CODE, airport_code), None)
            .order_by(COL_FLIGHT_CODE, None)
            .build();
        self.client.execute_strong_select_query(&query, frame_id)
            .unwrap_or_default()
    }

    fn values_to_flight_status(&self, values: &HashMap<String, String>, flight: &mut Flight)-> Option<()> {
        flight.code = values
            .get(COL_FLIGHT_CODE)?
            .to_string();
            
        let status_str = values.get(COL_STATUS)?;
        flight.status = FlightState::new(status_str);
        
        flight.arrival_airport = values
            .get(COL_ARRIVAL_AIRPORT)?
            .to_string();

        Some(())
    }

    fn get_flight_tracking(&mut self, airport_code: &str, frame_id: &usize) -> Vec<HashMap<String, String>> {
        let query = QueryBuilder::new("SELECT", TABLE_FLIGHTS_BY_AIRPORT)
            .select(vec![COL_FLIGHT_CODE, COL_POSITION_LAT, COL_POSITION_LON, COL_ARRIVAL_POSITION_LAT, COL_ARRIVAL_POSITION_LON])
            .where_condition(&format!("{} = '{}'", COL_AIRPORT_CODE, airport_code), None)
            .order_by(COL_FLIGHT_CODE, None)
            .build();
        self.client.execute_weak_select_query(&query, frame_id)
            .unwrap_or_default()
    }

    fn values_to_flight_tracking(&self, values: &HashMap<String, String>, flight: &mut Flight)-> Option<()> {
        flight.position.0 = values
            .get(COL_POSITION_LAT)?
            .parse().ok()?;
        
        flight.position.1 = values
            .get(COL_POSITION_LON)?
            .parse().ok()?;

        flight.arrival_position.0 = values
            .get(COL_ARRIVAL_POSITION_LAT)?
            .parse().ok()?;
        
        flight.arrival_position.1 = values
            .get(COL_ARRIVAL_POSITION_LON)?
            .parse().ok()?;
        
        Some(())
    }

    // Get the complete information of the flight
    pub fn get_flight_selected(&mut self, flight_code: &str, airport_code: &str) -> Option<FlightSelected> {
        let status = self.get_flight_selected_status(flight_code, airport_code, &FRAME_ID)?;
        let tracking = self.get_flight_selected_tracking(flight_code, airport_code, &FRAME_ID)?;
        Some(FlightSelected {
            status,
            tracking,
        })
    }

    fn get_flight_selected_status(&mut self, flight_code: &str, airport_code: &str, frame_id: &usize) -> Option<FlightStatus> {
        let query = QueryBuilder::new("SELECT", TABLE_FLIGHTS_BY_AIRPORT)
            .select(vec![COL_FLIGHT_CODE, COL_STATUS, COL_DEPARTURE_AIRPORT, COL_ARRIVAL_AIRPORT, COL_DEPARTURE_TIME, COL_ARRIVAL_TIME])
            .where_condition(&format!("{} = '{}'", COL_AIRPORT_CODE, airport_code), Some("AND"))
            .where_condition(&format!("{} = '{}'", COL_FLIGHT_CODE, flight_code), None)
            .build();
        let values = self.client.execute_strong_select_query(&query, frame_id).ok()?;
        self.values_to_flight_selected_status(&values)
    }
    
    fn values_to_flight_selected_status(&self, values: &[HashMap<String, String>])-> Option<FlightStatus> {
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

    fn get_flight_selected_tracking(&mut self, flight_code: &str, airport_code: &str, frame_id: &usize) -> Option<FlightTracking> {
        let query = QueryBuilder::new("SELECT", TABLE_FLIGHTS_BY_AIRPORT)
            .select(vec![COL_POSITION_LAT, COL_POSITION_LON, COL_ARRIVAL_POSITION_LAT, COL_ARRIVAL_POSITION_LON, COL_ALTITUDE, COL_SPEED, COL_FUEL_LEVEL])
            .where_condition(&format!("{} = '{}'", COL_AIRPORT_CODE, airport_code), Some("AND"))
            .where_condition(&format!("{} = '{}'", COL_FLIGHT_CODE, flight_code), None)
            .build();
        let values = self.client.execute_weak_select_query(&query, frame_id).ok()?;
        self.values_to_flight_selected_tracking(&values)
    }

    fn values_to_flight_selected_tracking(&self, values: &[HashMap<String, String>])-> Option<FlightTracking> {
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
}