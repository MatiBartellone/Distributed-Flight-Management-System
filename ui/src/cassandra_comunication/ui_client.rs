use std::{collections::{HashMap, HashSet}, sync::{mpsc::{self}, Arc, Mutex}};

use crate::{airport_implementation::airport::Airport, flight_implementation::{flight::Flight, flight_selected::{FlightSelected, FlightStatus, FlightTracking}, flight_state::FlightState}, utils::{constants::OP_RESULT, frame::Frame}};

use super::{cassandra_client::{CassandraClient, FLAGS, OP_CODE_QUERY, STREAM, VERSION}, thread_pool_client::ThreadPoolClient};

#[derive(Clone)]
pub struct UIClient;

impl UIClient {
    pub fn use_aviation_keyspace(&self, client: &CassandraClient) -> Result<(), String> {
        let frame_id = STREAM as usize;
        let mut frame = self.get_strong_query_frame(client, "USE aviation;", &frame_id)?;
        let rx = client.send_frame(&mut frame)?;
        client.read_frame_response()?;
        rx.recv().unwrap();
        Ok(())
    }

    // Get the information of the airports
    pub fn get_airports(&self, airports_codes: Vec<String>, thread_pool: &ThreadPoolClient) -> Vec<Airport> {
        let (tx, rx) = mpsc::channel();
        let tx = Arc::new(Mutex::new(tx));
        for code in airports_codes {
            let simulator = self.clone(); 
            let tx = Arc::clone(&tx);
            thread_pool.execute(move |frame_id, client| {
                if let Some(flight) = simulator.get_airport(client, &code, &frame_id) {
                    if let Err(e) = tx.lock().unwrap().send(flight) {
                        eprintln!("Error sending airport: {}", e);
                    }
                }
            });
        }
        thread_pool.join();
        drop(tx);
        rx.into_iter().collect()
    }

    pub fn get_airport(&self, client: &CassandraClient, airport_code: &str, frame_id: &usize) -> Option<Airport> {
        let query = format!(
            "SELECT name, positionLat, positionLon, code FROM aviation.airports WHERE code = '{}';",
            airport_code
        );
        let mut frame = self.get_strong_query_frame(client, &query, frame_id).ok()?;
        let response = self.get_body_frame_response(client, &mut frame).ok()?;
        self.row_to_airport(&response, vec![
            "name".to_string(),
            "positionLat".to_string(),
            "positionLon".to_string(),
            "code".to_string(),
        ])
    }

    // Transforms the row to airport
    fn row_to_airport(&self, body: &[u8], header: Vec<String>) -> Option<Airport> {
        let rows = self.get_rows(body, header)?;
        if rows.is_empty() {
            return None;
        }

        let row = &rows[0];
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

    pub fn get_codes(
        &self,
        airport_code: &str,
        thread_pool: &ThreadPoolClient
    ) -> HashSet<String> {
        let (tx, rx) = mpsc::channel();
        let airport_code = airport_code.to_string();
        let simulator = self.clone(); 
        thread_pool.execute(move |frame_id, client| {
            if let Some(flight_codes) = simulator.get_flight_codes_by_airport(client, &airport_code, &frame_id) {
                tx.send(flight_codes).expect("Error sending the flight codes");
            } else {
                tx.send(HashSet::new()).expect("Error sending the flight codes");
            }
        });
    
        thread_pool.join();
        rx.recv().unwrap()
    }

    // Gets all de flights codes going or leaving the aiport
    pub fn get_flight_codes_by_airport(&self, client: &CassandraClient, airport_code: &str, frame_id: &usize) -> Option<HashSet<String>> {
        let query = format!(
            "SELECT flightCode FROM aviation.flightsByAirport WHERE airportCode = '{}'",
            airport_code
        );
        let mut frame = self.get_strong_query_frame(client, &query, frame_id).ok()?;
        let response = self.get_body_frame_response(client, &mut frame).ok()?;
        self.extract_flight_codes(&response, vec!["flightCode".to_string()])
    }

    // Transforms the rows to flight codes
    fn extract_flight_codes(
        &self,
        response: &[u8],
        header: Vec<String>,
    ) -> Option<HashSet<String>> {
        let rows_codes = self.get_rows(response, header)?;
        let mut codes = HashSet::new();

        for row in rows_codes {
            let code = row.get("flightCode").unwrap_or(&String::new()).to_string();
            codes.insert(code);
        }
        Some(codes)
    }

    // Get the information of the flights
    pub fn get_flights(
        &self,
        airport_code: &str,
        thread_pool: &ThreadPoolClient
    ) -> Vec<Flight> {
        let flight_codes = self.get_codes(airport_code, thread_pool);
    
        let (tx, rx) = mpsc::channel();
        let tx = Arc::new(Mutex::new(tx));
        for code in flight_codes {
            let simulator = self.clone(); 
            let tx = Arc::clone(&tx);
            thread_pool.execute(move |frame_id, client| {
                if let Some(flight) = simulator.get_flight(client, &code, &frame_id) {
                    if let Err(e) = tx.lock().unwrap().send(flight) {
                        eprintln!("Error sending flight: {}", e);
                    }
                }
            });
        }
    
        thread_pool.join();
        drop(tx);
        rx.into_iter().collect()
    }

    // Get the basic information of the flight
    pub fn get_flight(&self, client: &CassandraClient, flight_code: &str, frame_id: &usize) -> Option<Flight> {
        let mut flight = Flight::default();
        flight.code = flight_code.to_string();
        self.get_flight_status(client, &mut flight, frame_id)?;
        self.get_flight_tracking(client, &mut flight, frame_id)?;
        Some(flight)
    }

    fn get_flight_status(&self, client: &CassandraClient, flight: &mut Flight, frame_id: &usize) -> Option<()> {
        let query = format!(
            "SELECT flightCode, status, arrivalAirport FROM aviation.flightInfo WHERE flightCode = '{}';",
            flight.code
        );
        let mut frame = self.get_strong_query_frame(client, &query, frame_id).ok()?;
        let response = self.get_body_frame_response(client, &mut frame).ok()?;
        self.extract_flight_status(
            flight,
            &response, 
            vec![
                "flightCode".to_string(),
                "status".to_string(),
                "arrivalAirport".to_string(),
            ]
        )
    }

    fn extract_flight_status(
        &self,
        flight: &mut Flight,
        row_strong: &[u8],
        header_strong: Vec<String>
    ) -> Option<()> {
        let strong_row = self
            .get_rows(row_strong, header_strong)?
            .into_iter()
            .next()?;

        flight.code = strong_row
            .get("flightCode")?
            .to_string();
            
        let status_str = strong_row.get("status")?;
        flight.status = FlightState::new(status_str);
        
        flight.arrival_airport = strong_row
            .get("arrivalAirport")?
            .to_string();

        Some(())
    }

    fn get_flight_tracking(&self, client: &CassandraClient, flight: &mut Flight, frame_id: &usize) -> Option<()> {
        let query = format!(
            "SELECT positionLat, positionLon FROM aviation.flightInfo WHERE flightCode = '{}';",
            flight.code
        );
        let mut frame = self.get_weak_query_frame(client, &query, frame_id).ok()?;
        let response = self.get_body_frame_response(client, &mut frame).ok()?;
        self.extract_flight_tracking(
            flight,
            &response,
            vec!["positionLat".to_string(), "positionLon".to_string()]
        )
    }

    fn extract_flight_tracking(
        &self,
        flight: &mut Flight,
        row_weak: &[u8],
        header_weak: Vec<String>
    ) -> Option<()> {
        let row_weak = self
            .get_rows(row_weak, header_weak)?
            .into_iter()
            .next()?;

        flight.position.0 = row_weak
            .get("positionLat")?
            .parse().ok()?;
        
        flight.position.1 = row_weak
            .get("positionLon")?
            .parse().ok()?;
        
        Some(())
    }

    // Get the complete information of the flight
    pub fn get_flight_selected(&self, flight_code: &str, thread_pool: &ThreadPoolClient) -> Option<FlightSelected> {

        let (tx, rx) = mpsc::channel();
        let flight_code = flight_code.to_string();
        let simulator = self.clone(); 
        thread_pool.execute(move |frame_id, client| {
            let flight_status = simulator.get_flight_selected_status(client, &flight_code, &frame_id);
            let flight_tracking = simulator.get_flight_selected_tracking(client, &flight_code, &frame_id);
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

    fn get_flight_selected_status(&self, client: &CassandraClient, flight_code: &str, frame_id: &usize) -> Option<FlightStatus> {
        let query = format!(
            "SELECT flightCode, status, departureAirport, arrivalAirport, departureTime, arrivalTime FROM aviation.flightInfo WHERE flightCode = '{}';",
            flight_code
        );
        let mut frame = self.get_strong_query_frame(client, &query, frame_id).ok()?;
        let response = self.get_body_frame_response(client, &mut frame).ok()?;
        self.extract_flight_selected_status(&response, vec![
            "flightCode".to_string(),
            "status".to_string(),
            "departureAirport".to_string(),
            "arrivalAirport".to_string(),
            "departureTime".to_string(),
            "arrivalTime".to_string(),
        ])
    }
    
    fn extract_flight_selected_status(
        &self,
        row_strong: &[u8],
        header_strong: Vec<String>
    ) -> Option<FlightStatus> {
        let strong_row = self
            .get_rows(row_strong, header_strong)?
            .into_iter()
            .next()?;

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

    fn get_flight_selected_tracking(&self, client: &CassandraClient, flight_code: &str, frame_id: &usize) -> Option<FlightTracking> {
        let query = format!(
            "SELECT positionLat, positionLon, altitude, speed, fuelLevel FROM aviation.flightInfo WHERE flightCode = '{}'",
            flight_code
        );
        let header_weak = vec![
            "positionLat".to_string(),
            "positionLon".to_string(),
            "altitude".to_string(),
            "speed".to_string(),
            "fuelLevel".to_string(),
        ];
        let mut frame = self.get_weak_query_frame(client, &query, frame_id).ok()?;
        let response = self.get_body_frame_response(client, &mut frame).ok()?;
        self.extract_flight_selected_tracking(&response, header_weak)
    }

    fn extract_flight_selected_tracking(
        &self,
        row_weak: &[u8],
        header_weak: Vec<String>
    ) -> Option<FlightTracking> {
        let weak_row = self.get_rows(row_weak, header_weak)?.into_iter().next()?;

        let position_lat: f64 = weak_row.get("positionLat")?.parse().ok()?;
        let position_lon: f64 = weak_row.get("positionLon")?.parse().ok()?;
        let altitude: f64 = weak_row.get("altitude")?.parse().ok()?;
        let speed: f32 = weak_row.get("speed")?.parse().ok()?;
        let fuel_level: f32 = weak_row.get("fuelLevel")?.parse().ok()?;

        Some(FlightTracking {
            position: (position_lat, position_lon),
            altitude,
            speed,
            fuel_level,
        })
    }

    fn get_rows(&self, body: &[u8], headers: Vec<String>) -> Option<Vec<HashMap<String, String>>> {
        let binding = String::from_utf8(body.to_vec()).ok()?;
        let mut rows = binding.split("\n");

        let mut result = Vec::new();
        rows.next();
        for row in rows {
            let mut row_hash = HashMap::new();
            for (header, value) in headers.iter().zip(row.split(", ")) {
                row_hash.insert(header.to_string(), value.to_string());
            }
            result.push(row_hash);
        }
        result.pop();
        Some(result)
        /*
        let columns_count = cursor.read_int()?;
        let mut column_names = Vec::new();
        for _ in 0..columns_count {
            let column_name = cursor.read_long_string()?;
            column_names.push(column_name);
        }

        let rows_count = cursor.read_u32()?;
        let mut rows = Vec::new();
        for _ in 0..rows_count {
            let mut row = HashMap::new();
            for column_name in &column_names {
                let value = cursor.read_bytes()?.unwrap();
                let string_value = String::from_utf8(value)
                    .map_err(|_| Errors::ProtocolError(String::from("Invalid UTF-8 string")))?;
                row.insert(column_name.to_string(), string_value);
            }
            rows.push(row);
        }
        Ok(rows)*/
    }
    
    // Get the result of the query
    fn get_body_result(&self, frame: Frame) -> Result<Vec<u8>, String> {
        if frame.opcode != OP_RESULT {
            return Err("Error reading the frame".to_string());
        }
        Ok(frame.body)
    }

    fn get_strong_query_frame(&self, client: &CassandraClient, query: &str, frame_id: &usize) -> Result<Frame, String> {
        let body = client.get_body_query_strong(query)?;
        self.get_query_frame(&body, frame_id)
    }

    fn get_weak_query_frame(&self, client: &CassandraClient, query: &str, frame_id: &usize) -> Result<Frame, String> {
        let body = client.get_body_query_weak(query)?;
        self.get_query_frame(&body, frame_id)
    }

    fn get_query_frame(&self, body: &[u8], frame_id: &usize) -> Result<Frame, String> {
        Ok(Frame::new(
            VERSION,
            FLAGS,
            *frame_id as i16,
            OP_CODE_QUERY,
            body.len() as u32,
            body.to_vec(),
        ))
    }

    fn get_body_frame_response(&self, client: &CassandraClient, frame: &mut Frame) -> Result<Vec<u8>, String> {
        let frame_response = client.send_and_receive(frame)?;
        // let rx = client.send_frame(frame)?;
        // client.read_frame_response()?;
        // let frame_response = rx.recv().unwrap();

        let body_response = self.get_body_result(frame_response)?;
        println!("Body response: {:?}", String::from_utf8(body_response.to_vec()));
        Ok(body_response)
    }
}
