use std::{collections::{HashMap, HashSet}, sync::{mpsc::{self}, Arc, Mutex}, thread, time::Duration};

use crate::{flight_implementation::{airport::Airport, flight::{Flight, FlightStatus, FlightTracking}, flight_state::FlightState}, utils::{constants::OP_RESULT, frame::Frame}};

use super::{cassandra_client::{CassandraClient, FLAGS, OP_CODE_QUERY, STREAM, VERSION}, thread_pool_client::ThreadPoolClient};

#[derive(Clone)]
pub struct FlightSimulatorClient;

impl FlightSimulatorClient {
    /// Use the aviation keyspace in the cassandra database
    pub fn use_aviation_keyspace(&self, client: &mut CassandraClient) -> Result<(), String> {
        let frame_id = STREAM as usize;
        let mut frame = self.get_strong_query_frame(client, "USE aviation;", &frame_id)?;
        let rx = client.send_frame(&mut frame)?;
        client.read_frame_response()?;
        let _ = rx.recv().map_err(|_| "Error receiving the response".to_string())?;
        Ok(())
    }

    fn get_airports(&self, airports_codes: Vec<String>, thread_pool: &ThreadPoolClient) -> HashMap<String, Airport> {
        let (tx, rx) = mpsc::channel();
        let tx = Arc::new(Mutex::new(tx));
        for code in airports_codes {
            let simulator = self.clone(); 
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

    fn get_codes(
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
        match rx.recv() {
            Ok(flight_codes) => flight_codes,
            Err(_) => HashSet::new()
        }
    }

    // Gets all de flights codes going or leaving the aiport
    fn get_flight_codes_by_airport(&self, client: &mut CassandraClient, airport_code: &str, frame_id: &usize) -> Option<HashSet<String>> {
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
            let simulator = self.clone(); 
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
            "SELECT flightCode, status, departureAirport, arrivalAirport, departureTime, arrivalTime FROM aviation.flightInfo WHERE flightCode = '{}';",
            flight_code
        );
        let mut frame = self.get_strong_query_frame(client, &query, frame_id).ok()?;
        let response = self.get_body_frame_response(client, &mut frame).ok()?;
        self.extract_flight_status(&response, vec![
            "flightCode".to_string(),
            "status".to_string(),
            "departureAirport".to_string(),
            "arrivalAirport".to_string(),
            "departureTime".to_string(),
            "arrivalTime".to_string(),
        ])
    }
    
    fn extract_flight_status(
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

    fn get_flight_tracking(&self, client: &mut CassandraClient, flight_code: &str, frame_id: &usize) -> Option<FlightTracking> {
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
        self.extract_flight_tracking(&response, header_weak)
    }

    fn extract_flight_tracking(
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

    /// Update the flight information in the database with the new information
    pub fn update_flight(&self, client: &mut CassandraClient, flight: &Flight, frame_id: &usize) -> Result<(), String> {
        self.update_flight_status(client, flight, frame_id)?;
        self.update_flight_tracking(client, flight, frame_id)
    }

    fn update_flight_status(&self, client: &mut CassandraClient, flight: &Flight, frame_id: &usize) -> Result<(), String> {
        let query = format!(
            "UPDATE aviation.flightInfo SET status = '{}', departureAirport = '{}', arrivalAirport = '{}', departureTime = '{}', arrivalTime = '{}' WHERE flightCode = '{}';",
            flight.get_status().to_string(), flight.get_departure_airport(), flight.get_arrival_airport(), flight.get_departure_time(), flight.get_arrival_time(),
            flight.get_code()
        );
        let mut frame = self.get_strong_query_frame(client, &query, frame_id)?;
        let _ = self.get_body_frame_response(client, &mut frame)?;
        Ok(())
    }

    fn update_flight_tracking(&self, client: &mut CassandraClient, flight: &Flight, frame_id: &usize) -> Result<(), String> {
        let query = format!(
            "UPDATE aviation.flightInfo SET positionLat = '{}', positionLon = '{}', altitude = '{}', speed = '{}', fuelLevel = '{}' WHERE flightCode = '{}';",
            flight.get_position().0, flight.get_position().1, flight.get_altitude(), flight.get_speed(), flight.get_fuel_level(),
            flight.get_code()
        );
        let mut frame = self.get_weak_query_frame(client, &query, frame_id)?;
        let _ = self.get_body_frame_response(client, &mut frame)?;
        Ok(())
    }

    /// Restarts all the flights in the airport to the initial state
    pub fn restart_flights(&self, airport_code: &str, thread_pool: &ThreadPoolClient) {
        let flights = self.get_flights(airport_code, &thread_pool);

        for mut flight in flights {
            let simulator = self.clone(); 
            thread_pool.execute(move |frame_id, client| {
                flight.restart((0.0, 0.0));
                _ = simulator.update_flight(client, &flight, &frame_id);
            });
        }

        thread_pool.join();
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

    /// Loop that updates the flights in the airport every interval of time
    pub fn flight_updates_loop(
        &self,
        airport_code: &str,
        step: f32,
        interval: u64,
        thread_pool: &ThreadPoolClient
    ) {
        let codes = FlightSimulatorClient::get_airports_codes();
        let airports = self.get_airports(codes, &thread_pool);
        loop {
            for mut flight in self.get_flights(airport_code, &thread_pool) {
                let arrival_position = match  airports.get(flight.get_arrival_airport()) {
                    Some(airport) => airport.position,
                    None => continue,
                };

                let simulator = self.clone();
                thread_pool.execute(move |frame_id, client| {
                    flight.update_progress(arrival_position, step);
                    _ = simulator.update_flight(client, &flight, &frame_id);
                });
            }
            thread_pool.join();
            thread::sleep(Duration::from_millis(interval));
        }
    }

    // Get the result of the query
    fn get_body_result(&self, frame: Frame) -> Result<Vec<u8>, String> {
        if frame.opcode != OP_RESULT {
            return Err("Error reading the frame".to_string());
        }
        Ok(frame.body)
    }

    fn get_strong_query_frame(&self, client: &mut CassandraClient, query: &str, frame_id: &usize) -> Result<Frame, String> {
        let body = client.get_body_query_strong(query)?;
        self.get_query_frame(&body, frame_id)
    }

    fn get_weak_query_frame(&self, client: &mut CassandraClient, query: &str, frame_id: &usize) -> Result<Frame, String> {
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

    fn get_body_frame_response(&self, client: &mut CassandraClient, frame: &mut Frame) -> Result<Vec<u8>, String> {
        let frame_response = client.send_and_receive(frame)?;
        // let rx = client.send_frame(frame)?;
        // client.read_frame_response()?;
        // let frame_response = rx.recv().unwrap();

        let body_response = self.get_body_result(frame_response)?;
        Ok(body_response)
    }
}