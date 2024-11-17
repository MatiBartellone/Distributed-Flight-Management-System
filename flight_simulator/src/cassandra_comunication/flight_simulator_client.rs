use std::{collections::{HashMap, HashSet}, sync::{mpsc::{self, Receiver}, Arc, Mutex}, thread, time::Duration};

use crate::{flight_implementation::{flight::{Flight, FlightStatus, FlightTracking}, flight_state::FlightState}, thread_pool::thread_pool::ThreadPool, utils::{constants::OP_RESULT, frame::Frame}};

use super::cassandra_client::{CassandraClient, FLAGS, OP_CODE_QUERY, STREAM, VERSION};

#[derive(Clone)]
pub struct FlightSimulatorClient {
    client: CassandraClient
}

impl FlightSimulatorClient {
    pub fn new(node: &str) -> Result<Self, String> {
        let client = CassandraClient::new(node)?;
        Ok(Self { client })
    }

    pub fn use_aviation_keyspace(&self) -> Result<(), String> {
        let mut frame = self.get_strong_query_frame("USE aviation;")?;
        let frame_id = STREAM as usize;
        self.send_frame(&mut frame, frame_id)?;
        self.read_frame_response()?;
        Ok(())
    }

    pub fn get_codes(
        &self,
        airport_code: &str,
        thread_pool: &ThreadPool
    ) -> HashSet<String> {
        let (tx, rx) = mpsc::channel();
        let airport_code = airport_code.to_string();
        let simulator = self.clone(); 
        thread_pool.execute(move |frame_id| {
            if let Some(flight_codes) = simulator.get_flight_codes_by_airport(&airport_code, &frame_id) {
                tx.send(flight_codes).expect("Error sending the flight codes");
            } else {
                tx.send(HashSet::new()).expect("Error sending the flight codes");
            }
        });
    
        thread_pool.wait();
        rx.recv().unwrap()
    }

    // Gets all de flights codes going or leaving the aiport
    pub fn get_flight_codes_by_airport(&self, airport_code: &str, frame_id: &usize) -> Option<HashSet<String>> {
        let query = format!(
            "SELECT flightCode FROM aviation.flightsByAirport WHERE airportCode = '{}'",
            airport_code
        );
        let mut frame = self.get_strong_query_frame(&query).ok()?;
        let rx = self.send_frame(&mut frame, *frame_id).ok()?;
        let response = self.get_body_result(rx).ok()?;
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
        thread_pool: &ThreadPool
    ) -> Vec<Flight> {
        let flight_codes = self.get_codes(airport_code, thread_pool);
    
        let (tx, rx) = mpsc::channel();
        let tx = Arc::new(Mutex::new(tx));
        for code in flight_codes {
            let simulator = self.clone(); 
            let tx = Arc::clone(&tx);
            thread_pool.execute(move |frame_id| {
                if let Some(flight) = simulator.get_flight(&code, &frame_id) {
                    if let Err(e) = tx.lock().unwrap().send(flight) {
                        eprintln!("Error sending flight: {}", e);
                    }
                }
            });
        }
    
        thread_pool.wait();
        drop(tx);
        rx.into_iter().collect()
    }

    // Get the information of the flight
    fn get_flight(&self, flight_code: &str, frame_id: &usize) -> Option<Flight> {
        let flight_status = self.get_flight_status(flight_code, frame_id)?;
        let flight_tracking = self.get_flight_tracking(flight_code, frame_id)?;
        Some(Flight {
            info: flight_tracking,
            status: flight_status
        })
    }

    fn get_flight_status(&self, flight_code: &str, frame_id: &usize) -> Option<FlightStatus> {
        let query = format!(
            "SELECT flightCode, status, departureAirport, arrivalAirport, departureTime, arrivalTime FROM aviation.flightInfo WHERE flightCode = '{}';",
            flight_code
        );
        let mut frame = self.get_strong_query_frame(&query).ok()?;
        let rx = self.send_frame(&mut frame, *frame_id).ok()?;
        let response = self.get_body_result(rx).ok()?;
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

    fn get_flight_tracking(&self, flight_code: &str, frame_id: &usize) -> Option<FlightTracking> {
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
        let mut frame = self.get_weak_query_frame(&query).ok()?;
        let rx = self.send_frame(&mut frame, *frame_id).ok()?;
        let body_weak = self.get_body_result(rx).ok()?;
        self.extract_flight_tracking(&body_weak, header_weak)
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

    pub fn update_flight(&self, flight: &Flight, frame_id: &usize) -> Result<(), String> {
        self.update_flight_status(flight, frame_id)?;
        self.update_flight_tracking(flight, frame_id)
    }

    fn update_flight_status(&self, flight: &Flight, frame_id: &usize) -> Result<(), String> {
        let query = format!(
            "UPDATE aviation.flightInfo SET status = '{}', departureAirport = '{}', arrivalAirport = '{}', departureTime = '{}', arrivalTime = '{}' WHERE flightCode = '{}';",
            flight.get_status().to_string(), flight.get_departure_airport(), flight.get_arrival_airport(), flight.get_departure_time(), flight.get_arrival_time(),
            flight.get_code()
        );
        let mut frame = self.get_strong_query_frame(&query)?;
        let rx = self.send_frame(&mut frame, *frame_id)?;
        let _ = self.get_body_result(rx)?;
        Ok(())
    }

    fn update_flight_tracking(&self, flight: &Flight, frame_id: &usize) -> Result<(), String> {
        let query = format!(
            "UPDATE aviation.flightInfo SET positionLat = '{}', positionLon = '{}', altitude = '{}', speed = '{}', fuelLevel = '{}' WHERE flightCode = '{}';",
            flight.get_position().0, flight.get_position().1, flight.get_altitude(), flight.get_speed(), flight.get_fuel_level(),
            flight.get_code()
        );
        let mut frame = self.get_weak_query_frame(&query)?;
        let rx = self.send_frame(&mut frame, *frame_id)?;
        let _ = self.get_body_result(rx)?;
        Ok(())
    }

    // Restarts all the flights in the airport
    pub fn restart_flights(&self, airport_code: &str, thread_pool: &ThreadPool) {
        let flights = self.get_flights(airport_code, &thread_pool);

        for mut flight in flights {
            let simulator = self.clone(); 
            thread_pool.execute(move |frame_id| {
                flight.restart((0.0, 0.0));
                _ = simulator.update_flight(&flight, &frame_id);
            });
        }

        thread_pool.wait();
    }

    // Updates the flights in the simulator
    pub fn flight_updates_loop(
        &self,
        airport_code: &str,
        step: f32,
        interval: u64,
        thread_pool: &ThreadPool
    ) {
        loop {
            for mut flight in self.get_flights(airport_code, &thread_pool) {
                let simulator = self.clone();
                thread_pool.execute(move |frame_id| {
                    flight.update_progress(step);
                    _ = simulator.update_flight(&flight, &frame_id);
                });
            }
            thread_pool.wait();
            thread::sleep(Duration::from_millis(interval));
        }
    }

    // Get the result of the query
    fn get_body_result(&self, rx: Receiver<Frame>) -> Result<Vec<u8>, String> {
        let _ = self.read_frame_response()?;
        let frame = rx.recv().unwrap();
        if frame.opcode != OP_RESULT {
            return Err("Error reading the frame".to_string());
        }
        Ok(frame.body)
    }

    fn get_strong_query_frame(&self, query: &str) -> Result<Frame, String> {
        let body = self.get_body_query_strong(query)?;
        self.get_query_frame(&body)
    }

    fn get_weak_query_frame(&self, query: &str) -> Result<Frame, String> {
        let body = self.get_body_query_weak(query)?;
        self.get_query_frame(&body)
    }

    fn get_query_frame(&self, body: &[u8]) -> Result<Frame, String> {
        Ok(Frame::new(
            VERSION,
            FLAGS,
            STREAM,
            OP_CODE_QUERY,
            body.len() as u32,
            body.to_vec(),
        ))
    }

    // Wrap functions of CassandraClient
    pub fn inicializate(&self) -> Result<(), String> {
        self.client.inicializate()
    }

    fn get_body_query_strong(&self, query: &str) -> Result<Vec<u8>, String> {
        self.client.get_body_query_strong(query)
    }

    fn get_body_query_weak(&self, query: &str) -> Result<Vec<u8>, String> {
        self.client.get_body_query_weak(query)
    }

    fn send_frame(&self, frame: &mut Frame, frame_id: usize) -> Result<Receiver<Frame>, String> {
        self.client.send_frame(frame, &frame_id)
    }

    fn read_frame_response(&self) -> Result<(), String> {
        self.client.read_frame_response()
    }
}