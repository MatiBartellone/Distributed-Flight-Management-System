use std::collections::{HashMap, HashSet};

use crate::{flight_implementation::{flight::{Flight, FlightStatus, FlightTracking}, flight_state::FlightState}, utils::{constants::OP_RESULT, frame::Frame}};

use super::cassandra_client::{CassandraClient, FLAGS, OP_CODE_QUERY, STREAM, VERSION};

pub struct FlightSimulatorClient {
    client: CassandraClient
}

impl FlightSimulatorClient {
    pub fn new(node: &str) -> Result<Self, String> {
        let client = CassandraClient::new(node)?;
        Ok(Self { client })
    }

    pub fn use_aviation_keyspace(&mut self) -> Result<(), String> {
        let frame = self.get_strong_query_frame("USE aviation;")?;
        self.send_frame(&frame.to_bytes()?)?;
        self.read_frame_response()?;
        Ok(())
    }

    // Get the information of the flights
    pub fn get_flights(&mut self, airport_code: &str) -> Vec<Flight> {
        let Some(flight_codes) = self.get_flight_codes_by_airport(airport_code) else {
            return Vec::new();
        };
        flight_codes
            .into_iter()
            .filter_map(|code| self.get_flight(&code))
            .collect()
    }

    // Get the information of the flight
    pub fn get_flight(&mut self, flight_code: &str) -> Option<Flight> {
        let flight_status = self.get_flight_status(flight_code)?;
        let flight_tracking = self.get_flight_tracking(flight_code)?;
        Some(Flight {
            info: flight_tracking,
            status: flight_status
        })
    }

    fn get_flight_status(&mut self, flight_code: &str) -> Option<FlightStatus> {
        let query = format!(
            "SELECT flightCode, status, departureAirport, arrivalAirport, departureTime, arrivalTime FROM aviation.flightInfo WHERE flightCode = '{}';",
            flight_code
        );
        let frame = self.get_strong_query_frame(&query).ok()?;
        self.send_frame(&frame.to_bytes().ok()?).ok()?;
        let response = self.get_body_result().ok()?;
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

    fn get_flight_tracking(&mut self, flight_code: &str) -> Option<FlightTracking> {
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
        let frame = self.get_weak_query_frame(&query).ok()?;
        self.send_frame(&frame.to_bytes().ok()?).ok()?;
        let body_weak = self.get_body_result().ok()?;
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

    // Gets all de flights codes going or leaving the aiport
    fn get_flight_codes_by_airport(&mut self, airport_code: &str) -> Option<HashSet<String>> {
        let query = format!(
            "SELECT flightCode FROM aviation.flightsByAirport WHERE airportCode = '{}'",
            airport_code
        );
        let frame = self.get_strong_query_frame(&query).ok()?;
        self.send_frame(&frame.to_bytes().ok()?).ok()?;
        let response = self.get_body_result().ok()?;
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

    pub fn update_flight(&mut self, flight: &Flight) -> Result<(), String> {
        self.update_flight_status(flight)?;
        self.update_flight_tracking(flight)
    }

    fn update_flight_status(&mut self, flight: &Flight) -> Result<(), String> {
        let query = format!(
            "UPDATE aviation.flightInfo SET status = '{}', departureAirport = '{}', arrivalAirport = '{}', departureTime = '{}', arrivalTime = '{}' WHERE flightCode = '{}';",
            flight.get_status().to_string(), flight.get_departure_airport(), flight.get_arrival_airport(), flight.get_departure_time(), flight.get_arrival_time(),
            flight.get_code()
        );
        let frame = self.get_strong_query_frame(&query)?;
        self.send_frame(&frame.to_bytes()?)?;
        self.get_body_result()?;
        Ok(())
    }

    fn update_flight_tracking(&mut self, flight: &Flight) -> Result<(), String> {
        let query = format!(
            "UPDATE aviation.flightInfo SET positionLat = '{}', positionLon = '{}', altitude = '{}', speed = '{}', fuelLevel = '{}' WHERE flightCode = '{}';",
            flight.get_position().0, flight.get_position().1, flight.get_altitude(), flight.get_speed(), flight.get_fuel_level(),
            flight.get_code()
        );
        let frame = self.get_weak_query_frame(&query)?;
        self.send_frame(&frame.to_bytes()?)?;
        self.get_body_result()?;
        Ok(())
    }

    fn get_body_result(&mut self) -> Result<Vec<u8>, String> {
        let frame = self.read_frame_response()?;
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
    pub fn inicializate(&mut self) -> Result<(), String> {
        self.client.inicializate()
    }

    fn get_body_query_strong(&self, query: &str) -> Result<Vec<u8>, String> {
        self.client.get_body_query_strong(query)
    }

    fn get_body_query_weak(&self, query: &str) -> Result<Vec<u8>, String> {
        self.client.get_body_query_weak(query)
    }

    fn send_frame(&mut self, frame: &[u8]) -> Result<(), String> {
        self.client.send_frame(frame)
    }

    fn read_frame_response(&mut self) -> Result<Frame, String> {
        self.client.read_frame_response()
    }
    
}
