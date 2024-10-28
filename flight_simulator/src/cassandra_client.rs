use std::{
    collections::{HashMap, HashSet},
    io::{Read, Write},
    net::TcpStream,
};

use crate::{
    flight_implementation::{flight::Flight, flight_status::FlightStatus},
    utils::{
        consistency_level::ConsistencyLevel,
        constants::{ERROR_WRITE, FLUSH_ERROR},
        frame::Frame,
        types_to_bytes::TypesToBytes,
    },
};

pub const VERSION: u8 = 3;
pub const FLAGS: u8 = 0;
pub const STREAM: i16 = 4;
pub const OP_CODE_QUERY: u8 = 7;
pub const OP_CODE_START: u8 = 1;

pub struct CassandraClient {
    stream: TcpStream,
}

impl CassandraClient {
    pub fn new(node: &str) -> Result<Self, String> {
        match TcpStream::connect(node) {
            Ok(stream) => Ok(Self { stream }),
            Err(e) => Err(format!("Failed to connect to node {}: {}", node, e)),
        }
    }

    // Get ready the client for use in keyspace airport
    pub fn inicializate(&mut self) -> Result<(), String> {
        self.start_up()?;
        self.read_frame_response()
    }

    // Send a startup
    fn start_up(&mut self) -> Result<(), String> {
        let body = self.get_start_up_body()?;
        let frame = Frame::new(
            VERSION,
            FLAGS,
            STREAM,
            OP_CODE_START,
            body.len() as u32,
            body,
        );
        self.send_frame(&frame.to_bytes()?)
    }

    fn get_start_up_body(&self) -> Result<Vec<u8>, String> {
        let mut types_to_bytes = TypesToBytes::default();
        let mut options_map = HashMap::new();
        options_map.insert("CQL_VERSION".to_string(), "3.0.0".to_string());
        types_to_bytes.write_string_map(&options_map)?;
        Ok(types_to_bytes.into_bytes())
    }

    // Send the authentication until it success
    fn authenticate_response(&mut self) -> Result<(), String> {
        let auth_response_bytes = vec![
            0x03, 0x00, 0x00, 0x01, 0x0F, 0x00, 0x00, 0x00, 0x12, 0x00, 0x00, 0x00, 0x0E, b'a',
            b'd', b'm', b'i', b'n', b':', b'p', b'a', b's', b's', b'w', b'o', b'r', b'd',
        ];
        self.send_frame(&auth_response_bytes)?;
        self.read_frame_response()
    }

    // Get a query body with consistency
    fn get_body_query(
        &self,
        query: &str,
        consistency_level: ConsistencyLevel,
    ) -> Result<Vec<u8>, String> {
        let mut types_to_bytes = TypesToBytes::default();
        types_to_bytes.write_long_string(query)?;
        types_to_bytes.write_consistency(consistency_level)?;
        Ok(types_to_bytes.into_bytes())
    }

    fn get_body_query_strong(&self, query: &str) -> Result<Vec<u8>, String> {
        self.get_body_query(query, ConsistencyLevel::Quorum)
    }

    fn get_body_query_weak(&self, query: &str) -> Result<Vec<u8>, String> {
        self.get_body_query(query, ConsistencyLevel::One)
    }

    fn send_frame(&mut self, frame: &[u8]) -> Result<(), String> {
        self.stream.write_all(frame).map_err(|_| ERROR_WRITE)?;
        self.stream.flush().map_err(|_| FLUSH_ERROR)?;
        Ok(())
    }

    // Handles the read frame
    fn read_frame_response(&mut self) -> Result<(), String> {
        let mut buf = [0; 1024];
        match self.stream.read(&mut buf) {
            Ok(n) if n > 0 => {
                let frame = Frame::parse_frame(&buf[..n])?;
                self.handle_frame(frame)
            }
            _ => Err("Fail reading the response".to_string()),
        }
    }

    fn handle_frame(&mut self, frame: Frame) -> Result<(), String> {
        match frame.opcode {
            0x0E | 0x03 => self.authenticate_response(),
            _ => Ok(()),
        }
    }
}

// Manejo de queries especificas de la app
impl CassandraClient {
    pub fn use_aviation_keyspace(&mut self) -> Result<(), String> {
        let body = self.get_body_query_strong("USE aviation;")?;
        let frame = Frame::new(
            VERSION,
            FLAGS,
            STREAM,
            OP_CODE_QUERY,
            body.len() as u32,
            body,
        );
        self.send_frame(&frame.to_bytes()?)?;
        self.read_frame_response()?;
        Ok(())
    }

    // Get the basic information of the flights
    pub fn get_flights(&mut self, airport_code: &str) -> Vec<Flight> {
        let Some(flight_codes) = self.get_flight_codes_by_airport(airport_code) else {
            return Vec::new();
        };
        flight_codes
            .into_iter()
            .filter_map(|code| self.get_flight(&code))
            .collect()
    }

    // Get the basic information of the flights
    pub fn get_flight(&mut self, flight_code: &str) -> Option<Flight> {
        // Pide la strong information
        let strong_query = format!(
            "SELECT flightCode, status, departureAirport, arrivalAirport, departureTime, arrivalTime FROM aviation.flightInfo WHERE flightCode = '{}';",
            flight_code
        );
        let header_strong = vec![
            "flightCode".to_string(),
            "status".to_string(),
            "departureAirport".to_string(),
            "arrivalAirport".to_string(),
            "departureTime".to_string(),
            "arrivalTime".to_string(),
        ];
        let body = self.get_body_query_strong(&strong_query).ok()?;
        let frame = Frame::new(
            VERSION,
            FLAGS,
            STREAM,
            OP_CODE_QUERY,
            body.len() as u32,
            body,
        );
        self.send_frame(&frame.to_bytes().ok()?).ok()?;
        let body_strong = self.get_body_response().ok()?;

        // Pide la weak information
        let weak_query = format!(
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
        let body = self.get_body_query_weak(&weak_query).ok()?;
        let frame = Frame::new(
            VERSION,
            FLAGS,
            STREAM,
            OP_CODE_QUERY,
            body.len() as u32,
            body,
        );
        self.send_frame(&frame.to_bytes().ok()?).ok()?;
        let body_weak = self.get_body_response().ok()?;

        self.row_to_flight(&body_strong, header_strong, &body_weak, header_weak)
    }

    // Transforms the rows to flight
    fn row_to_flight(
        &self,
        row_strong: &[u8],
        header_strong: Vec<String>,
        row_weak: &[u8],
        header_weak: Vec<String>,
    ) -> Option<Flight> {
        let strong_row = self
            .get_rows(row_strong, header_strong)?
            .into_iter()
            .next()?;
        let weak_row = self.get_rows(row_weak, header_weak)?.into_iter().next()?;

        // Strong Consistency
        let code = strong_row.get("flightCode")?.to_string();
        let status_str = strong_row.get("status")?;
        let status = FlightStatus::new(status_str);
        let departure_airport = strong_row.get("departureAirport")?.to_string();
        let arrival_airport = strong_row.get("arrivalAirport")?.to_string();
        let departure_time = strong_row.get("departureTime")?.to_string();
        let arrival_time = strong_row.get("arrivalTime")?.to_string();

        // Weak Consistency
        let position_lat: f64 = weak_row.get("positionLon")?.parse().ok()?;
        let position_lon: f64 = weak_row.get("positionLat")?.parse().ok()?;
        let altitude: f64 = weak_row.get("altitude")?.parse().ok()?;
        let speed: f32 = weak_row.get("speed")?.parse().ok()?;
        let fuel_level: f32 = weak_row.get("fuelLevel")?.parse().ok()?;

        Some(Flight {
            code,
            status,
            departure_airport,
            arrival_airport,
            departure_time,
            arrival_time,
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
        let body = self.get_body_query_strong(&query).ok()?;
        let frame = Frame::new(
            VERSION,
            FLAGS,
            STREAM,
            OP_CODE_QUERY,
            body.len() as u32,
            body,
        );
        self.send_frame(&frame.to_bytes().ok()?).ok()?;
        let response = self.get_body_response().ok()?;
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

    pub fn update_flight(&mut self, flight: Flight) -> Result<(), String> {
        // Actualiza la strong information
        let strong_query = format!(
            "UPDATE aviation.flightInfo SET status = '{}' WHERE flightCode = '{}';",
            "OnTime", flight.code
        );
        let body = self.get_body_query_strong(&strong_query)?;
        let frame = Frame::new(
            VERSION,
            FLAGS,
            STREAM,
            OP_CODE_QUERY,
            body.len() as u32,
            body,
        );
        self.send_frame(&frame.to_bytes()?)?;
        self.get_body_response()?;

        // Pide la weak information
        let weak_query = format!(
            "UPDATE aviation.flightInfo SET positionLon = '{}', positionLat = '{}', altitude = '{}', speed = '{}', fuelLevel = '{}', WHERE flightCode = '{}'",
            flight.position.0, flight.position.1, flight.altitude, flight.speed,flight.fuel_level,
            flight.code
        );
        let body = self.get_body_query_weak(&weak_query)?;
        let frame = Frame::new(
            VERSION,
            FLAGS,
            STREAM,
            OP_CODE_QUERY,
            body.len() as u32,
            body,
        );
        self.send_frame(&frame.to_bytes()?)?;
        self.get_body_response()?;
        Ok(())
    }

    fn get_body_response(&mut self) -> Result<Vec<u8>, String> {
        let mut buf = [0; 1024];
        match self.stream.read(&mut buf) {
            Ok(n) if n > 0 => {
                let frame = Frame::parse_frame(&buf[..n])?;
                Ok(frame.body)
            }
            Ok(_) | Err(_) => Ok(Vec::new()),
        }
    }
}
