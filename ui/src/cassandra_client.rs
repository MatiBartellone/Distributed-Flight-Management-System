use std::{collections::{HashMap, HashSet}, io::{Read, Write}, net::TcpStream};

use crate::{airport::airport::Airport, flight::{flight::Flight, flight_selected::FlightSelected, flight_status::FlightStatus}, utils::{bytes_cursor::BytesCursor, consistency_level::ConsistencyLevel, errors::Errors, frame::Frame, types_to_bytes::TypesToBytes}};

pub const VERSION: u8 = 3;
pub const FLAGS: u8 = 0;
pub const STREAM: i16 = 4;
pub const OP_CODE_QUERY: u8 = 7;
pub const OP_CODE_START: u8 = 1;

pub struct CassandraClient {
    stream: TcpStream,
}

impl CassandraClient {
    pub fn new(node: &str, port: u16) -> Result<Self, String> {
        let stream = TcpStream::connect((node, port)).map_err(|e| e.to_string())?;
        Ok(Self { stream })
    }

    // Get ready the client
    pub fn inicializate(&mut self){
        self.start_up();
        self.read_frame_response();
    }

    // Send a startup
    fn start_up(&mut self){
        let body = self.get_start_up_body();
        let frame = Frame::new(VERSION, FLAGS, STREAM, OP_CODE_START, body.len() as u32, body);
        self.send_frame(&frame.to_bytes());
    }

    fn get_start_up_body(&self) -> Vec<u8> {
        let mut types_to_bytes = TypesToBytes::new();
        let mut options_map = HashMap::new();
        options_map.insert("CQL_VERSION".to_string(), "3.0.0".to_string());
        types_to_bytes.write_string_map(&options_map);
        types_to_bytes.into_bytes()
    }

    // Send the authentication until it success
    fn authenticate_response(&mut self){
        let auth_response_bytes = vec![
            0x03, 0x00, 0x00, 0x01, 0x0F, 0x00, 0x00, 0x00, 0x12, 0x00, 0x00, 0x00, 0x0E, b'a', b'd',
            b'm', b'i', b'n', b':', b'p', b'a', b's', b's', b'w', b'o', b'r', b'd',
        ];
        self.send_frame(&auth_response_bytes);
        self.read_frame_response(); 
    }

    fn get_body_respones(&mut self) -> Result<Vec<u8>, Errors> {
        let mut buf = [0; 1024];
        match self.stream.read(&mut buf) {
            Ok(n) if n > 0 => {
                let frame = Frame::parse_frame(&buf[..n]).expect("Error parsing frame");
                Ok(frame.body)
            }
            Ok(_) | Err(_) => Ok(Vec::new())
        }
    }

    fn get_body_strong_consistency(&self, query: &str) -> Vec<u8> {
        let mut types_to_bytes = TypesToBytes::new();
        types_to_bytes.write_long_string(query);
        types_to_bytes.write_consistency(ConsistencyLevel::Quorum);
        types_to_bytes.into_bytes()
    }

    fn get_body_weak_consistency(&self, query: &str) -> Vec<u8> {
        let mut types_to_bytes = TypesToBytes::new();
        types_to_bytes.write_long_string(query);
        types_to_bytes.write_consistency(ConsistencyLevel::One);
        types_to_bytes.into_bytes()
    }

    fn send_frame(&mut self, frame: &[u8]) {
        self.stream.write_all(frame).expect("Error writing to socket");
        self.stream.flush().expect("Error flushing socket");
    }

    fn read_frame_response(&mut self) {
        let mut buf = [0; 1024];
        match self.stream.read(&mut buf) {
            Ok(n) if n > 0 => {
                let frame = Frame::parse_frame(&buf[..n]).expect("Error parsing frame");
                self.handle_frame(frame);
            }
            _ => {}
        }
    }

    fn handle_frame(&mut self, frame: Frame) {
        match frame.opcode {
            0x0E | 0x03 => self.authenticate_response(),
            _ => {}
        }
    }
}


// Manejo de queries especificas de mi app
impl CassandraClient {
    // Get the information of the airports
    pub fn get_airports(&mut self) -> Vec<Airport> {
        let body = self.get_body_strong_consistency("SELECT name, code, position_lat, position_lon FROM airports");
        let frame = Frame::new(VERSION, FLAGS, STREAM, OP_CODE_QUERY, body.len() as u32, body);
        self.send_frame(&frame.to_bytes());
        let body = self.get_body_respones().unwrap();
        self.rows_to_airports(&body)
    }

    fn rows_to_airports(&self, body: &[u8]) -> Vec<Airport>{
        let rows = self.get_rows(body).unwrap();
        let mut airports = Vec::new();

        for row in rows {
            let name = row.get("name")?.to_string();
            let code = row.get("code")?.to_string();
            let position_lat = row.get("position_lat")?.parse::<f64>().ok()?;
            let position_lon = row.get("position_lon")?.parse::<f64>().ok()?;

            airports.push(Airport {
                name,
                code,
                position: (position_lat, position_lon),
            });
        }

        airports
    }

    // Get the basic information of the flights
    pub fn get_flights(&mut self, airport_name: &str) -> Vec<Flight> {
        let flight_codes = self.get_flight_codes_by_airport(airport_name);
        flight_codes
            .into_iter()
            .filter_map(|code| self.get_flight(&code))
            .collect()
    }

    // Get the basic information of the flights
    pub fn get_flight(&mut self, flight_code: &str) -> Option<Flight>{
        // Pide la strong information
        let strong_query = format!(
            "SELECT code, status, arrival_airport FROM flights_by_airport WHERE flight_code = '{}';",
            flight_code
        );
        let body = self.get_body_strong_consistency(&strong_query);
        let frame = Frame::new(VERSION, FLAGS, STREAM, OP_CODE_QUERY, body.len() as u32, body);
        self.send_frame(&frame.to_bytes());
        let body_strong = self.get_body_respones().unwrap();
    
        // Pide la weak information
        let weak_query = format!(
            "SELECT position_lat, position_lon FROM flights_by_airport WHERE flight_code = '{}'",
            flight_code
        );
        let body = self.get_body_weak_consistency(&weak_query);
        let frame = Frame::new(VERSION, FLAGS, STREAM, OP_CODE_QUERY, body.len() as u32, body);
        self.send_frame(&frame.to_bytes());
        let body_weak = self.get_body_respones().unwrap();
    
        self.row_to_flight(&body_strong, &body_weak)
    }

    fn row_to_flight(&self, body_strong: &[u8], body_weak: &[u8]) -> Option<Flight> {
        let strong_row = self.get_rows(body_strong).ok()?.into_iter().next()?;
        let weak_row = self.get_rows(body_weak).ok()?.into_iter().next()?;

        // Strong Consistency
        let code = strong_row.get("code")?.to_string();
        let status_str = strong_row.get("status")?;
        let status = FlightStatus::new(status_str);
        let arrival_airport = strong_row.get("arrival_airport")?.to_string();

        // Weak Consistency
        let position_lat = weak_row.get("position_lat")?.parse::<f64>().ok()?;
        let position_lon = weak_row.get("position_lon")?.parse::<f64>().ok()?;

        Some(Flight {
            position: (position_lat, position_lon),
            code,
            status,
            arrival_airport,
        })
    }

    // Get the flight selected if exists
    pub fn get_flight_selected(&mut self, flight_code: &str) -> Option<FlightSelected> {
        // Pide la strong information
        let strong_query = format!(
            "SELECT code, status, departure_airport, arrival_airport, departure_time, arrival_time FROM flights_by_airport WHERE flight_code = '{}';",
            flight_code
        );
        let body = self.get_body_strong_consistency(&strong_query);
        let frame = Frame::new(VERSION, FLAGS, STREAM, OP_CODE_QUERY, body.len() as u32, body);
        self.send_frame(&frame.to_bytes());
        let body_strong = match self.get_body_respones() {
            Ok(response) => response,
            Err(_) => return None,
        };
    
        // Pide la weak information
        let weak_query = format!(
            "SELECT position_lat, position_lon, altitude, speed, fuel_level FROM flights_by_airport WHERE flight_code = '{}'",
            flight_code
        );
        let body = self.get_body_weak_consistency(&weak_query);
        let frame = Frame::new(VERSION, FLAGS, STREAM, OP_CODE_QUERY, body.len() as u32, body);
        self.send_frame(&frame.to_bytes());
        let body_weak = match self.get_body_respones() {
            Ok(response) => response,
            Err(_) => return None,
        };
    
        // une la informacion
        self.row_to_flight_selected(&body_strong, &body_weak)
    }

    fn row_to_flight_selected(&self, row_strong: &[u8], row_weak: &[u8]) -> Option<FlightSelected>{
        let strong_row = self.get_rows(row_strong).ok()?.into_iter().next()?;
        let weak_row = self.get_rows(row_weak).ok()?.into_iter().next()?;
    
        // Strong Consistency
        let code = strong_row.get("code")?.to_string();
        let status_str = strong_row.get("status")?;
        let status = FlightStatus::new(&status_str);
        let departure_airport = strong_row.get("departure_airport")?.to_string();
        let arrival_airport = strong_row.get("arrival_airport")?.to_string();
        let departure_time = strong_row.get("departure_time")?.to_string();
        let arrival_time = strong_row.get("arrival_time")?.to_string();
    
        // Weak Consistency
        let position_lat: f64 = weak_row.get("position_lat")?.parse().ok()?;
        let position_lon: f64 = weak_row.get("position_lon")?.parse().ok()?;
        let altitude: f64 = weak_row.get("altitude")?.parse().ok()?;
        let speed: f32 = weak_row.get("speed")?.parse().ok()?;
        let fuel_level: f32 = weak_row.get("fuel_level")?.parse().ok()?;
    
        Some(FlightSelected {
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
    fn get_flight_codes_by_airport(&mut self, airport_name: &str) -> HashSet<String> {
        let query = format!(
            "SELECT flight_code FROM flights_by_airport WHERE airport_code = '{}'",
            airport_name
        );
        let body = self.get_body_strong_consistency(&query);
        let frame = Frame::new(VERSION, FLAGS, STREAM, OP_CODE_QUERY, body.len() as u32, body);
        self.send_frame(&frame.to_bytes());
        let response = self.get_body_respones().unwrap();
        self.extract_flight_codes(&response)
    }

    fn extract_flight_codes(&self, response: &[u8]) -> HashSet<String> {
        let rows = self.get_rows(response).unwrap();
        let mut codes = HashSet::new();

        for row in rows {
            let code = row.get("flight_code").unwrap_or(&String::new()).to_string();
            codes.insert(code);
        }
        codes
    }

    fn get_rows(&self, body: &[u8]) -> Result<Vec<HashMap<String, String>>, Errors> {
        let mut cursor = BytesCursor::new(body);
        let _flags = cursor.read_int()?;

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
        Ok(rows)
    }
}