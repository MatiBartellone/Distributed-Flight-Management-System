use std::{collections::HashMap, io::{Read, Write}, net::TcpStream};

use crate::{airport::airport::Airport, flight::flight::Flight, utils::{bytes_cursor::BytesCursor, consistency_level::ConsistencyLevel, frame::Frame, types_to_bytes::TypesToBytes}};

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
        let mut types_to_bytes = TypesToBytes::new();
        let mut options_map = HashMap::new();
        options_map.insert("CQL_VERSION".to_string(), "3.0.0".to_string());
        types_to_bytes.write_string_map(&options_map);
        self.send_frame(&types_to_bytes.into_bytes());
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

    // Get the information of the airports
    pub fn get_airports(&mut self) -> Vec<Airport> {
        let mut types_to_bytes = TypesToBytes::new();
        types_to_bytes.write_long_string("SELECT name, code, position_lat, position_lon FROM airports");
        types_to_bytes.write_consistency(ConsistencyLevel::All);
        self.send_frame(&types_to_bytes.into_bytes());

        let mut buf = [0; 1024];
        match self.stream.read(&mut buf) {
            Ok(n) if n > 0 => {
                let frame = Frame::parse_frame(&buf[..n]).expect("Error parsing frame");
                let mut cursor = BytesCursor::new(&frame.body);
                self.rows_to_airports(&cursor.read_long_string().unwrap())
            }
            _ => Vec::new()
        }
    }

    fn rows_to_airports(&self, rows: &str) -> Vec<Airport>{

    }

    // Get the full information of the flights
    pub fn get_flights_full() -> Vec<Flight> {

    }

    // Update the full information of the flights
    pub fn update_flights_full(flights: Vec<Flight>) {

    }

    // Update the full information of the flight
    pub fn update_flight_full(flights: Vec<Flight>) {

    }

    // Update the basic information of the flights
    pub fn update_flights_basic(flights: Vec<Flight>) {

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