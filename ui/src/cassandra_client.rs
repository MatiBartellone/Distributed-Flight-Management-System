use std::{collections::{HashMap, HashSet}, io::{Read, Write}, net::TcpStream};

use num_cpus::get;

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
                dbg!(&frame);
                Ok(frame.body)
            }
            Ok(_) | Err(_) => Ok(Vec::new())
        }
    }

    fn get_body_strong_consistency(&self, query: &str) -> Vec<u8> {
        let mut types_to_bytes = TypesToBytes::new();
        let _ = types_to_bytes.write_long_string(query);
        let _ = types_to_bytes.write_consistency(ConsistencyLevel::Quorum);
        types_to_bytes.into_bytes()
    }

    fn get_body_weak_consistency(&self, query: &str) -> Vec<u8> {
        let mut types_to_bytes = TypesToBytes::new();
        let _ = types_to_bytes.write_long_string(query);
        let _ = types_to_bytes.write_consistency(ConsistencyLevel::One);
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
        let body = self.get_body_strong_consistency("SELECT * FROM airports");
        let frame = Frame::new(VERSION, FLAGS, STREAM, OP_CODE_QUERY, body.len() as u32, body);
        self.send_frame(&frame.to_bytes());
        let body = self.get_body_respones().unwrap();
        let mut cursor = BytesCursor::new(body.as_slice());
        dbg!(cursor.read_long_string().unwrap());
        self.rows_to_airports(&body)
    }

    fn rows_to_airports(&self, body: &[u8]) -> Vec<Airport>{
        return vec![
        Airport::new(
            "Aeropuerto Internacional Ministro Pistarini".to_string(),
            "EZE".to_string(),
            (-58.535, -34.812),
        ), // EZE
        Airport::new(
            "Aeropuerto Internacional John F. Kennedy".to_string(),
            "JFK".to_string(),
            (-73.7781, 40.6413),
        ), // JFK
        Airport::new(
            "Aeropuerto Internacional Comodoro Arturo Merino Benítez".to_string(),
            "SCL".to_string(),
            (-70.7859, -33.3928),
        ), // SCL
        Airport::new(
            "Aeropuerto Internacional de Miami".to_string(),
            "MIA".to_string(),
            (-80.2870, 25.7959),
        ), // MIA
        Airport::new(
            "Aeropuerto Internacional de Dallas/Fort Worth".to_string(),
            "DFW".to_string(),
            (-97.0382, 32.8968),
        ), // DFW
        Airport::new(
            "Aeroporto Internacional de São Paulo/Guarulhos".to_string(),
            "GRU".to_string(),
            (-46.4731, -23.4255),
        ), // GRU
        Airport::new(
            "Aeropuerto Adolfo Suárez Madrid-Barajas".to_string(),
            "MAD".to_string(),
            (-3.5706, 40.4935),
        ), // MAD
        Airport::new(
            "Aéroport de Paris-Charles-de-Gaulle".to_string(),
            "CDG".to_string(),
            (2.5479, 49.0097),
        ), // CDG
        Airport::new(
            "Aeropuerto Internacional de Los Ángeles".to_string(),
            "LAX".to_string(),
            (-118.4108, 33.9428),
        ), // LAX
        Airport::new(
            "Luchthaven Schiphol".to_string(),
            "AMS".to_string(),
            (4.7642, 52.3086),
        ), // AMS
        Airport::new(
            "Narita International Airport".to_string(),
            "NRT".to_string(),
            (140.3851, 35.7653),
        ), // NRT
        Airport::new(
            "Aeropuerto de Heathrow".to_string(),
            "LHR".to_string(),
            (-0.4543, 51.4700),
        ), // LHR
        Airport::new(
            "Aeropuerto de Fráncfort del Meno".to_string(),
            "FRA".to_string(),
            (8.5706, 50.0333),
        ), // FRA
        Airport::new(
            "Aeropuerto de Sídney".to_string(),
            "SYD".to_string(),
            (151.1772, -33.9461),
        ), // SYD
        Airport::new(
            "Aeropuerto Internacional de San Francisco".to_string(),
            "SFO".to_string(),
            (-122.3790, 37.6213),
        ), // SFO
    ];/*
        let rows = self.get_rows(body).unwrap();
        let mut airports = Vec::new();

        for row in rows {
            let name = match row.get("name") {
                Some(name) => name.to_string(),
                None => continue,
            };
    
            let code = match row.get("code") {
                Some(code) => code.to_string(),
                None => continue,
            };
    
            let position_lat = match row.get("position_lat")
                .and_then(|val| val.parse::<f64>().ok()) {
                Some(lat) => lat,
                None => continue,
            };
    
            let position_lon = match row.get("position_lon")
                .and_then(|val| val.parse::<f64>().ok()) {
                Some(lon) => lon,
                None => continue,
            };

            airports.push(Airport {
                name,
                code,
                position: (position_lat, position_lon),
            });
        }*/
    }

    // Get the basic information of the flights
    pub fn get_flights(&mut self, airport_name: &str) -> Vec<Flight> {
        get_flights()
        //let flight_codes = self.get_flight_codes_by_airport(airport_name);
        //flight_codes
        //    .into_iter()
        //    .filter_map(|code| self.get_flight(&code))
        //    .collect()
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
        get_flight_selected(flight_code)
        // Pide la strong information
        /*let strong_query = format!(
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
        self.row_to_flight_selected(&body_strong, &body_weak)*/
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
        let codes = vec![
            "AR1130", "LA8050", "AA940", "IB6844", "AF2280", "KL7028", 
            "BA246", "JL704", "QF12", "NZ7", "EK202", "CA981", "LH400", 
            "SU100", "CX846", "AF006", "KE85", "JL006"
        ];
        
        codes.into_iter().map(|s| s.to_string()).collect()
        /*let rows = self.get_rows(response).unwrap();
        let mut codes = HashSet::new();

        for row in rows {
            let code = row.get("flight_code").unwrap_or(&String::new()).to_string();
            codes.insert(code);
        }*/
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

pub fn get_flights() -> Vec<Flight> {
    vec![
        Flight {
            code: "AR1130".to_string(),
            position: (-75.7787, 41.6413),
            status: FlightStatus::OnTime,
            arrival_airport: "JFK".to_string(),
        },
        Flight {
            code: "LA8050".to_string(),
            position: (-82.2903, 27.7617),
            status: FlightStatus::Delayed,
            arrival_airport: "MIA".to_string(),
        },
        Flight {
            code: "AA940".to_string(),
            position: (-45.6350, -22.5505),
            status: FlightStatus::OnTime,
            arrival_airport: "GRU".to_string(),
        },
        Flight {
            code: "IB6844".to_string(),
            position: (-62.3019, -37.6083),
            status: FlightStatus::OnTime,
            arrival_airport: "EZE".to_string(),
        },
        Flight {
            code: "AF2280".to_string(),
            position: (-120.4085, 35.9416),
            status: FlightStatus::Cancelled,
            arrival_airport: "LAX".to_string(),
        },
        Flight {
            code: "KL7028".to_string(),
            position: (-123.4194, 38.7749),
            status: FlightStatus::OnTime,
            arrival_airport: "SFO".to_string(),
        },
        Flight {
            code: "BA246".to_string(),
            position: (-3.4543, 51.4700),
            status: FlightStatus::OnTime,
            arrival_airport: "EZE".to_string(),
        },
        Flight {
            code: "JL704".to_string(),
            position: (140.3929, 35.6735),
            status: FlightStatus::OnTime,
            arrival_airport: "LAX".to_string(),
        },
        Flight {
            code: "QF12".to_string(),
            position: (151.2093, -33.8688),
            status: FlightStatus::OnTime,
            arrival_airport: "LAX".to_string(),
        },
        Flight {
            code: "NZ7".to_string(),
            position: (174.7633, -36.8485),
            status: FlightStatus::Delayed,
            arrival_airport: "SFO".to_string(),
        },
        Flight {
            code: "EK202".to_string(),
            position: (55.3333, 25.2697),
            status: FlightStatus::OnTime,
            arrival_airport: "JFK".to_string(),
        },
        Flight {
            code: "CA981".to_string(),
            position: (116.4074, 39.9042),
            status: FlightStatus::OnTime,
            arrival_airport: "JFK".to_string(),
        },
        Flight {
            code: "LH400".to_string(),
            position: (8.6821, 50.1109),
            status: FlightStatus::OnTime,
            arrival_airport: "JFK".to_string(),
        },
        Flight {
            code: "SU100".to_string(),
            position: (37.6173, 55.7558),
            status: FlightStatus::OnTime,
            arrival_airport: "JFK".to_string(),
        },
        Flight {
            code: "CX846".to_string(),
            position: (114.1095, 22.3964),
            status: FlightStatus::Delayed,
            arrival_airport: "JFK".to_string(),
        },
        Flight {
            code: "AF006".to_string(),
            position: (2.3522, 48.8566),
            status: FlightStatus::OnTime,
            arrival_airport: "JFK".to_string(),
        },
        Flight {
            code: "KE85".to_string(),
            position: (126.9780, 37.5665),
            status: FlightStatus::OnTime,
            arrival_airport: "JFK".to_string(),
        },
        Flight {
            code: "JL006".to_string(),
            position: (139.6917, 35.6895),
            status: FlightStatus::OnTime,
            arrival_airport: "JFK".to_string(),
        },
    ]
}

pub fn get_flight_selected(flight_code: &str) -> Option<FlightSelected> {
    let aviones = vec![
        FlightSelected {
            code: "AR1130".to_string(),
            position: (-75.7787, 41.6413),
            altitude: 32000.0,
            speed: 560.0,
            fuel_level: 85.0,
            status: FlightStatus::OnTime,
            departure_airport: "EZE".to_string(),
            departure_time: "08:30".to_string(),
            arrival_airport: "JFK".to_string(),
            arrival_time: "16:45".to_string(),
        },
        FlightSelected {
            code: "LA8050".to_string(),
            position: (-82.2903, 27.7617),
            altitude: 34000.0,
            speed: 580.0,
            fuel_level: 78.0,
            status: FlightStatus::Delayed,
            departure_airport: "SCL".to_string(),
            departure_time: "09:15".to_string(),
            arrival_airport: "MIA".to_string(),
            arrival_time: "17:00".to_string(),
        },
        FlightSelected {
            code: "AA940".to_string(),
            position: (-45.6350, -22.5505),
            altitude: 30000.0,
            speed: 550.0,
            fuel_level: 65.0,
            status: FlightStatus::OnTime,
            departure_airport: "DFW".to_string(),
            departure_time: "07:00".to_string(),
            arrival_airport: "GRU".to_string(),
            arrival_time: "15:30".to_string(),
        },
        FlightSelected {
            code: "IB6844".to_string(),
            position: (-62.3019, -37.6083),
            altitude: 31000.0,
            speed: 570.0,
            fuel_level: 72.0,
            status: FlightStatus::OnTime,
            departure_airport: "MAD".to_string(),
            departure_time: "10:00".to_string(),
            arrival_airport: "EZE".to_string(),
            arrival_time: "18:00".to_string(),
        },
        FlightSelected {
            code: "AF2280".to_string(),
            position: (-120.4085, 35.9416),
            altitude: 33000.0,
            speed: 590.0,
            fuel_level: 80.0,
            status: FlightStatus::Cancelled,
            departure_airport: "CDG".to_string(),
            departure_time: "12:30".to_string(),
            arrival_airport: "LAX".to_string(),
            arrival_time: "20:45".to_string(),
        },
        FlightSelected {
            code: "KL7028".to_string(),
            position: (-123.4194, 38.7749),
            altitude: 32000.0,
            speed: 600.0,
            fuel_level: 60.0,
            status: FlightStatus::OnTime,
            departure_airport: "AMS".to_string(),
            departure_time: "11:45".to_string(),
            arrival_airport: "SFO".to_string(),
            arrival_time: "20:10".to_string(),
        },
        FlightSelected {
            code: "BA246".to_string(),
            position: (-3.4543, 51.4700),
            altitude: 31000.0,
            speed: 575.0,
            fuel_level: 77.0,
            status: FlightStatus::OnTime,
            departure_airport: "LHR".to_string(),
            departure_time: "14:00".to_string(),
            arrival_airport: "EZE".to_string(),
            arrival_time: "17:30".to_string(),
        },
        FlightSelected {
            code: "JL704".to_string(),
            position: (140.3929, 35.6735),
            altitude: 33000.0,
            speed: 580.0,
            fuel_level: 70.0,
            status: FlightStatus::OnTime,
            departure_airport: "NRT".to_string(),
            departure_time: "16:00".to_string(),
            arrival_airport: "LAX".to_string(),
            arrival_time: "11:00".to_string(),
        },
        FlightSelected {
            code: "QF12".to_string(),
            position: (151.2093, -33.8688),
            altitude: 35000.0,
            speed: 590.0,
            fuel_level: 82.0,
            status: FlightStatus::OnTime,
            departure_airport: "SYD".to_string(),
            departure_time: "11:30".to_string(),
            arrival_airport: "LAX".to_string(),
            arrival_time: "06:15".to_string(),
        },
        FlightSelected {
            code: "NZ7".to_string(),
            position: (174.7633, -36.8485),
            altitude: 34000.0,
            speed: 580.0,
            fuel_level: 75.0,
            status: FlightStatus::Delayed,
            departure_airport: "AKL".to_string(),
            departure_time: "15:45".to_string(),
            arrival_airport: "SFO".to_string(),
            arrival_time: "08:30".to_string(),
        },
        FlightSelected {
            code: "EK202".to_string(),
            position: (55.3333, 25.2697),
            altitude: 36000.0,
            speed: 600.0,
            fuel_level: 88.0,
            status: FlightStatus::OnTime,
            departure_airport: "DXB".to_string(),
            departure_time: "02:00".to_string(),
            arrival_airport: "JFK".to_string(),
            arrival_time: "07:30".to_string(),
        },
        FlightSelected {
            code: "CA981".to_string(),
            position: (116.4074, 39.9042),
            altitude: 34000.0,
            speed: 570.0,
            fuel_level: 74.0,
            status: FlightStatus::OnTime,
            departure_airport: "PEK".to_string(),
            departure_time: "06:45".to_string(),
            arrival_airport: "JFK".to_string(),
            arrival_time: "10:00".to_string(),
        },
        FlightSelected {
            code: "LH400".to_string(),
            position: (8.6821, 50.1109),
            altitude: 32000.0,
            speed: 565.0,
            fuel_level: 80.0,
            status: FlightStatus::OnTime,
            departure_airport: "FRA".to_string(),
            departure_time: "09:00".to_string(),
            arrival_airport: "JFK".to_string(),
            arrival_time: "12:00".to_string(),
        },
        FlightSelected {
            code: "SU100".to_string(),
            position: (37.6173, 55.7558),
            altitude: 35000.0,
            speed: 600.0,
            fuel_level: 81.0,
            status: FlightStatus::OnTime,
            departure_airport: "SVO".to_string(),
            departure_time: "08:30".to_string(),
            arrival_airport: "JFK".to_string(),
            arrival_time: "12:15".to_string(),
        },
        FlightSelected {
            code: "CX846".to_string(),
            position: (114.1095, 22.3964),
            altitude: 33000.0,
            speed: 585.0,
            fuel_level: 77.0,
            status: FlightStatus::Delayed,
            departure_airport: "HKG".to_string(),
            departure_time: "12:00".to_string(),
            arrival_airport: "JFK".to_string(),
            arrival_time: "16:30".to_string(),
        },
        FlightSelected {
            code: "AF006".to_string(),
            position: (2.3522, 48.8566),
            altitude: 32000.0,
            speed: 575.0,
            fuel_level: 79.0,
            status: FlightStatus::OnTime,
            departure_airport: "CDG".to_string(),
            departure_time: "10:00".to_string(),
            arrival_airport: "JFK".to_string(),
            arrival_time: "13:30".to_string(),
        },
        FlightSelected {
            code: "KE85".to_string(),
            position: (126.9780, 37.5665),
            altitude: 35000.0,
            speed: 590.0,
            fuel_level: 85.0,
            status: FlightStatus::OnTime,
            departure_airport: "ICN".to_string(),
            departure_time: "09:45".to_string(),
            arrival_airport: "JFK".to_string(),
            arrival_time: "11:00".to_string(),
        },
        FlightSelected {
            code: "JL006".to_string(),
            position: (139.6917, 35.6895),
            altitude: 34000.0,
            speed: 580.0,
            fuel_level: 72.0,
            status: FlightStatus::OnTime,
            departure_airport: "NRT".to_string(),
            departure_time: "13:30".to_string(),
            arrival_airport: "JFK".to_string(),
            arrival_time: "17:30".to_string(),
        },
    ];

    for avion in aviones {
        if avion.code == flight_code {
            return Some(avion);
        }
    }
    None
}