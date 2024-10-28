use std::io;
use std::io::{Read, Write};
use std::net::TcpStream;

use simulator::bytes_cursor::BytesCursor;
use simulator::frame::Frame;

fn main() {
    print!("Ip: ");
    io::stdout().flush().unwrap();
    let mut node = String::new();
    io::stdin()
        .read_line(&mut node)
        .expect("Error reading node");
    let node = node.trim();

    //let mut addrs_iter = ip.to_socket_addrs().expect("Invalid socket address");
    if let Ok(mut stream) = TcpStream::connect((node, 8080)) {
        let queries = vec![
            "INSERT INTO aviation.flightInfo (flightCode, positionLat, positionLon, altitude, speed, fuelLevel, status, departureAirport, departureTime, arrivalAirport, arrivalTime) VALUES ('AR1130', '41.6413', '-75.7787', '32000', '560', '85', 'OnTime', 'EZE', '08:30', 'JFK', '16:45');",
            "INSERT INTO aviation.flightInfo (flightCode, positionLat, positionLon, altitude, speed, fuelLevel, status, departureAirport, departureTime, arrivalAirport, arrivalTime) VALUES ('LA8050', '27.7617', '-82.2903', '34000', '580', '78', 'Delayed', 'SCL', '09:15', 'MIA', '17:00');",
            "INSERT INTO aviation.flightInfo (flightCode, positionLat, positionLon, altitude, speed, fuelLevel, status, departureAirport, departureTime, arrivalAirport, arrivalTime) VALUES ('AA940', '-22.5505', '-45.6350', '30000', '550', '65', 'OnTime', 'DFW', '07:00', 'GRU', '15:30');",
            "INSERT INTO aviation.flightInfo (flightCode, positionLat, positionLon, altitude, speed, fuelLevel, status, departureAirport, departureTime, arrivalAirport, arrivalTime) VALUES ('IB6844', '-37.6083', '-62.3019', '31000', '570', '72', 'OnTime', 'MAD', '10:00', 'EZE', '18:00');",
            "INSERT INTO aviation.flightInfo (flightCode, positionLat, positionLon, altitude, speed, fuelLevel, status, departureAirport, departureTime, arrivalAirport, arrivalTime) VALUES ('AF2280', '35.9416', '-120.4085', '33000', '590', '80', 'Cancelled', 'CDG', '12:30', 'LAX', '20:45');",
            "INSERT INTO aviation.flightInfo (flightCode, positionLat, positionLon, altitude, speed, fuelLevel, status, departureAirport, departureTime, arrivalAirport, arrivalTime) VALUES ('KL7028', '38.7749', '-123.4194', '32000', '600', '60', 'OnTime', 'AMS', '11:45', 'SFO', '20:10');",
            "INSERT INTO aviation.flightInfo (flightCode, positionLat, positionLon, altitude, speed, fuelLevel, status, departureAirport, departureTime, arrivalAirport, arrivalTime) VALUES ('BA246', '51.4700', '-3.4543', '31000', '575', '77', 'OnTime', 'LHR', '14:00', 'EZE', '17:30');",
            "INSERT INTO aviation.flightInfo (flightCode, positionLat, positionLon, altitude, speed, fuelLevel, status, departureAirport, departureTime, arrivalAirport, arrivalTime) VALUES ('JL704', '35.6735', '140.3929', '33000', '580', '70', 'OnTime', 'NRT', '16:00', 'LAX', '11:00');",
            "INSERT INTO aviation.flightInfo (flightCode, positionLat, positionLon, altitude, speed, fuelLevel, status, departureAirport, departureTime, arrivalAirport, arrivalTime) VALUES ('QF12', '-33.8688', '151.2093', '35000', '590', '82', 'OnTime', 'SYD', '11:30', 'LAX', '06:15');",
            "INSERT INTO aviation.flightInfo (flightCode, positionLat, positionLon, altitude, speed, fuelLevel, status, departureAirport, departureTime, arrivalAirport, arrivalTime) VALUES ('NZ7', '-36.8485', '174.7633', '34000', '580', '75', 'Delayed', 'AKL', '15:45', 'SFO', '08:30');",
            "INSERT INTO aviation.flightInfo (flightCode, positionLat, positionLon, altitude, speed, fuelLevel, status, departureAirport, departureTime, arrivalAirport, arrivalTime) VALUES ('EK202', '25.2697', '55.3333', '36000', '600', '88', 'OnTime', 'DXB', '02:00', 'JFK', '07:30');",
            "INSERT INTO aviation.flightInfo (flightCode, positionLat, positionLon, altitude, speed, fuelLevel, status, departureAirport, departureTime, arrivalAirport, arrivalTime) VALUES ('CA981', '39.9042', '116.4074', '34000', '570', '74', 'OnTime', 'PEK', '06:45', 'JFK', '10:00');",
            "INSERT INTO aviation.flightInfo (flightCode, positionLat, positionLon, altitude, speed, fuelLevel, status, departureAirport, departureTime, arrivalAirport, arrivalTime) VALUES ('LH400', '50.1109', '8.6821', '32000', '565', '80', 'OnTime', 'FRA', '09:00', 'JFK', '12:00');",
            "INSERT INTO aviation.flightInfo (flightCode, positionLat, positionLon, altitude, speed, fuelLevel, status, departureAirport, departureTime, arrivalAirport, arrivalTime) VALUES ('SU100', '55.7558', '37.6173', '35000', '600', '81', 'OnTime', 'SVO', '08:30', 'JFK', '12:15');",
            "INSERT INTO aviation.flightInfo (flightCode, positionLat, positionLon, altitude, speed, fuelLevel, status, departureAirport, departureTime, arrivalAirport, arrivalTime) VALUES ('CX846', '22.3964', '114.1095', '33000', '585', '77', 'Delayed', 'HKG', '12:00', 'JFK', '16:30');",
            "INSERT INTO aviation.flightInfo (flightCode, positionLat, positionLon, altitude, speed, fuelLevel, status, departureAirport, departureTime, arrivalAirport, arrivalTime) VALUES ('AF006', '48.8566', '2.3522', '32000', '575', '79', 'OnTime', 'CDG', '10:00', 'JFK', '13:30');",
            "INSERT INTO aviation.flightInfo (flightCode, positionLat, positionLon, altitude, speed, fuelLevel, status, departureAirport, departureTime, arrivalAirport, arrivalTime) VALUES ('KE85', '37.5665', '126.9780', '35000', '590', '85', 'OnTime', 'ICN', '09:45', 'JFK', '11:00');",
            "INSERT INTO aviation.flightInfo (flightCode, positionLat, positionLon, altitude, speed, fuelLevel, status, departureAirport, departureTime, arrivalAirport, arrivalTime) VALUES ('JL006', '35.6895', '139.6917', '34000', '580', '72', 'OnTime', 'NRT', '13:30', 'JFK', '17:30');",
        ];


        let consistency = 1;
        for query in queries {
            let mut body = Vec::new();
            body.extend_from_slice((query.len() as i32).to_be_bytes().as_slice());
            body.extend_from_slice(query.as_bytes());
            body.extend_from_slice((consistency as i16).to_be_bytes().as_slice());
            let mut query_bytes = vec![
                0x03, 0x00, 0x00, 0x01, 0x07
            ];

            query_bytes.extend_from_slice((body.len() as i32).to_be_bytes().as_slice());
            query_bytes.extend_from_slice(body.as_slice());

            stream
                .write_all(query_bytes.as_slice())
                .expect("Error writing to socket");

            stream.flush().expect("sds");
            let mut buf = [0; 1024];
            match stream.read(&mut buf) {
                Ok(n) => {
                    if n > 0 {
                        let frame = Frame::parse_frame(&buf[..n]).expect("Error parsing frame");
                        match frame.opcode {
                            0x00 => {
                                let mut cursor = BytesCursor::new(frame.body.as_slice());
                                println!("ERROR");
                                if let Ok(string) = cursor.read_long_string() {
                                    println!("{}", string);
                                } else {
                                    println!("{:?}", &String::from_utf8_lossy(frame.body.as_slice()));
                                }
                            }
                            0x08 => {
                                let mut cursor = BytesCursor::new(frame.body.as_slice());
                                println!("RESULT");
                                if let Ok(string) = cursor.read_long_string() {
                                    println!("{}", string);
                                } else {
                                    println!("{:?}", &String::from_utf8_lossy(frame.body.as_slice()));
                                }
                            }
                            _ => {}
                        }
                    }
                }
                Err(e) => {
                    println!("Error leyendo del socket: {}", e);
                }
            }
            stream.flush().expect("sds");
        }
    }
}