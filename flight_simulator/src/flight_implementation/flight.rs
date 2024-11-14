use rand::Rng;

use super::flight_state::FlightState;

#[derive(Default)]
pub struct Flight {
    pub status: FlightStatus,
    pub info: FlightTracking,
}

#[derive(Default)]
pub struct FlightStatus {
    // strong consistency
    pub code: String,
    pub status: FlightState,
    pub departure_airport: String,
    pub arrival_airport: String,
    pub departure_time: String,
    pub arrival_time: String,
}

#[derive(Default)]
pub struct FlightTracking  {
    // weak consistency
    pub position: (f64, f64),
    pub altitude: f64,
    pub speed: f32,
    pub fuel_level: f32,
}

impl Flight {
    pub fn new(info: FlightTracking, status: FlightStatus) -> Self {
        Self { info, status }
    }

    // Restart with initial values
    pub fn restart(&mut self, position: (f64, f64)) {
        self.set_position(position);
        self.set_altitude(0.0);
        self.set_speed(0.0);
        self.set_fuel_level(gen_random(80.0, 100.0));
        self.set_status(FlightState::OnTime);
    }

    // Update the flight progress
    pub fn update_progress(&mut self, _step: f32) {
        todo!();
    }

    pub fn get_code(&self) -> &String {
        &self.status.code
    }

    pub fn set_code(&mut self, code: String) {
        self.status.code = code;
    }

    pub fn get_status(&self) -> &FlightState {
        &self.status.status
    }

    pub fn set_status(&mut self, status: FlightState) {
        self.status.status = status;
    }

    pub fn get_position(&self) -> &(f64, f64) {
        &self.info.position
    }

    pub fn set_position(&mut self, position: (f64, f64)) {
        self.info.position = position;
    }

    pub fn get_altitude(&self) -> f64 {
        self.info.altitude
    }

    pub fn set_altitude(&mut self, altitude: f64) {
        self.info.altitude = altitude;
    }

    pub fn get_departure_airport(&self) -> &String {
        &self.status.departure_airport
    }

    pub fn set_departure_airport(&mut self, airport: String) {
        self.status.departure_airport = airport;
    }

    pub fn get_arrival_airport(&self) -> &String {
        &self.status.arrival_airport
    }

    pub fn set_arrival_airport(&mut self, airport: String) {
        self.status.arrival_airport = airport;
    }

    pub fn get_speed(&self) -> f32 {
        self.info.speed
    }

    pub fn set_speed(&mut self, speed: f32) {
        self.info.speed = speed;
    }

    pub fn get_fuel_level(&self) -> f32 {
        self.info.fuel_level
    }

    pub fn set_fuel_level(&mut self, fuel_level: f32) {
        self.info.fuel_level = fuel_level;
    }

    pub fn get_departure_time(&self) -> &String {
        &self.status.departure_time
    }

    pub fn get_arrival_time(&self) -> &String {
        &self.status.arrival_time
    }
}

// Generador aleatorio de float en un rango
fn gen_random(min: f32, max: f32) -> f32 {
    let mut rng = rand::thread_rng();
    rng.gen_range(min..=max)
}

pub fn get_airport_coordinates(airport: &str) -> (f64, f64) {
    match airport {
        "JFK" => (-73.7781, 40.6413),  // JFK, Nueva York
        "LAX" => (-118.4085, 33.9416), // LAX, Los Ángeles
        "EZE" => (-58.5358, -34.8222), // EZE, Buenos Aires (Ezeiza)
        "CDG" => (2.55, 49.0097),      // CDG, París Charles de Gaulle
        "LHR" => (-0.4543, 51.4700),   // LHR, Londres Heathrow
        "NRT" => (140.3929, 35.7735),  // NRT, Tokio Narita
        "FRA" => (8.5706, 50.0333),    // FRA, Frankfurt
        "SYD" => (151.1772, -33.9399), // SYD, Sídney Kingsford Smith
        "SCL" => (-70.7853, -33.3929), // SCL, Santiago de Chile
        "MIA" => (-80.2906, 25.7957),  // MIA, Miami
        "DFW" => (-97.0379, 32.8968),  // DFW, Dallas/Fort Worth
        "GRU" => (-46.6506, -23.4253), // GRU, São Paulo-Guarulhos
        "MAD" => (-3.5705, 40.4719),   // MAD, Madrid
        "AMS" => (4.7600, 52.3081),    // AMS, Ámsterdam
        "SFO" => (-122.4194, 37.6213), // SFO, San Francisco
        _ => (0.0, 0.0),
    }
}
