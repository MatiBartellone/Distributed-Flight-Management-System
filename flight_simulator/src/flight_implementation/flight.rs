use rand::Rng;

use super::flight_status::FlightStatus;

#[derive(Default, Debug)]
pub struct Flight {
    // strong consistency
    pub code: String,
    pub status: FlightStatus,
    pub departure_airport: String,
    pub arrival_airport: String,
    pub departure_time: String,
    pub arrival_time: String,
    // weak consistency
    pub position: (f64, f64),
    pub altitude: f64,
    pub speed: f32,
    pub fuel_level: f32,
}

impl Flight {
    pub fn new(
        code: String,
        status: FlightStatus,
        departure_airport: String,
        arrival_airport: String,
        departure_time: String,
        arrival_time: String,
        position: (f64, f64),
        altitude: f64,
        speed: f32,
        fuel_level: f32,
    ) -> Self {
        Flight {
            code,
            status,
            departure_airport,
            arrival_airport,
            departure_time,
            arrival_time,
            position,
            altitude,
            speed,
            fuel_level,
        }
    }

    pub fn restart(&mut self) {
        self.position = get_airport_coordinates(&self.departure_airport);
        self.altitude = 0.0;
        self.speed = 0.0;
        self.fuel_level = gen_random(80.0, 100.0);
        self.status = FlightStatus::OnTime;
    }

    pub fn update_progress(&mut self, step: f32) {
        todo!();
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
