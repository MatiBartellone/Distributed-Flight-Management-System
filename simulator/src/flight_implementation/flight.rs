use rand::Rng;

use super::flight_state::FlightState;

#[derive(Default, Clone)]
pub struct Flight {
    pub status: FlightStatus,
    pub info: FlightTracking,
}

#[derive(Default, Clone)]
pub struct FlightStatus {
    // strong consistency
    pub code: String,
    pub status: FlightState,
    pub departure_airport: String,
    pub arrival_airport: String,
    pub departure_time: String,
    pub arrival_time: String,
}

#[derive(Default, Clone)]
pub struct FlightTracking  {
    // weak consistency
    pub position: (f64, f64),
    pub arrival_position: (f64, f64),
    pub altitude: f64,
    pub speed: f32,
    pub fuel_level: f32
}

impl Flight {
    pub fn new(info: FlightTracking, status: FlightStatus) -> Self {
        Self { info, status }
    }

    /// Restart with initial values and set the position
    pub fn restart(&mut self, position: (f64, f64)) {
        self.set_position(position);
        self.set_altitude(0.0);
        self.set_speed(0.0);
        self.set_fuel_level(gen_random(80.0, 100.0));
        self.set_status(FlightState::OnTime);
    }

    /// Update the flight progress based on the arrival position and the step time
    pub fn update_progress(&mut self, arrival_position: (f64, f64), step: f32) {
        self.update_position(arrival_position, step);
        self.update_altitude(arrival_position, step);
        self.update_speed(arrival_position, step);
        self.update_fuel(step);
        self.update_status_if_target_reached(arrival_position);
    }

    fn update_position(&mut self, arrival_position: (f64, f64), step: f32) {
        let (current_x, current_y) = self.info.position;
        let (target_x, target_y) = arrival_position;

        // Calculate the direction to the target
        let direction_x = target_x - current_x;
        let direction_y = target_y - current_y;

        // Calculate the distance to the target --> sqrt(x^2 + y^2)
        let distance = (direction_x.powi(2) + direction_y.powi(2)).sqrt();
        if distance == 0.0 {
            return;
        }

        // Normalize the direction to get the unit vector --> unit_x + unit_y = 1
        let unit_x = direction_x / distance;
        let unit_y = direction_y / distance;

        // Calculate the movement with the step and the speed --> movement_x + movement_y = speed * step
        let movement_x = unit_x * self.info.speed as f64 * step as f64;
        let movement_y = unit_y * self.info.speed as f64 * step as f64;

        // Update the position, if we are close enough to the target, we set the position to the target
        if movement_x.abs() >= direction_x.abs() && movement_y.abs() >= direction_y.abs() {
            self.info.position = arrival_position;
        } else {
            self.info.position.0 += movement_x;
            self.info.position.1 += movement_y;
        }
    }

    fn update_altitude(&mut self, arrival_position: (f64, f64), step: f32) {
        let distance_to_target = self.get_distance_to_target(arrival_position);

        if distance_to_target < 200.0 {
            self.info.altitude -= 500.0 * step as f64;
        } else if self.info.altitude < 10000.0 {
            self.info.altitude += 300.0 * step as f64;
        } else if distance_to_target < 500.0 {
            self.info.altitude -= 200.0 * step as f64;
        }
    
        self.info.altitude = self.info.altitude.clamp(0.0, 12000.0);
    }

    fn update_speed(&mut self, arrival_position: (f64, f64), step: f32) {
        let target_speed = if self.info.altitude < 5000.0 {
            300.0
        } else if self.get_distance_to_target(arrival_position) < 50.0 {
            200.0
        } else {
            600.0
        };
    
        let acceleration = 50.0 * step;
    
        if self.info.speed < target_speed {
            self.info.speed = (self.info.speed + acceleration).min(target_speed);
        } else if self.info.speed > target_speed {
            self.info.speed = (self.info.speed - acceleration).max(target_speed);
        }
    }

    fn update_fuel(&mut self, step: f32) {
        let fuel_consumption = (self.info.speed * step) / 100.0;
        self.info.fuel_level -= fuel_consumption;
        self.info.fuel_level = self.info.fuel_level.max(0.0);
    }

    fn update_status_if_target_reached(&mut self, target_position: (f64, f64)) {
        if self.info.position == target_position {
            self.set_status(FlightState::Arrived);
        }
    }

    // Calculate the distance to the target --> sqrt(x^2 + y^2)
    fn get_distance_to_target(&self, arrival_position: (f64, f64)) -> f64 {
        let (current_x, current_y) = self.get_position();
        let (target_x, target_y) = arrival_position;
        ((target_x - current_x).powi(2) + (target_y - current_y).powi(2)).sqrt()
    }

    pub fn get_code(&self) -> String {
        self.status.code.to_string()
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

    pub fn get_arrival_position(&self) -> &(f64, f64) {
        &self.info.arrival_position
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
