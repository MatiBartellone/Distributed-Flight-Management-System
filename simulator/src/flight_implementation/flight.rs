use std::f64::consts::PI;

use super::flight_state::FlightState;

#[derive(Default, Clone, Debug)]
pub struct Flight {
    pub status: FlightStatus,
    pub tracking: FlightTracking,
}

#[derive(Default, Clone, Debug)]
pub struct FlightStatus {
    // strong consistency
    pub code: String,
    pub departure_airport: String,
    pub arrival_airport: String,
    pub departure_time: String,
    pub arrival_time: String,
}

#[derive(Default, Clone, Debug)]
pub struct FlightTracking  {
    // weak consistency
    pub position: (f64, f64),
    pub arrival_position: (f64, f64),
    pub altitude: f64,
    pub speed: f32,
    pub fuel_level: f32,
    pub status: FlightState,
}

impl Flight {
    pub fn new(tracking: FlightTracking, status: FlightStatus) -> Self {
        Self { tracking, status }
    }

    /// Restart with initial values and set the position
    pub fn restart(&mut self, position: (f64, f64)) {
        self.set_position(position);
        self.set_altitude(0.0);
        self.set_speed(0.0);
        self.set_fuel_level(100.0);
        self.set_status(FlightState::OnTime);
    }

    /// Update the flight progress based on the arrival position and the step time
    pub fn update_progress(&mut self, arrival_position: (f64, f64), step: f32) {
        if self.get_position() == &arrival_position {
            return;
        }
        self.update_position(arrival_position, step);
        self.update_altitude(arrival_position, step);
        self.update_speed(arrival_position, step);
        self.update_fuel(step);
        self.update_status_if_target_reached(arrival_position);
    }

    fn update_position(&mut self, arrival_position: (f64, f64), step: f32) {
        // Convertir coordenadas a radianes
        let lat1: f64 = deg_to_rad(self.tracking.position.0);
        let lon1: f64 = deg_to_rad(self.tracking.position.1);
        let lat2: f64 = deg_to_rad(arrival_position.0);
        let lon2: f64 = deg_to_rad(arrival_position.1);
    
        // Radio de la Tierra (km)
        let r: f64 = 6371.0;
    
        // Diferencias en latitud y longitud
        let delta_lat = lat2 - lat1;
        let delta_lon = lon2 - lon1;
    
        // Calcular la distancia restante al destino (fórmula de Haversine)
        let a = (delta_lat / 2.0).sin().powi(2)
            + lat1.cos() * lat2.cos() * (delta_lon / 2.0).sin().powi(2);
        let distance_to_target = 2.0 * r * a.sqrt().atan2((1.0 - a).sqrt());
    
        // Distancia a recorrer en este paso (km)
        let d_step: f64 = (self.get_speed() as f64) * (step as f64);
    
        // Si el paso supera la distancia restante, mover directamente al destino
        if d_step >= distance_to_target {
            self.set_position(arrival_position);
            return;
        }
    
        // Calcular el rumbo (bearing)
        let bearing: f64 = (delta_lon.sin() * lat2.cos())
            .atan2(lat1.cos() * lat2.sin() - lat1.sin() * lat2.cos() * delta_lon.cos());
    
        // Calcular la nueva posición
        let lat_new = (lat1.sin() * (d_step / r).cos()
            + lat1.cos() * (d_step / r).sin() * bearing.cos())
        .asin();
    
        let lon_new = lon1
            + ((bearing.sin() * (d_step / r).sin() * lat1.cos())
                .atan2((d_step / r).cos() - lat1.sin() * lat_new.sin()));
    
        // Convertir a grados y actualizar la posición
        self.set_position((rad_to_deg(lat_new), rad_to_deg(lon_new)));
    }

    fn update_altitude(&mut self, arrival_position: (f64, f64), step: f32) {
        let distance_to_target = self.get_distance_to_target(arrival_position);

        if distance_to_target < 200.0 {
            self.tracking.altitude -= 500.0 * step as f64;
        } else if self.tracking.altitude < 10000.0 {
            self.tracking.altitude += 300.0 * step as f64;
        } else if distance_to_target < 500.0 {
            self.tracking.altitude -= 200.0 * step as f64;
        }
    
        self.tracking.altitude = self.tracking.altitude.clamp(0.0, 12000.0);
    }

    fn update_speed(&mut self, arrival_position: (f64, f64), step: f32) {
        let target_speed = if self.tracking.altitude < 5000.0 {
            300.0
        } else if self.get_distance_to_target(arrival_position) < 50.0 {
            200.0
        } else {
            600.0
        };
    
        let acceleration = 50.0 * step;
    
        if self.tracking.speed < target_speed {
            self.tracking.speed = (self.tracking.speed + acceleration).min(target_speed);
        } else if self.tracking.speed > target_speed {
            self.tracking.speed = (self.tracking.speed - acceleration).max(target_speed);
        }
    }

    fn update_fuel(&mut self, step: f32) {
        let fuel_consumption = (self.tracking.speed * step) / 100.0;
        self.tracking.fuel_level -= fuel_consumption;
        self.tracking.fuel_level = self.tracking.fuel_level.max(0.0);
    }

    fn update_status_if_target_reached(&mut self, target_position: (f64, f64)) {
        if self.tracking.position == target_position {
            self.set_status(FlightState::Arrived);
        }
    }

    // Calculate the distance to the target --> sqrt(x^2 + y^2)
    fn get_distance_to_target(&self, arrival_position: (f64, f64)) -> f64 {
        let (current_x, current_y) = self.get_position();
        let (target_x, target_y) = arrival_position;
        ((target_x - current_x).powi(2) + (target_y - current_y).powi(2)).sqrt()
    }

    pub fn has_arrived(&self) -> bool {
        self.get_status() == &FlightState::Arrived
    }

    pub fn get_code(&self) -> String {
        self.status.code.to_string()
    }

    pub fn set_code(&mut self, code: String) {
        self.status.code = code;
    }

    pub fn get_status(&self) -> &FlightState {
        &self.tracking.status
    }

    pub fn set_status(&mut self, status: FlightState) {
        self.tracking.status = status;
    }

    pub fn get_position(&self) -> &(f64, f64) {
        &self.tracking.position
    }

    pub fn set_position(&mut self, position: (f64, f64)) {
        self.tracking.position = position;
    }

    pub fn get_altitude(&self) -> f64 {
        self.tracking.altitude
    }

    pub fn set_altitude(&mut self, altitude: f64) {
        self.tracking.altitude = altitude;
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
        self.status.arrival_airport = airport
    }

    pub fn get_speed(&self) -> f32 {
        self.tracking.speed
    }

    pub fn set_speed(&mut self, speed: f32) {
        self.tracking.speed = speed;
    }

    pub fn get_fuel_level(&self) -> f32 {
        self.tracking.fuel_level
    }

    pub fn set_fuel_level(&mut self, fuel_level: f32) {
        self.tracking.fuel_level = fuel_level;
    }

    pub fn get_departure_time(&self) -> &String {
        &self.status.departure_time
    }

    pub fn get_arrival_time(&self) -> &String {
        &self.status.arrival_time
    }

    pub fn get_arrival_position(&self) -> &(f64, f64) {
        &self.tracking.arrival_position
    }

    pub fn set_arrival_position(&mut self, position: (f64, f64)) {
        self.tracking.arrival_position = position;
    }
}

/// Convierte grados a radianes.
fn deg_to_rad(deg: f64) -> f64 {
    deg * PI / 180.0
}

/// Convierte radianes a grados.
fn rad_to_deg(rad: f64) -> f64 {
    rad * 180.0 / PI
}