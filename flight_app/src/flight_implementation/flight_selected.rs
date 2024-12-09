use super::flight_state::FlightState;

#[derive(Default, Debug)]
pub struct FlightSelected {
    pub status: FlightStatus,
    pub tracking: FlightTracking
}

#[derive(Default, Debug)]
pub struct FlightStatus {
    // strong consistency
    pub code: String,
    pub status: FlightState,
    pub departure_airport: String,
    pub arrival_airport: String,
    pub departure_time: String,
    pub arrival_time: String,
}

#[derive(Default, Debug)]
pub struct FlightTracking {
    // weak consistency
    pub position: (f64, f64),
    pub arrival_position: (f64, f64),
    pub altitude: f64,
    pub speed: f32,
    pub fuel_level: f32,
}


impl FlightSelected {
    pub fn new(tracking: FlightTracking, status: FlightStatus) -> Self {
        Self { tracking, status }
    }

    /// List the full information of the selected flight
    pub fn list_information(&self, ui: &mut egui::Ui) {
        ui.label(self.get_code());
        ui.label(format!(
            "Posición: ({:.2}, {:.2})",
            self.get_position().0, self.get_position().1
        ));
        ui.label(format!("Altitud: {} ft", self.get_altitude()));
        ui.label(format!("Velocidad: {} km/h", self.get_speed()));
        ui.label(format!("Nivel de combustible: {}%", self.get_fuel_level()));
        ui.label(format!("Estado: {}", self.get_status()));
        ui.label(format!("Aeropuerto de salida: {}", self.get_departure_airport()));
        ui.label(format!("Hora de salida: {}", self.get_departure_time()));
        ui.label(format!("Aeropuerto de llegada: {}", self.get_arrival_airport()));
        ui.label(format!("Hora estimada de llegada: {}", self.get_arrival_time()));
        ui.label(format!(
            "Posición de llegada: ({:.2}, {:.2})",
            self.get_arrival_position().0, self.get_arrival_position().1
        ));
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

    pub fn get_position(&self) -> &(f64, f64) {
        &self.tracking.position
    }

    pub fn set_position(&mut self, position: (f64, f64)) {
        self.tracking.position = position;
    }

    pub fn get_altitude(&self) -> f64 {
        self.tracking.altitude
    }

    pub fn get_departure_airport(&self) -> &String {
        &self.status.departure_airport
    }

    pub fn set_departure_airport(&mut self, departure_airport: String) {
        self.status.departure_airport = departure_airport;
    }

    pub fn get_arrival_airport(&self) -> &String {
        &self.status.arrival_airport
    }

    pub fn set_arrival_airport(&mut self, arrival_airport: String) {
        self.status.arrival_airport = arrival_airport;
    }

    pub fn get_speed(&self) -> f32 {
        self.tracking.speed
    }

    pub fn get_fuel_level(&self) -> f32 {
        self.tracking.fuel_level
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

    pub fn set_arrival_position(&mut self, arrival_position: (f64, f64)) {
        self.tracking.arrival_position = arrival_position;
    }
}

