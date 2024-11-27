use egui::{Color32, Painter, Pos2, Stroke};
use walkers::Projector;
use crate::airport_implementation::airports::get_airport_screen_position;

use super::{flight_state::FlightState, flights::get_flight_pos2};

#[derive(Default)]
pub struct FlightSelected {
    pub status: FlightStatus,
    pub info: FlightTracking
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
pub struct FlightTracking {
    // weak consistency
    pub position: (f64, f64),
    pub altitude: f64,
    pub speed: f32,
    pub fuel_level: f32,
}


impl FlightSelected {
<<<<<<< HEAD
    pub fn new(info: FlightTracking, status: FlightStatus) -> Self {
        Self { info, status }
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
        ui.label(format!("Estado: {}", self.get_status().to_string()));
        ui.label(format!("Aeropuerto de salida: {}", self.get_departure_airport()));
        ui.label(format!("Hora de salida: {}", self.get_departure_time()));
        ui.label(format!("Aeropuerto de llegada: {}", self.get_arrival_airport()));
        ui.label(format!("Hora estimada de llegada: {}", self.get_arrival_time()));
    }

    /// Draw the flight path of the selected flight on the map
    pub fn draw_flight_path(&self, painter: Painter, projector: &Projector, airport_coordinates: (f64, f64)) {
        let screen_airport_position = get_airport_screen_position(airport_coordinates, projector);
        let screen_flight_position = get_flight_pos2(self.get_position(), projector);
        draw_flight_curve(painter, screen_flight_position, screen_airport_position);
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
        &self.info.position
    }

    pub fn set_position(&mut self, position: (f64, f64)) {
        self.info.position = position;
    }

    pub fn get_altitude(&self) -> f64 {
        self.info.altitude
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
        self.info.speed
    }

    pub fn get_fuel_level(&self) -> f32 {
        self.info.fuel_level
    }

    pub fn get_departure_time(&self) -> &String {
        &self.status.departure_time
    }

    pub fn get_arrival_time(&self) -> &String {
        &self.status.arrival_time
    }
}

fn draw_flight_line(painter: &Painter, start: Pos2, end: Pos2) {
    painter.line_segment([start, end], Stroke::new(2.0, Color32::from_rgb(0, 255, 0)));
}

fn draw_flight_curve(painter: Painter, start: Pos2, end: Pos2) {
    // Calcula la distancia horizontal y vertical
    let dx = start.x - end.x;
    let dy = start.y - end.y;
    let control_height = ((dx.abs() + dy.abs()) / 10.0).min(100.0);

    // Crea un punto de control para la curva
    let control_point = Pos2::new(
        (end.x + start.x) / 2.0,
        (end.y + start.y) / 2.0 - control_height,
    );

    // Dibuja la curva como una serie de segmentos de línea
    let num_segments = 30;
    for i in 0..num_segments {
        let t0 = i as f32 / num_segments as f32;
        let t1 = (i + 1) as f32 / num_segments as f32;

        let p0 = quadratic_bezier(start, control_point, end, t0);
        let p1 = quadratic_bezier(start, control_point, end, t1);
        draw_flight_line(&painter, p0, p1);
    }
}

// Función para calcular un punto en la curva cuadrática
fn quadratic_bezier(p0: Pos2, p1: Pos2, p2: Pos2, t: f32) -> Pos2 {
    let x = (1.0 - t).powi(2) * p0.x + 2.0 * (1.0 - t) * t * p1.x + t.powi(2) * p2.x;
    let y = (1.0 - t).powi(2) * p0.y + 2.0 * (1.0 - t) * t * p1.y + t.powi(2) * p2.y;
    Pos2::new(x, y)
}
