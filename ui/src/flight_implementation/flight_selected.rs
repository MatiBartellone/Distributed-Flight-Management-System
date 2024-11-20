use egui::{Color32, Painter, Pos2, Stroke};
use walkers::Projector;

use crate::airport_implementation::airports::get_airport_position;

use super::{flight_status::FlightStatus, flights::get_flight_pos2};

#[derive(Default)]
pub struct FlightSelected {
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

impl FlightSelected {
    #[allow(clippy::too_many_arguments)]
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
        FlightSelected {
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

    pub fn set_code(&mut self, code: &str) {
        self.code = code.to_string();
    }

    pub fn list_information(&self, ui: &mut egui::Ui) {
        ui.label(&self.code);
        ui.label(format!(
            "Posición: ({:.2}, {:.2})",
            self.position.0, self.position.1
        ));
        ui.label(format!("Altitud: {} ft", self.altitude));
        ui.label(format!("Velocidad: {} km/h", self.speed));
        ui.label(format!("Nivel de combustible: {}%", self.fuel_level));
        ui.label(format!("Estado: {}", self.status.get_status()));
        ui.label(format!("Aeropuerto de salida: {}", self.departure_airport));
        ui.label(format!("Hora de salida: {}", self.departure_time));
        ui.label(format!("Aeropuerto de llegada: {}", self.arrival_airport));
        ui.label(format!("Hora estimada de llegada: {}", self.arrival_time));
    }

    pub fn draw_flight_path(&self, painter: Painter, projector: &Projector) {
        let screen_airport_position = get_airport_position(&self.arrival_airport, projector);
        let screen_flight_position = get_flight_pos2(&self.position, projector);
        draw_flight_curve(painter, screen_flight_position, screen_airport_position);
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
