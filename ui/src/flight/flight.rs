use egui::{vec2, Align2, Color32, FontId, Painter, Pos2, Response, Stroke, Vec2};
use std::{
    fs::read,
    sync::{Arc, Mutex},
};
use walkers::{extras::Image, Position, Projector, Texture};

use crate::airport::airports::{calculate_angle_to_airport, get_arrival_airport_position};

use super::flight_status::FlightStatus;

#[derive(Clone, PartialEq)]
pub struct Flight {
    // weak consistency
    pub position: (f64, f64),
    pub altitude: f64,
    pub speed: f32,
    pub fuel_level: f32,
    // strong consistency
    pub code: String,
    pub status: FlightStatus,
    pub departure_airport: String,
    pub arrival_airport: String,
    pub departure_time: String,
    pub arrival_time: String,
}

impl Flight {
    pub fn draw(
        &self,
        response: &Response,
        painter: Painter,
        projector: &Projector,
        on_flight_selected: &Arc<Mutex<Option<Flight>>>,
    ) {
        //self.draw_icon_flight(painter.clone(), projector);
        self.draw_image_flight(response, painter.clone(), projector);
        self.clickeable_flight(response, projector, on_flight_selected);
        self.holdeable_flight(response, painter, projector);
    }

    fn holdeable_flight(&self, response: &Response, painter: Painter, projector: &Projector) {
        let screen_flight_position = self.get_flight_pos2(projector);
        if self.is_hovering_on_flight(response, screen_flight_position) {
            self.draw_description(painter, projector);
        }
    }

    fn draw_description(&self, painter: Painter, projector: &Projector) {
        let screen_flight_position = self.get_flight_vec2(projector);

        let label = painter.layout_no_wrap(
            self.code.to_string(),
            FontId::proportional(12.),
            Color32::from_gray(200),
        );

        let offset = vec2(8., 8.);

        painter.rect_filled(
            label
                .rect
                .translate(screen_flight_position)
                .translate(offset)
                .expand(5.),
            10.,
            Color32::BLACK.gamma_multiply(0.8),
        );

        painter.galley(
            (screen_flight_position + offset).to_pos2(),
            label,
            egui::Color32::BLACK,
        );
    }

    // Dibuja el icono del avion en su posicion
    pub fn draw_icon_flight(&self, painter: Painter, projector: &Projector) {
        let screen_flight_position = self.get_flight_pos2(projector);
        painter.text(
            screen_flight_position,
            Align2::CENTER_CENTER,
            '✈'.to_string(),
            FontId::proportional(20.0),
            Color32::BLACK,
        );
    }

    // Dibuja la imagen del avion en su posicion
    pub fn draw_image_flight(&self, response: &Response, painter: Painter, projector: &Projector) {
        let image_data = read("src/flight32.png").expect("Failed to load image");
        let airplane_texture = Texture::new(&image_data, &painter.ctx()).unwrap();
        let flight_position = Position::from_lon_lat(self.position.0, self.position.1);
        let mut image = Image::new(airplane_texture, flight_position);

        let screen_airport_position = get_arrival_airport_position(self, projector);
        let angle =
            calculate_angle_to_airport(self.get_flight_pos2(projector), screen_airport_position);
        image.angle(angle);

        image.scale(0.6, 0.6);
        image.draw(response, painter, projector);
    }

    // Si lo clikea cambia el vuelo seleccionado
    fn clickeable_flight(
        &self,
        response: &Response,
        projector: &Projector,
        on_flight_selected: &Arc<Mutex<Option<Flight>>>,
    ) {
        let screen_flight_position = self.get_flight_pos2(projector);
        if self.is_hovering_on_flight(response, screen_flight_position)
            && response.clicked_by(egui::PointerButton::Primary)
        {
            let mut selected_flight = match on_flight_selected.lock() {
                Ok(lock) => lock,
                Err(_) => return,
            };
            match &*selected_flight {
                // Si lo vuelve a clickear lo deseleciona
                Some(flight) if flight == self => *selected_flight = None,
                // Si no estaba seleccionado el lo selecciona
                Some(_) | None => *selected_flight = Some(self.clone()),
            }
        }
    }

    fn is_hovering_on_flight(&self, response: &Response, screen_flight_position: Pos2) -> bool {
        response.hover_pos().map_or(false, |pos| {
            let airplane_size = egui::Vec2::new(30.0, 30.0);
            let airplane_rect = egui::Rect::from_center_size(screen_flight_position, airplane_size);
            airplane_rect.contains(pos)
        })
    }

    pub fn list_information(&self, ui: &mut egui::Ui) {
        ui.label(format!("{}", self.code));
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

    pub fn get_flight_pos2(&self, projector: &Projector) -> Pos2 {
        self.get_flight_vec2(projector).to_pos2()
    }

    pub fn get_flight_vec2(&self, projector: &Projector) -> Vec2 {
        let flight_coordinates = self.position;
        let flight_position = Position::from_lon_lat(flight_coordinates.0, flight_coordinates.1);
        projector.project(flight_position)
    }

    pub fn draw_flight_path(&self, painter: Painter, projector: &Projector) {
        let screen_airport_position = get_arrival_airport_position(self, projector);
        let screen_flight_position = self.get_flight_pos2(projector);
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
