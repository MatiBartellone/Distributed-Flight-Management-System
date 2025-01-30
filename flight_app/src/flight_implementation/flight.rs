use egui::{vec2, Align2, Color32, FontId, Painter, Pos2, Response, Stroke};
use std::{
    fs::read,
    sync::{Arc, Mutex},
};
use walkers::{extras::Image, Position, Projector, Texture};

use crate::airport_implementation::airports::{calculate_angle_to_airport, get_airport_screen_position};

use super::{
    flight_selected::FlightSelected,
    flight_state::FlightState,
    flights::{get_flight_pos2, get_flight_vec2},
};

#[derive(PartialEq, Default, Debug)]
pub struct Flight {
    // weak consistency
    pub position: (f64, f64),
    pub arrival_position: (f64, f64),
    // strong consistency
    pub code: String,
    pub status: FlightState,
    pub arrival_airport: String,
}

impl Flight {
    /// Draw the flight in the screen with its icon and information when hovering
    /// If the flight is clicked, it will change the selected flight
    pub fn draw(
        &self,
        response: &Response,
        painter: Painter,
        projector: &Projector,
        selected_flight: &Arc<Mutex<Option<FlightSelected>>>,
        selected_flight_code: &Option<String>,
    ) {
        self.draw_image_flight(response, painter.clone(), projector);
        self.clickeable_flight(response, projector, selected_flight);
        self.holdeable_flight(response, painter.clone(), projector);
        let Some(selected_flight_code) = selected_flight_code else {return};
        if &self.code == selected_flight_code {
            self.draw_flight_path(painter, projector);
        }
    }

    fn holdeable_flight(&self, response: &Response, painter: Painter, projector: &Projector) {
        let screen_flight_position = get_flight_pos2(&self.position, projector);
        if self.is_hovering_on_flight(response, screen_flight_position) {
            self.draw_description(painter, projector);
        }
    }

    /// Draw the flight path of the selected flight on the map
    fn draw_flight_path(&self, painter: Painter, projector: &Projector) {
        let screen_airport_position = get_airport_screen_position(&self.arrival_position, projector);
        let screen_flight_position = get_flight_pos2(&self.position, projector);
        draw_flight_curve(painter, screen_flight_position, screen_airport_position);
    }

    fn draw_description(&self, painter: Painter, projector: &Projector) {
        let screen_flight_position = get_flight_vec2(&self.position, projector);

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

    fn draw_icon_flight(&self, painter: Painter, projector: &Projector) {
        let screen_flight_position = get_flight_pos2(&self.position, projector);
        painter.text(
            screen_flight_position,
            Align2::CENTER_CENTER,
            '✈'.to_string(),
            FontId::proportional(20.0),
            Color32::BLACK,
        );
    }

    // Draw the airplane image (or icon if the image is not found) in the screen
    fn draw_image_flight(&self, response: &Response, painter: Painter, projector: &Projector) {
        let airplane_texture = match self.image_texture(&painter) {
            Ok(airplane_texture) => airplane_texture,
            Err(_) => {
                self.draw_icon_flight(painter, projector);
                return;
            }
        };
        let flight_position = Position::from_lon_lat(self.position.0, self.position.1);
        let mut image = Image::new(airplane_texture, flight_position);

        let screen_airport_position = get_airport_screen_position(&self.arrival_position, projector);
        let angle = calculate_angle_to_airport(
            get_flight_pos2(&self.position, projector),
            screen_airport_position,
        );
        image.angle(angle);

        image.scale(0.6, 0.6);
        image.draw(response, painter, projector);
    }

    fn image_texture(&self, painter: &Painter) -> Result<Texture, ()> {
        let image_data = read("src/img/flight32.png").map_err(|_| ())?;
        let airplane_texture = Texture::new(&image_data, painter.ctx()).map_err(|_| ())?;
        Ok(airplane_texture)
    }

    // Si lo clikea cambia el vuelo seleccionado
    fn clickeable_flight(
        &self,
        response: &Response,
        projector: &Projector,
        on_flight_selected: &Arc<Mutex<Option<FlightSelected>>>,
    ) {
        let screen_flight_position = get_flight_pos2(&self.position, projector);
        if self.is_hovering_on_flight(response, screen_flight_position)
            && response.clicked_by(egui::PointerButton::Primary)
        {
            let mut selected_flight = match on_flight_selected.lock() {
                Ok(lock) => lock,
                Err(_) => return,
            };
            *selected_flight = match &*selected_flight {
                // Si lo vuelve a clickear lo deseleciona
                Some(flight) if flight.get_code() == self.code => None,
                // Si no estaba seleccionado lo selecciona
                Some(_) | None => {
                    let mut flight_selected = FlightSelected::default();
                    flight_selected.set_code(self.code.to_string());
                    flight_selected.set_position(self.position);
                    flight_selected.set_arrival_airport(self.arrival_airport.to_string());
                    flight_selected.set_arrival_position(self.arrival_position);
                    Some(flight_selected)
                }
            }
        }
    }

    fn is_hovering_on_flight(&self, response: &Response, screen_flight_position: Pos2) -> bool {
        response.hover_pos().is_some_and(|pos| {
            let airplane_size = egui::Vec2::new(30.0, 30.0);
            let airplane_rect = egui::Rect::from_center_size(screen_flight_position, airplane_size);
            airplane_rect.contains(pos)
        })
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
