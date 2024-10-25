use egui::{vec2, Align2, Color32, FontId, Painter, Pos2, Response};
use std::{
    fs::read,
    sync::{Arc, Mutex},
};
use walkers::{extras::Image, Position, Projector, Texture};

use crate::airport::airports::{calculate_angle_to_airport, get_airport_position};

use super::{flight_selected::FlightSelected, flight_status::FlightStatus, flights::{get_flight_pos2, get_flight_vec2}};

#[derive(Clone, PartialEq)]
pub struct Flight {
    // weak consistency
    pub position: (f64, f64),
    // strong consistency
    pub code: String,
    pub status: FlightStatus,
    pub arrival_airport: String,
}

impl Flight {
    pub fn draw(
        &self,
        response: &Response,
        painter: Painter,
        projector: &Projector,
        on_flight_selected: &Arc<Mutex<Option<FlightSelected>>>,
    ) {
        //self.draw_icon_flight(painter.clone(), projector);
        self.draw_image_flight(response, painter.clone(), projector);
        self.clickeable_flight(response, projector, on_flight_selected);
        self.holdeable_flight(response, painter, projector);
    }

    fn holdeable_flight(&self, response: &Response, painter: Painter, projector: &Projector) {
        let screen_flight_position = get_flight_pos2(&self.position, projector);
        if self.is_hovering_on_flight(response, screen_flight_position) {
            self.draw_description(painter, projector);
        }
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

    // Dibuja el icono del avion en su posicion
    pub fn draw_icon_flight(&self, painter: Painter, projector: &Projector) {
        let screen_flight_position = get_flight_pos2(&self.position, projector);
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

        let screen_airport_position = get_airport_position(&self.arrival_airport, projector);
        let angle =
            calculate_angle_to_airport(get_flight_pos2(&self.position, projector), screen_airport_position);
        image.angle(angle);

        image.scale(0.6, 0.6);
        image.draw(response, painter, projector);
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
                Some(flight) if flight.code == self.code => None,
                // Si no estaba seleccionado el lo selecciona
                Some(_) | None => {
                    let mut flight_selected = FlightSelected::default();
                    flight_selected.set_code(&self.code);
                    Some(flight_selected)
                }
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
}