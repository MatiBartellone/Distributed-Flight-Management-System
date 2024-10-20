use std::sync::{Arc, Mutex};

use egui::{Align2, Color32, FontId, Painter, Response};
use walkers::{ Position, Projector};


#[derive(Clone)]
pub struct Flight {
    pub code: String,
    pub position: (f64, f64),
    pub altitude: f64,
    pub speed: f32
}

impl Flight {
    pub fn draw(&self, response: &Response, painter: Painter, projector: &Projector, on_flight_selected: &Arc<Mutex<Option<Flight>>>) {
        // Dibuja el icono del avion en su posicion 
        let flight_position = Position::from_lon_lat(self.position.0, self.position.1);
        let screen_position = projector.project(flight_position);
        painter.text(
            screen_position.to_pos2(),
            Align2::CENTER_CENTER,
            '✈'.to_string(),
            FontId::proportional(20.0),
            Color32::BLACK,
        );
        
        // Si lo clikea cambia el vuel oseleccionado
        if response.hover_pos().map_or(false, |pos| {
            let airplane_size = egui::Vec2::new(30.0, 30.0);
            let airplane_rect = egui::Rect::from_center_size(screen_position.to_pos2(), airplane_size);
            airplane_rect.contains(pos)
        }) && response.clicked_by(egui::PointerButton::Primary) {
            if let Ok(mut lock) = on_flight_selected.lock() {
                *lock = Some(self.clone());
            }
        }
    }

    pub fn list_information(&self, ui: &mut egui::Ui) {
        ui.label(format!("Vuelo: {}", self.code));
        ui.label(format!("Posición: ({}, {})", self.position.0, self.position.1));
        ui.label(format!("Altitud: {}", self.altitude));
        ui.label(format!("Velocidad: {}", self.speed));
    }
}