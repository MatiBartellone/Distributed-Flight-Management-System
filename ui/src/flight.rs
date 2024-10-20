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
    pub fn draw(&self, response: &Response, painter: Painter, projector: &Projector) {
        let flight_position = Position::from_lon_lat(self.position.0, self.position.1);
        let screen_position = projector.project(flight_position);
        painter.text(
            screen_position.to_pos2(),
            Align2::CENTER_CENTER,
            '✈'.to_string(),
            FontId::proportional(20.0),
            Color32::BLACK,
        );
        
        if response.clicked_by(egui::PointerButton::Primary) {
    
        }
    }

    pub fn list_information(&self, ui: &mut egui::Ui) {
        ui.label(format!("Vuelo: {}", self.code));
        ui.label(format!("Posición: ({}, {})", self.position.0, self.position.1));
        ui.label(format!("Altitud: {}", self.altitude));
        ui.label(format!("Velocidad: {}", self.speed));
    }
}