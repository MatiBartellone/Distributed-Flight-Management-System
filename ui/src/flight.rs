use std::sync::{Arc, Mutex};

use egui::{Align2, Color32, FontId, Painter, Pos2, Response};
use walkers::{ Position, Projector};

use crate::{airports::get_airport_coordinates, flight_status::FlightStatus};


#[derive(Clone)]
pub struct Flight {
    pub code: String,               
    // weak consistency
    pub position: (f64, f64),    
    pub altitude: f64,           
    pub speed: f32,              
    pub fuel_level: f32,         
    // strong consistency
    pub status: FlightStatus,    
    pub departure_airport: String, 
    pub arrival_airport: String,   
    pub departure_time: String,    
    pub arrival_time: String,      
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
        
        // Si lo clikea cambia el vuelo seleccionado
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
        ui.label(format!("{}", self.code));
        ui.label(format!("Posición: ({:.2}, {:.2})", self.position.0, self.position.1));
        ui.label(format!("Altitud: {} ft", self.altitude));
        ui.label(format!("Velocidad: {} km/h", self.speed));
        ui.label(format!("Nivel de combustible: {}%", self.fuel_level));
        ui.label(format!("Estado: {}", self.status.get_status()));
        ui.label(format!("Aeropuerto de salida: {}", self.departure_airport));
        ui.label(format!("Hora de salida: {}", self.departure_time));
        ui.label(format!("Aeropuerto de llegada: {}", self.arrival_airport));
        ui.label(format!("Hora estimada de llegada: {}", self.arrival_time));
    }

    pub fn get_flight_pos(&self, projector: &Projector) -> Pos2 {
        let flight_coordinates = self.position;
        let flight_position = Position::from_lon_lat(flight_coordinates.0, flight_coordinates.1);
        projector.project(flight_position).to_pos2()        
    }
}

pub fn draw_flight_path(painter: Painter, projector: &Projector, flight: &Flight) {
    // Position aeropuerto
    let airport_coordinates = get_airport_coordinates(&Some(flight.arrival_airport.to_string()));
    let airport_position = Position::from_lon_lat(airport_coordinates.0, airport_coordinates.1);
    let screen_airport_position = projector.project(airport_position).to_pos2();

    // Position vuelo
    let screen_flight_position = flight.get_flight_pos(projector);

    painter.line_segment(
        [screen_flight_position, screen_airport_position],
        egui::Stroke::new(2.0, egui::Color32::from_rgb(0, 255, 0)),
    );
}