use std::{fs::read, sync::{Arc, Mutex}};
use egui::{Align2, Color32, FontId, Painter, Pos2, Response, Stroke};
use walkers::{ extras::Image, Position, Projector, Texture};

use crate::{airports::{calculate_angle_to_airport, get_arrival_airport_position}, flight_status::FlightStatus};


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
        self.draw_image_flight(response, painter, projector);
        self.clickeable_flight(response, projector, on_flight_selected);
    }

    // Dibuja el icono del avion en su posicion 
    pub fn draw_icon_flight(&self, painter: Painter, projector: &Projector) {
        let screen_flight_position = self.get_flight_pos(projector);
        painter.text(
            screen_flight_position,
            Align2::CENTER_CENTER,
            '✈'.to_string(),
            FontId::proportional(20.0),
            Color32::BLACK,
        );
    }

    pub fn draw_image_flight(&self, response: &Response, painter: Painter, projector: &Projector){
        let image_data = read("src/image.png").expect("Failed to load image");
        let airplane_texture = Texture::new(&image_data, &painter.ctx()).unwrap();
        let flight_position = Position::from_lon_lat(self.position.0, self.position.1);
        let mut image = Image::new(airplane_texture, flight_position);

        let screen_airport_position = get_arrival_airport_position(self, projector);
        let angle = calculate_angle_to_airport(self.get_flight_pos(projector), screen_airport_position);
        image.angle(angle);

        image.scale(0.1, 0.1);
        image.draw(response, painter, projector);
    }

    // Si lo clikea cambia el vuelo seleccionado
    fn clickeable_flight(&self, response: &Response, projector: &Projector, on_flight_selected: &Arc<Mutex<Option<Flight>>>){
        let screen_flight_position = self.get_flight_pos(projector);
        if response.hover_pos().map_or(false, |pos| {
            let airplane_size = egui::Vec2::new(30.0, 30.0);
            let airplane_rect = egui::Rect::from_center_size(screen_flight_position, airplane_size);
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
    let screen_airport_position = get_arrival_airport_position(flight, projector);
    let screen_flight_position = flight.get_flight_pos(projector);
    draw_flight_line(painter, screen_flight_position, screen_airport_position);
}

fn draw_flight_line(painter: &Painter, start: Pos2, end: Pos2) {
    painter.line_segment([start, end], Stroke::new(2.0, Color32::from_rgb(0, 255, 0)));
}