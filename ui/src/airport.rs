use std::sync::{Arc, Mutex};

use eframe::Error;
use egui::{vec2, Align2, Color32, FontId, Painter, Pos2, Response, Vec2};
use walkers::{Position, Projector};

use crate::{flight::Flight, flight_status::FlightStatus};

#[derive(Clone, PartialEq)]
pub struct Airport {
    pub name: String,
    pub code: String,
    pub position: (f64, f64)   
}

// Comunicacion con Cassandra o informacion de aeropuerto
impl Airport {
    pub fn new(name: String, code: String, position: (f64, f64)) -> Self {
        Airport { name, code, position }
    }
    
    pub fn get_flights(&self) -> Result<Vec<Flight>, Error> {
        Ok(vec![
            Flight {
                code: "AR1130".to_string(), // Aerolíneas Argentinas
                position: (-75.7787, 41.6413), // Más lejos de JFK
                altitude: 32000.0,
                speed: 560.0,
                fuel_level: 85.0, // Nivel de combustible (porcentaje)
                status: FlightStatus::OnTime, // Estado del vuelo
                departure_airport: "EZE".to_string(), // Ezeiza
                departure_time: "08:30".to_string(), // Hora de salida
                arrival_airport: "JFK".to_string(), // JFK
                arrival_time: "16:45".to_string(), // Hora estimada de llegada
            },
            Flight {
                code: "LA8050".to_string(), // LATAM Airlines
                position: (-82.2903, 27.7617), // Más lejos de MIA
                altitude: 34000.0,
                speed: 580.0,
                fuel_level: 78.0, // Nivel de combustible
                status: FlightStatus::Delayed, // Estado del vuelo retrasado
                departure_airport: "SCL".to_string(), // Santiago de Chile
                departure_time: "09:15".to_string(), // Hora de salida
                arrival_airport: "MIA".to_string(), // Miami
                arrival_time: "17:00".to_string(), // Hora estimada de llegada
            },
            Flight {
                code: "AA940".to_string(), // American Airlines
                position: (-45.6350, -22.5505), // Más lejos de GRU
                altitude: 30000.0,
                speed: 550.0,
                fuel_level: 65.0, // Nivel de combustible
                status: FlightStatus::OnTime, // Estado del vuelo
                departure_airport: "DFW".to_string(), // Dallas/Fort Worth
                departure_time: "07:00".to_string(), // Hora de salida
                arrival_airport: "GRU".to_string(), // Sao Paulo-Guarulhos
                arrival_time: "15:30".to_string(), // Hora estimada de llegada
            },
            Flight {
                code: "IB6844".to_string(), // Iberia
                position: (-62.3019, -37.6083), // Más lejos de EZE
                altitude: 31000.0,
                speed: 570.0,
                fuel_level: 72.0, // Nivel de combustible
                status: FlightStatus::OnTime, // Estado del vuelo
                departure_airport: "MAD".to_string(), // Madrid
                departure_time: "10:00".to_string(), // Hora de salida
                arrival_airport: "EZE".to_string(), // Ezeiza
                arrival_time: "18:00".to_string(), // Hora estimada de llegada
            },
            Flight {
                code: "AF2280".to_string(), // Air France
                position: (-120.4085, 35.9416), // Más lejos de LAX
                altitude: 33000.0,
                speed: 590.0,
                fuel_level: 80.0, // Nivel de combustible
                status: FlightStatus::Cancelled, // Estado del vuelo cancelado
                departure_airport: "CDG".to_string(), // París Charles de Gaulle
                departure_time: "12:30".to_string(), // Hora de salida
                arrival_airport: "LAX".to_string(), // Los Ángeles
                arrival_time: "20:45".to_string(), // Hora estimada de llegada
            },
            Flight {
                code: "KL7028".to_string(), // KLM
                position: (-123.4194, 38.7749), // Más lejos de SFO
                altitude: 32000.0,
                speed: 600.0,
                fuel_level: 60.0, // Nivel de combustible
                status: FlightStatus::OnTime, // Estado del vuelo
                departure_airport: "AMS".to_string(), // Ámsterdam
                departure_time: "11:45".to_string(), // Hora de salida
                arrival_airport: "SFO".to_string(), // San Francisco
                arrival_time: "20:10".to_string(), // Hora estimada de llegada
            },
            Flight {
                code: "BA246".to_string(), // British Airways
                position: (-3.4543, 51.4700), // Más lejos de LHR
                altitude: 31000.0,
                speed: 575.0,
                fuel_level: 77.0, // Nivel de combustible
                status: FlightStatus::OnTime, // Estado del vuelo
                departure_airport: "LHR".to_string(), // Londres Heathrow
                departure_time: "14:00".to_string(), // Hora de salida
                arrival_airport: "EZE".to_string(), // Ezeiza
                arrival_time: "17:30".to_string(), // Hora estimada de llegada
            },
            Flight {
                code: "JL704".to_string(), // Japan Airlines
                position: (140.3929, 35.6735), // Más lejos de NRT
                altitude: 33000.0,
                speed: 580.0,
                fuel_level: 70.0, // Nivel de combustible
                status: FlightStatus::OnTime, // Estado del vuelo
                departure_airport: "NRT".to_string(), // Tokio Narita
                departure_time: "16:00".to_string(), // Hora de salida
                arrival_airport: "LAX".to_string(), // Los Ángeles
                arrival_time: "11:00".to_string(), // Hora estimada de llegada
            },
        ])
    }
    
    pub fn list_information(&self, ui: &mut egui::Ui) {
        ui.label(format!("{} ({})", self.name, self.code));
    }

    pub fn get_airport_pos2(&self, projector: &Projector) -> Pos2 {
        self.get_flight_vec2(projector).to_pos2()        
    }

    pub fn get_flight_vec2(&self, projector: &Projector) -> Vec2 {
        let airport_coordinates = self.position;
        let airport_position = Position::from_lon_lat(airport_coordinates.0, airport_coordinates.1);
        projector.project(airport_position)      
    }
}

// Interfaz grafica
impl Airport {
    pub fn draw(&self, response: &Response, painter: Painter, projector: &Projector, on_airport_selected: &Arc<Mutex<Option<Airport>>>) {
        self.draw_icon_airport(painter.clone(), projector);
        self.clickeable_airport(response, projector, on_airport_selected);
        self.holdeable_airport(response, painter, projector);
    }

    fn holdeable_airport(&self, response: &Response, painter: Painter, projector: &Projector) {
        let screen_flight_position = self.get_airport_pos2(projector);
        if self.is_hovering_on_airport(response, screen_flight_position) {
            self.draw_description(painter, projector);
        }
    }

    fn draw_description(&self, painter: Painter, projector: &Projector){
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
    pub fn draw_icon_airport(&self, painter: Painter, projector: &Projector) {
        let screen_airport_position = self.get_airport_pos2(projector);
        painter.text(
            screen_airport_position,
            Align2::CENTER_CENTER,
            // 📍, 🏫, 📌
            '🏫'.to_string(),
            FontId::proportional(20.0),
            Color32::BLACK,
        );
    }

    // Si lo clikea cambia el aeropuerto seleccionado
    fn clickeable_airport(&self, response: &Response, projector: &Projector, on_airport_selected: &Arc<Mutex<Option<Airport>>>){
        let screen_airport_position = self.get_airport_pos2(projector);
        if self.is_hovering_on_airport(response, screen_airport_position)&& response.clicked_by(egui::PointerButton::Primary) {
            let mut selected_airport = match on_airport_selected.lock() {
                Ok(lock) => lock,
                Err(_) => return,
            };
            match &*selected_airport {
                Some(airport) if airport == self => *selected_airport = None,
                Some(_) | None => *selected_airport = Some(self.clone())
            }
        }
    }

    fn is_hovering_on_airport(&self, response: &Response, screen_airport_position: Pos2) -> bool {
        response.hover_pos().map_or(false, |pos| {
            let airplane_size = egui::Vec2::new(30.0, 30.0);
            let airplane_rect = egui::Rect::from_center_size(screen_airport_position, airplane_size);
            airplane_rect.contains(pos)
        })
    }
}