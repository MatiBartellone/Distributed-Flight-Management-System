use std::sync::{Arc, Mutex};

use egui::scroll_area::ScrollBarVisibility::VisibleWhenNeeded;
use egui::{Painter, Pos2, Response, ScrollArea};
use walkers::{Plugin, Position, Projector};

use super::airport::Airport;

#[derive(Clone)]
pub struct Airports {
    pub airports: Vec<Airport>,
    pub on_airport_selected: Arc<Mutex<Option<Airport>>>,
}

impl Airports {
    pub fn new(airports: Vec<Airport>, on_airport_selected: Arc<Mutex<Option<Airport>>>) -> Self {
        Self {
            airports,
            on_airport_selected,
        }
    }

    pub fn list_airports(&self, ui: &mut egui::Ui) {
        ScrollArea::vertical()
            .scroll_bar_visibility(VisibleWhenNeeded)
            .show(ui, |ui| {
                ui.set_width(ui.available_width());
                for airport in &self.airports {
                    airport.list_information(ui);
                    ui.separator();
                }
            });
    }
}

impl Plugin for &mut Airports {
    fn run(&mut self, response: &Response, painter: Painter, projector: &Projector) {
        // Intenta abrir el lock del aeropuerto seleccionado
        let selected_airport_lock = match self.on_airport_selected.lock() {
            Ok(lock) => lock,
            Err(_) => return,
        };

        if let Some(airport) = &*selected_airport_lock {
            // Si hay un aeropuerto seleccionado dibuja solo ese
            let airport = airport.clone();
            drop(selected_airport_lock);
            airport.draw(
                response,
                painter.clone(),
                projector,
                &self.on_airport_selected,
            );
        } else {
            // Sino dibuja todos los aeropuertos
            drop(selected_airport_lock);
            for airport in &self.airports {
                airport.draw(
                    response,
                    painter.clone(),
                    projector,
                    &self.on_airport_selected,
                );
            }
        }
    }
}

pub fn get_airport_coordinates(airport: &String) -> (f64, f64) {
    match airport.as_str() {
        "JFK" => (-73.7781, 40.6413),  // JFK, Nueva York
        "LAX" => (-118.4085, 33.9416), // LAX, Los Ángeles
        "EZE" => (-58.5358, -34.8222), // EZE, Buenos Aires (Ezeiza)
        "CDG" => (2.55, 49.0097),      // CDG, París Charles de Gaulle
        "LHR" => (-0.4543, 51.4700),   // LHR, Londres Heathrow
        "NRT" => (140.3929, 35.7735),  // NRT, Tokio Narita
        "FRA" => (8.5706, 50.0333),    // FRA, Frankfurt
        "SYD" => (151.1772, -33.9399), // SYD, Sídney Kingsford Smith
        "SCL" => (-70.7853, -33.3929), // SCL, Santiago de Chile
        "MIA" => (-80.2906, 25.7957),  // MIA, Miami
        "DFW" => (-97.0379, 32.8968),  // DFW, Dallas/Fort Worth
        "GRU" => (-46.6506, -23.4253), // GRU, São Paulo-Guarulhos
        "MAD" => (-3.5705, 40.4719),   // MAD, Madrid
        "AMS" => (4.7600, 52.3081),    // AMS, Ámsterdam
        "SFO" => (-122.4194, 37.6213), // SFO, San Francisco
        _ => (0.0, 0.0),
    }
}

pub fn get_airport_position(airport: &str, projector: &Projector) -> Pos2 {
    let airport_coordinates = get_airport_coordinates(&airport.to_string());
    let airport_position = Position::from_lon_lat(airport_coordinates.0, airport_coordinates.1);
    projector.project(airport_position).to_pos2()
}

pub fn calculate_angle_to_airport(flight_position: Pos2, airport_position: Pos2) -> f32 {
    let delta_x = airport_position.x - flight_position.x;
    let delta_y = airport_position.y - flight_position.y;
    delta_y.atan2(delta_x)
}
