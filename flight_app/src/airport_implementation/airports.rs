use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use egui::scroll_area::ScrollBarVisibility::VisibleWhenNeeded;
use egui::{Painter, Pos2, Response, ScrollArea};
use walkers::{Plugin, Position, Projector};

use crate::flight_implementation::flight_selected::FlightSelected;

use super::airport::Airport;

#[derive(Clone)]
pub struct Airports {
    pub airports: HashMap<String, Airport>, 
    pub selected_airport_code: Arc<Mutex<Option<String>>>,
    pub selected_flight: Arc<Mutex<Option<FlightSelected>>>,
}

impl Airports {
    pub fn new(airports: Vec<Airport>, selected_airport_code: Arc<Mutex<Option<String>>>, selected_flight: Arc<Mutex<Option<FlightSelected>>>) -> Self {
        let airports = airports
            .into_iter()
            .map(|airport| (airport.code.to_string(), airport))
            .collect();

        Self {
            airports,
            selected_airport_code,
            selected_flight,
        }
    }

    /// List all airports in the UI with their information
    pub fn list_airports(&self, ui: &mut egui::Ui) {
        ScrollArea::vertical()
            .scroll_bar_visibility(VisibleWhenNeeded)
            .show(ui, |ui| {
                ui.set_width(ui.available_width());
                for airport in self.airports.values() {
                    airport.list_information(ui);
                    ui.separator();
                }
            });
    }

    /// Get the coordinates of an airport given its code
    pub fn get_airport_coordinates(&self, airport_code: &str) -> (f64, f64) {
        self.airports
            .get(airport_code)
            .map(|airport| airport.position)
            .unwrap_or((0.0, 0.0))
    }

    /// Get the name of an airport given its code
    pub fn get_aiport_name(&self, airport_code: &str) -> String {
        self.airports
            .get(airport_code)
            .map(|airport| airport.name.to_string())
            .unwrap_or("".to_string())
    }

    fn draw_selected_airport(&self, response: &Response, painter: &Painter, projector: &Projector) -> bool {
        let selected_airport_code = match self.selected_airport_code.lock() {
            Ok(lock) => lock,
            Err(_) => return false,
        };

        if let Some(code) = &*selected_airport_code {
            if let Some(airport) = self.airports.get(code) {
                drop(selected_airport_code);
                airport.draw(response, painter.clone(), projector, &self.selected_airport_code);
                return true;
            }
        }
        false
    }

    fn draw_selected_flight_airports(&self, response: &Response, painter: &Painter, projector: &Projector) -> bool {
        let selected_flight = match self.selected_flight.lock() {
            Ok(lock) => lock,
            Err(_) => return false,
        };

        if let Some(flight) = &*selected_flight {
            if let Some(airport) = self.airports.get(flight.get_departure_airport()) {
                airport.draw(response, painter.clone(), projector, &self.selected_airport_code);
            }
            if let Some(airport) = self.airports.get(flight.get_arrival_airport()) {
                airport.draw(response, painter.clone(), projector, &self.selected_airport_code);
                flight.draw_flight_path(painter.clone(), projector, airport.position);
            }
            return true;
        }
        false
    }
}

impl Plugin for &mut Airports {
    /// Could be three cases:
    /// 1. Draw the selected airport
    /// 2. Draw the selected flight airport
    /// 3. Draw all airports if there is no selected airport or flight
    fn run(&mut self, response: &Response, painter: Painter, projector: &Projector) {
        if self.draw_selected_flight_airports(response, &painter, projector) {
            return;
        }
        if self.draw_selected_airport(response, &painter, projector){
            return;
        }
        for airport in self.airports.values() {
            airport.draw(
                response,
                painter.clone(),
                projector,
                &self.selected_airport_code,
            );
        }
    }
}

/// Get the position of an airport given its code
pub fn get_airport_position(airport: &str, projector: &Projector) -> Pos2 {
    let airport_coordinates = get_airport_coordinates(airport);
    let airport_position = Position::from_lon_lat(airport_coordinates.0, airport_coordinates.1);
    projector.project(airport_position).to_pos2()
}

/// Get the position of an airport given its coordinates
pub fn get_airport_screen_position(airport_coordinates: (f64, f64), projector: &Projector) -> Pos2 {
    let airport_position = Position::from_lon_lat(airport_coordinates.0, airport_coordinates.1);
    projector.project(airport_position).to_pos2()
}

/// Calculate the angle between the flight and the airport
pub fn calculate_angle_to_airport(flight_position: Pos2, airport_position: Pos2) -> f32 {
    let delta_x = airport_position.x - flight_position.x;
    let delta_y = airport_position.y - flight_position.y;
    delta_y.atan2(delta_x)
}

/// Get the coordinates of an airport given its code
pub fn get_airport_coordinates(airport: &str) -> (f64, f64) {
    match airport {
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