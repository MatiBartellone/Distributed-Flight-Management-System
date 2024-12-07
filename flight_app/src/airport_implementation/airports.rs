use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use egui::scroll_area::ScrollBarVisibility::VisibleWhenNeeded;
use egui::{Painter, Pos2, Response, ScrollArea};
use walkers::{Plugin, Position, Projector};

use crate::flight_implementation::flight_selected::FlightSelected;

use super::airport::Airport;

pub struct Airports {
    pub airports: HashMap<String, Airport>, 
    pub selected_airport_code: Arc<Mutex<Option<String>>>,
    pub selected_flight: Arc<Mutex<Option<FlightSelected>>>,
}

impl Airports {
    pub fn new(airports: HashMap<String, Airport>, selected_airport_code: Arc<Mutex<Option<String>>>, selected_flight: Arc<Mutex<Option<FlightSelected>>>) -> Self {
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

    /// Set the airports to the airports list
    pub fn set_airports(&mut self, airports: HashMap<String, Airport>) {
        self.airports = airports;
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
                airport.draw(response, painter.clone(), projector, &self.selected_airport_code, &self.selected_flight);
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

        let Some(flight) = &*selected_flight else {
            return false;
        };

        let departure_airport = self.airports.get(flight.get_departure_airport());
        let arrival_airport = self.airports.get(flight.get_arrival_airport());
        drop(selected_flight); // Unlock the selected flight to use it in the draw method

        if let Some(airport) = departure_airport {
            airport.draw(response, painter.clone(), projector, &self.selected_airport_code, &self.selected_flight);
        }
        if let Some(airport) = arrival_airport {
            airport.draw(response, painter.clone(), projector, &self.selected_airport_code, &self.selected_flight);
        }
        true
    }
}

impl Plugin for &mut Airports {
    /// Could be three cases:
    /// 1. Draw the selected flight airports
    /// 2. Draw the selected airport
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
                &self.selected_flight,
            );
        }
    }
}

/// Get the position of an airport given its coordinates
pub fn get_airport_screen_position(airport_coordinates: &(f64, f64), projector: &Projector) -> Pos2 {
    let airport_position = Position::from_lon_lat(airport_coordinates.0, airport_coordinates.1);
    projector.project(airport_position).to_pos2()
}

/// Calculate the angle between the flight and the airport
pub fn calculate_angle_to_airport(flight_position: Pos2, airport_position: Pos2) -> f32 {
    let delta_x = airport_position.x - flight_position.x;
    let delta_y = airport_position.y - flight_position.y;
    delta_y.atan2(delta_x)
}