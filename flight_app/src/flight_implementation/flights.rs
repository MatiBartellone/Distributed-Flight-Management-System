use std::sync::{Arc, Mutex};

use egui::scroll_area::ScrollBarVisibility::VisibleWhenNeeded;
use egui::{Painter, Pos2, Response, ScrollArea, Vec2};
use walkers::{Plugin, Position, Projector};

use super::flight::Flight;
use super::flight_selected::FlightSelected;

pub struct Flights {
    pub flights: Arc<Mutex<Vec<Flight>>>,
    pub on_flight_selected: Arc<Mutex<Option<FlightSelected>>>,
}

impl Flights {
    pub fn new(
        flights: Vec<Flight>,
        on_flight_selected: Arc<Mutex<Option<FlightSelected>>>,
    ) -> Self {
        Self {
            flights: Arc::new(Mutex::new(flights)),
            on_flight_selected,
        }
    }

    /// List all the information of the flights
    pub fn list_flights(&self, ui: &mut egui::Ui) {
        ui.label("Lista de vuelos:");
        ui.add_space(10.0);
        ScrollArea::vertical()
            .scroll_bar_visibility(VisibleWhenNeeded)
            .show(ui, |ui| {
                ui.set_width(ui.available_width());

                let flights = match self.flights.lock() {
                    Ok(lock) => lock,
                    Err(_) => return,
                };

                for flight in flights.iter() {
                    let status_text = flight.status.to_string();
                    let status_color = flight.status.get_color();
                
                    ui.horizontal(|ui| {
                        ui.label(flight.code.to_string() + ": ");
                        ui.label(egui::RichText::new(status_text).color(status_color));
                    });
                }
            });
    }

    fn get_flight_selected_code(&self) -> Option<String> {
        let on_flight_selected = match self.on_flight_selected.lock() {
            Ok(lock) => lock,
            Err(_) => return None,
        };
        if let Some(flight_selected) = &*on_flight_selected {
            return Some(flight_selected.get_code());
        }
        None
    }

    fn draw_flights(&self, response: &Response, painter: &Painter, projector: &Projector) {
        let flights = match self.flights.lock() {
            Ok(lock) => lock,
            Err(_) => return,
        };
        let selected_code = self.get_flight_selected_code();
        for flight in flights.iter() {
            flight.draw(
                response,
                painter.clone(),
                projector,
                &self.on_flight_selected,
                &selected_code
            );
        }
    }

}

impl Plugin for &mut Flights {
    /// Draw the flights in the screen
    fn run(&mut self, response: &Response, painter: Painter, projector: &Projector) {
        self.draw_flights(response, &painter, projector);
    }
}

/// Get the position of the flight in the screen
pub fn get_flight_pos2(position: &(f64, f64), projector: &Projector) -> Pos2 {
    get_flight_vec2(position, projector).to_pos2()
}

/// Get the vec of the flight
pub fn get_flight_vec2(position: &(f64, f64), projector: &Projector) -> Vec2 {
    let flight_coordinates = position;
    let flight_position = Position::from_lon_lat(flight_coordinates.0, flight_coordinates.1);
    projector.project(flight_position)
}
