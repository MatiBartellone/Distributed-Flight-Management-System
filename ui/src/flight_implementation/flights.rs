use std::sync::{Arc, Mutex};

use egui::scroll_area::ScrollBarVisibility::VisibleWhenNeeded;
use egui::{Painter, Pos2, Response, ScrollArea, Vec2};
use walkers::{Plugin, Position, Projector};

use super::flight::Flight;
use super::flight_selected::FlightSelected;
use super::flight_state::FlightState;

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
                    let status_color = match flight.status {
                        FlightState::OnTime => egui::Color32::GREEN,
                        FlightState::Delayed => egui::Color32::RED,
                        _ => egui::Color32::default(),
                    };
                
                    ui.horizontal(|ui| {
                        ui.label(flight.code.to_string() + ": ");
                        ui.label(egui::RichText::new(status_text).color(status_color));
                    });
                }
            });
    }

    fn draw_line_to_airport(&self, painter: Painter, projector: &Projector) {
        // Intenta abrir el lock
        let selected_flight = match self.on_flight_selected.lock() {
            Ok(lock) => lock,
            Err(_) => return,
        };

        // Si hay avion seleccionado dibuja la linea al aeropuerto
        if let Some(flight) = &*selected_flight {
            flight.draw_flight_path(painter, projector);
        }
    }

    fn draw_flights(&self, response: &Response, painter: Painter, projector: &Projector) {
        let flights = match self.flights.lock() {
            Ok(lock) => lock,
            Err(_) => return,
        };
        for flight in flights.iter() {
            flight.draw(
                response,
                painter.clone(),
                projector,
                &self.on_flight_selected,
            );
        }
    }

}

impl Plugin for &mut Flights {
    fn run(&mut self, response: &Response, painter: Painter, projector: &Projector) {
        self.draw_flights(response, painter.clone(), projector);
        self.draw_line_to_airport(painter, projector);
    }
}

pub fn get_flight_pos2(position: &(f64, f64), projector: &Projector) -> Pos2 {
    get_flight_vec2(position, projector).to_pos2()
}

pub fn get_flight_vec2(position: &(f64, f64), projector: &Projector) -> Vec2 {
    let flight_coordinates = position;
    let flight_position = Position::from_lon_lat(flight_coordinates.0, flight_coordinates.1);
    projector.project(flight_position)
}
