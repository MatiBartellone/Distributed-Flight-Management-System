use std::sync::{Arc, Mutex};

use egui::{Painter, Response, ScrollArea};
use egui::scroll_area::ScrollBarVisibility::VisibleWhenNeeded;
use walkers::{Plugin, Projector};

use crate::flight::{draw_flight_path, Flight};

#[derive(Clone)]
pub struct Flights {
    pub flights: Vec<Flight>,
    pub on_flight_selected: Arc<Mutex<Option<Flight>>>
}

impl Flights {
    pub fn new(flights: Vec<Flight>, on_flight_selected: Arc<Mutex<Option<Flight>>>) -> Self {
        Self {
            flights,
            on_flight_selected
        }
    }

    pub fn list_flight_codes(&self, ui: &mut egui::Ui) {
        ui.label("Lista de vuelos:");
        ui.add_space(10.0);
        ScrollArea::vertical()
        .scroll_bar_visibility(VisibleWhenNeeded)
        .show(ui, |ui| {
            ui.set_width(ui.available_width());
            for flight in &self.flights {
                ui.label(format!("{}: {}", flight.code, flight.status.get_status()));
            }
        });
    }
}

impl Plugin for Flights {
    fn run(&mut self, response: &Response, painter: Painter, projector: &Projector) {
        // Dibuja todos los vuelos
        for flight in &self.flights {
            flight.draw(response, painter.clone(), projector, &self.on_flight_selected);
        }

        // Si hay avion seleccionado dibuja la linea al aeropuerto
        if let Some(flight) = self.on_flight_selected.lock().unwrap().as_ref() {
            draw_flight_path(painter, projector, flight);
        }
    }
}