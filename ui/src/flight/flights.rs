use std::sync::{Arc, Mutex};

use egui::scroll_area::ScrollBarVisibility::VisibleWhenNeeded;
use egui::{Painter, Response, ScrollArea};
use walkers::{Plugin, Projector};

use super::flight::Flight;

#[derive(Clone)]
pub struct Flights {
    pub flights: Arc<Mutex<Vec<Flight>>>,
    pub on_flight_selected: Arc<Mutex<Option<Flight>>>,
}

impl Flights {
    pub fn new(flights: Vec<Flight>, on_flight_selected: Arc<Mutex<Option<Flight>>>) -> Self {
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
                    ui.label(format!("{}: {}", flight.code, flight.status.get_status()));
                }
            });
    }
}

impl Plugin for &mut Flights {
    fn run(&mut self, response: &Response, painter: Painter, projector: &Projector) {
        let flights = match self.flights.lock() {
            Ok(lock) => lock,
            Err(_) => return,
        };
        // Dibuja todos los vuelos
        for flight in flights.iter() {
            flight.draw(
                response,
                painter.clone(),
                projector,
                &self.on_flight_selected,
            );
        }

        // Si hay avion seleccionado dibuja la linea al aeropuerto
        if let Some(flight) = self.on_flight_selected.lock().unwrap().as_ref() {
            flight.draw_flight_path(painter, projector);
        }
    }
}
