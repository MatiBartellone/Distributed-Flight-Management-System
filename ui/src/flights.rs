use egui::{Painter, Response};
use walkers::{Plugin, Projector};

use crate::flight::Flight;

#[derive(Clone)]
pub struct Flights {
    pub flights: Vec<Flight>
}

impl Flights {
    pub fn new(flights: Vec<Flight>) -> Self {
        Self {
            flights
        }
    }

    pub fn render_flights(&self, ui: &mut egui::Ui) {
        for flight in &self.flights {
            ui.label(format!("Vuelo: {}", flight.code));
        }
    }
}

impl Plugin for Flights {
    fn run(&mut self, response: &Response, painter: Painter, projector: &Projector) {
        for flight in &self.flights {
            flight.draw(response, painter.clone(), projector);
        }
    }
}