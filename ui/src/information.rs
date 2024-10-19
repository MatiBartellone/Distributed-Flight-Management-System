use eframe::egui;

use crate::flight_app::FlightApp;

pub struct LeftPanel;

impl LeftPanel {
    pub fn ui(&self, ui: &mut egui::Ui, app: &mut FlightApp) {
        ui.heading("Aeropuerto Ezeiza");

        if let Some(selected_flight) = &app.selected_flight {
            ui.label(format!("Vuelo: {}", selected_flight.code));
            ui.label(format!("Posición: ({}, {})", selected_flight.position.0, selected_flight.position.1));
            ui.label(format!("Altitud: {}", selected_flight.altitude));
            ui.label(format!("Velocidad: {}", selected_flight.speed));

            if ui.button("Volver a la lista de vuelos").clicked() {
                app.selected_flight = None;
            }
        } else {
            for flight in &app.flights {
                ui.label(format!("Vuelo: {}", flight.code));
            }
        }
    }
}