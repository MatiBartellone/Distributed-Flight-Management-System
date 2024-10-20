use eframe::egui;

use crate::flight_app::FlightApp;

pub struct InformationPanel;

impl InformationPanel {
    pub fn ui(&self, ui: &mut egui::Ui, app: &mut FlightApp) {
        if ui.button("Volver").clicked() {
            app.selected_airport = None;
        }

        if let Some(airport) = &app.selected_airport {
            ui.heading(format!("Aeropuerto {}", airport));
        }

        ui.separator();

        let mut selected_flight_lock = match app.selected_flight.lock() {
            Ok(lock) => lock,
            Err(_) => return
        };

        // Si hay un vuelo seleccionado muestra su informacion detallada, sino lista todos
        if let Some(selected_flight) = &*selected_flight_lock {
            selected_flight.list_information(ui);
            if ui.button("Volver a la lista de vuelos").clicked() {
                *selected_flight_lock = None;
            }
        } else {
            app.flights.list_flight_codes(ui);
        }
    }
}