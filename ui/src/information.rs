use eframe::egui;

use crate::flight_app::FlightApp;

pub struct InformationPanel;

impl InformationPanel {
    pub fn ui(&self, ui: &mut egui::Ui, app: &mut FlightApp) {
        // Intenta abrir el lock del aeropuerto seleccionado
        let selected_airport_lock = match app.selected_airport.lock() {
            Ok(lock) => lock,
            Err(_) => return,
        };

        if let Some(airport) = &*selected_airport_lock {
            // Informacion Vuelos
            let airport_name = airport.name.to_string();
            drop(selected_airport_lock);
            self.show_heading_fligths(ui, app, &airport_name);
            ui.separator();
            self.show_flight_information(ui, app);
        } else {
            // Informacion Aeropuertos
            ui.heading("Aeropuertos");
            ui.separator();
            app.airports.list_airports(ui);
        }
    }

    fn show_heading_fligths(&self, ui: &mut egui::Ui, app: &mut FlightApp, airport_name: &str) {
        if ui.button("⬅").clicked() {
            self.clear_selection(app);
        }
        ui.heading(format!("{}", airport_name));
    }

    fn clear_selection(&self, app: &mut FlightApp) {
        if let Ok(mut selected_airport) = app.selected_airport.lock() {
            *selected_airport = None;
        }
        if let Ok(mut selected_flight_lock) = app.selected_flight.lock() {
            *selected_flight_lock = None;
        }
        if let Ok(mut flights_lock) = app.flights.flights.lock() {
            *flights_lock = Vec::new();
        }
    }

    fn show_flight_information(&self, ui: &mut egui::Ui, app: &mut FlightApp) {
        // Intenta abrir el lock del avion seleccionado
        let mut selected_flight_lock = match app.selected_flight.lock() {
            Ok(lock) => lock,
            Err(_) => return,
        };

        // Si hay un vuelo seleccionado, muestra su información detallada
        if let Some(selected_flight) = &*selected_flight_lock {
            selected_flight.list_information(ui);
            ui.add_space(10.0);
            if ui.button("Volver a la lista de vuelos").clicked() {
                *selected_flight_lock = None;
            }
        }
        // Sino lista todos
        else {
            app.flights.list_flights(ui);
        }
    }
}
