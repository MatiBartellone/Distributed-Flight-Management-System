use eframe::egui;

use crate::app_implementation::flight_app::FlightApp;

pub struct InformationPanel;

impl InformationPanel {
    /// Shows the information of the airports or the flights of an airport if one is selected
    pub fn ui(&self, ui: &mut egui::Ui, app: &mut FlightApp) {
        if let Some(airport_name) = app.get_airport_selected_name() {
            // Information of flights of an airport
            self.show_heading_fligths(ui, app, &airport_name);
            ui.separator();
            self.show_flight_information(ui, app);
            return;
        }
        // Information of airports
        self.show_heading_airports(ui, app);
        ui.separator();
        app.airports.list_airports(ui);
    }

    fn show_heading_fligths(&self, ui: &mut egui::Ui, app: &mut FlightApp, airport_name: &str) {
        if ui.button("⬅").clicked() {
            app.clear_selection();
        }
        ui.heading(airport_name);
    }

    fn show_heading_airports(&self, ui: &mut egui::Ui, app: &mut FlightApp) {
        if ui.button("⟳").clicked() {
            app.restore_airports();
        }
        ui.heading("Aeropuertos");
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
