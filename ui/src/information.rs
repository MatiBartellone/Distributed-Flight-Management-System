use eframe::egui;

use crate::flight_app::FlightApp;

pub struct LeftPanel;

impl LeftPanel {
    pub fn ui(&self, ui: &mut egui::Ui, app: &mut FlightApp) {
        ui.heading("Aeropuerto Ezeiza");

        if let Some(selected_flight) = &app.selected_flight {
            selected_flight.list_information(ui);
            if ui.button("Volver a la lista de vuelos").clicked() {
                app.selected_flight = None;
            }
        } else {
            app.flights.render_flights(ui);
        }
    }
}