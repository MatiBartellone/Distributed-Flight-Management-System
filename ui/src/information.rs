use eframe::egui;

use crate::flight_app::FlightApp;

pub struct LeftPanel;

impl LeftPanel {
    pub fn ui(&self, ui: &mut egui::Ui, app: &mut FlightApp) {
        ui.heading("Aeropuerto Ezeiza");
        ui.separator();
        if let Ok(selected_flight_lock) = app.selected_flight.lock() {
            if let Some(selected_flight) = &*selected_flight_lock {
                selected_flight.list_information(ui);
                if ui.button("Volver a la lista de vuelos").clicked() {
                    drop(selected_flight_lock); // Liberamos el lock antes de cambiar el valor
                    if let Ok(mut selected_flight) = app.selected_flight.lock() {
                        *selected_flight = None;
                    } else {
                        println!("Error al obtener el lock en selected_flight");
                    }
                }
            } else {
                app.flights.render_flights(ui);
            }
        } else {
            println!("Error al obtener el lock en selected_flight");
        }
    }
}