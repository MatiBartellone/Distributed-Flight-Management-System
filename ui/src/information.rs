use eframe::egui;

use crate::flight_app::FlightApp;

pub struct InformationPanel;

impl InformationPanel {
    pub fn ui(&self, ui: &mut egui::Ui, app: &mut FlightApp) {
        self.show_airport(ui, app);
        ui.separator();
        self.show_flight_information(ui, app);
    }

    fn show_airport(&self, ui: &mut egui::Ui, app: &mut FlightApp) {
        ui.horizontal(|ui| {
            if ui.button("⬅").clicked() {
                self.clear_selection(app);
            }
            if let Some(airport) = &app.selected_airport {
                ui.heading(format!("Aeropuerto {}", airport));
            }
        });
    }

    fn clear_selection(&self, app: &mut FlightApp) {
        app.selected_airport = None;
        if let Ok(mut selected_flight_lock) = app.selected_flight.lock() {
            *selected_flight_lock = None;
        }
    }

    fn show_flight_information(&self, ui: &mut egui::Ui, app: &mut FlightApp) {
        let mut selected_flight_lock = match app.selected_flight.lock() {
            Ok(lock) => lock,
            Err(_) => return,
        };

        // Si hay un vuelo seleccionado, muestra su información detallada y sino lista todos
        if let Some(selected_flight) = &*selected_flight_lock {
            selected_flight.list_information(ui);
            ui.add_space(10.0);
            if ui.button("Volver a la lista de vuelos").clicked() {
                *selected_flight_lock = None;
            }
        } else {
            app.flights.list_flight_codes(ui);
        }
    }
}
