use eframe::egui;

pub struct AirportSelection;

impl AirportSelection {
    pub fn show(ui: &mut egui::Ui, airports: &[String], selected_airport: &mut Option<String>) {
        ui.heading("Selecciona un Aeropuerto");
        ui.separator();

        for airport in airports {
            if ui.button(airport).clicked() {
                *selected_airport = Some(airport.to_string());
            }
        }
    }
}