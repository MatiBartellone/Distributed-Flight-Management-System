use eframe::egui;
use egui::ScrollArea;
use egui::scroll_area::ScrollBarVisibility::VisibleWhenNeeded;

pub struct AirportSelection;

impl AirportSelection {
    pub fn show(ui: &mut egui::Ui, airports: &[String], selected_airport: &mut Option<String>) {
        ui.vertical_centered_justified(|ui| {
            ui.set_width(ui.available_width() * 0.4);
            ui.heading("Selecciona un Aeropuerto");
            ui.separator();

            ScrollArea::vertical()
            .scroll_bar_visibility(VisibleWhenNeeded)
            .show(ui, |ui| {
                ui.set_width(ui.available_width());
                for airport in airports {
                    if ui.button(airport).clicked() {
                        *selected_airport = Some(airport.to_string());
                    }
                }
            });
        });
    }
}