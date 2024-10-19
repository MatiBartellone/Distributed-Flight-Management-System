use std::fmt::Error;

use eframe::egui;

use crate::{flight::FlightData, information::LeftPanel, map::RightPanel};

pub struct FlightApp {
    pub selected_airport: Option<String>,
    pub flights: Vec<FlightData>,
    pub selected_flight: Option<FlightData>,
}

impl FlightApp {
    pub fn default() -> Self {
        Self {
            selected_airport: Some("Ezeiza".to_string()),
            flights: vec![],
            selected_flight: None,
        }
    }

    pub fn update_flights_for_airport(&mut self) {
        let Some(airport) = &self.selected_airport else {return};
        if let Ok(flights) = get_flights_for_airport(airport) {
            self.selected_airport = Some(airport.to_string());
            self.flights = flights;
        }
    }
}

impl eframe::App for FlightApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.update_flights_for_airport();
        egui::SidePanel::left("information_panel").show(ctx, |ui| {
            let left_panel = LeftPanel;
            left_panel.ui(ui, self);
        });

        egui::CentralPanel::default().show(ctx, |ui| {
            let right_panel = RightPanel::new();
            right_panel.ui(ui, self);
        });
    }
}

fn get_flights_for_airport(_airport: &str) -> Result<Vec<FlightData>, Error> {
    Ok(vec![
        FlightData {
            code: "FL123".to_string(),
            position: (-34.8, -58.5),
            altitude: 30000.0,
            speed: 550.0,
        },
        FlightData {
            code: "FL456".to_string(),
            position: (-34.9, -58.6),
            altitude: 32000.0,
            speed: 600.0,
        },
    ])
}