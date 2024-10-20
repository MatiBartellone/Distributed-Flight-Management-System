use std::{fmt::Error, sync::{Arc, Mutex}};

use eframe::egui;
use egui::Context;
use walkers::{sources::OpenStreetMap, HttpTiles, MapMemory};

use crate::{flight::Flight, flights::Flights, information::LeftPanel, map::RightPanel};

pub struct FlightApp {
    pub selected_airport: Option<String>,
    pub flights: Flights,
    pub selected_flight: Arc<Mutex<Option<Flight>>>,
    // Map
    pub tiles: HttpTiles,
    pub map_memory: MapMemory,
}

impl FlightApp {
    pub fn new(egui_ctx: Context) -> Self {
        let selected_flight = Arc::new(Mutex::new(None));
        Self {
            selected_airport: Some("EZE".to_string()),
            flights: Flights::new(
                Vec::new(),
                Arc::clone(&selected_flight),
            ),
            selected_flight,
            tiles: HttpTiles::new(OpenStreetMap, egui_ctx),
            map_memory: MapMemory::default(),
        }
    }

    pub fn update_flights_for_airport(&mut self) {
        let Some(airport) = &self.selected_airport else {return};
        if let Ok(flights) = get_flights_for_airport(airport) {
            self.selected_airport = Some(airport.to_string());
            self.flights.flights = flights;
        }
    }
}

impl eframe::App for FlightApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.update_flights_for_airport();
        egui::SidePanel::left("information_panel").show(ctx, |ui| {
            LeftPanel.ui(ui, self);
        });

        egui::CentralPanel::default().show(ctx, |ui| {
            RightPanel.ui(ui, self);
        });
    }
}

fn get_flights_for_airport(_airport: &str) -> Result<Vec<Flight>, Error> {
    Ok(vec![
        Flight {
            code: "AR1130".to_string(), // Aerolíneas Argentinas
            position: (-58.5000, -34.8000), // Un poco más lejos de Ezeiza
            altitude: 32000.0,
            speed: 560.0,
        },
        Flight {
            code: "LA8050".to_string(), // LATAM Airlines
            position: (-58.4700, -34.7800), // Más separado
            altitude: 34000.0,
            speed: 580.0,
        },
        Flight {
            code: "AA940".to_string(), // American Airlines
            position: (-58.5500, -34.7500), // Más lejos
            altitude: 30000.0,
            speed: 550.0,
        },
        Flight {
            code: "IB6844".to_string(), // Iberia
            position: (-58.6000, -34.7900), // Separado
            altitude: 31000.0,
            speed: 570.0,
        },
        Flight {
            code: "AF2280".to_string(), // Air France
            position: (-58.4500, -34.8200), // Más lejos
            altitude: 33000.0,
            speed: 590.0,
        },
        Flight {
            code: "KL7028".to_string(), // KLM
            position: (-58.5200, -34.8600), // Un poco más lejos
            altitude: 32000.0,
            speed: 600.0,
        },
    ])
}