use std::{fmt::Error, sync::{Arc, Mutex}};

use eframe::egui;
use egui::Context;
use walkers::{sources::OpenStreetMap, HttpTiles, MapMemory};

use crate::{airport_selection::AirportSelection, flight::Flight, flights::Flights, information::InformationPanel, map::MapPanel};

pub struct FlightApp {
    pub selected_airport: Option<String>,
    pub flights: Flights,
    pub selected_flight: Arc<Mutex<Option<Flight>>>, // Variable compartida que se usa para comunicarse entre el map panel y el information panel
    // Map
    pub tiles: HttpTiles,
    pub map_memory: MapMemory,
}

impl FlightApp {
    pub fn new(egui_ctx: Context) -> Self {
        let selected_flight = Arc::new(Mutex::new(None));
        let flights = Flights::new(
            Vec::new(),
            Arc::clone(&selected_flight),
        );
        Self {
            selected_airport: None,
            flights,
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
        // Si no hay un aeropuerto seleccionado lo pide
        if self.selected_airport.is_none() {
            egui::CentralPanel::default().show(ctx, |ui| {
                AirportSelection::show(ui, &get_airports(), &mut self.selected_airport);
            });
            return;
        }

        // Interfaz principal con el aeropuerto seleccionado
        self.update_flights_for_airport();
        egui::SidePanel::left("information_panel")
            .min_width(150.0)
            .max_width(230.0)
            .show(ctx, |ui| {
                InformationPanel.ui(ui, self);
            });

        egui::CentralPanel::default().show(ctx, |ui| {
            MapPanel.ui(ui, self);
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

fn get_airports() -> Vec<String> {
    vec![
        "EZE".to_string(),
        "JFK".to_string(),
        "LHR".to_string(),
        "CDG".to_string(),
        "NRT".to_string(),
    ]
}