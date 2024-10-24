use std::sync::{Arc, Mutex};

use eframe::egui;
use egui::Context;
use walkers::{sources::OpenStreetMap, HttpTiles, MapMemory};

use crate::{airport::Airport, airports::{get_airports, Airports}, flight::Flight, flights::Flights, information::InformationPanel, map::MapPanel};

pub struct FlightApp {
    pub airports: Airports,
    pub selected_airport: Arc<Mutex<Option<Airport>>>,
    pub flights: Flights,
    pub selected_flight: Arc<Mutex<Option<Flight>>>,
    // Map
    pub tiles: HttpTiles,
    pub map_memory: MapMemory
}

impl FlightApp {
    pub fn new(egui_ctx: Context) -> Self {
        Self::set_scroll_style(&egui_ctx);

        let selected_airport = Arc::new(Mutex::new(None));
        let airports = Airports::new(
            get_airports(),
            Arc::clone(&selected_airport),
        );

        let selected_flight = Arc::new(Mutex::new(None));
        let flights = Flights::new(
            Vec::new(),
            Arc::clone(&selected_flight),
        );

        let mut map_memory = MapMemory::default();
        let _ = map_memory.set_zoom(2.);

        Self {
            airports,
            selected_airport,
            flights,
            selected_flight,
            tiles: HttpTiles::new(OpenStreetMap, egui_ctx),
            map_memory
        }
    }

    fn update_flights(&mut self) {
        // Intenta abrir el lock del aeropuerto seleccionado
        let selected_airport = match self.selected_airport.lock() {
            Ok(lock) => lock,
            Err(_) => return,
        };

        // Se fija si hay un aeropuerto seleccionado
        let airport = match &*selected_airport {
            Some(airport) => airport,
            None => {
                if self.selected_flight.lock().is_ok() {
                    let mut flight_lock = self.selected_flight.lock().unwrap();
                    *flight_lock = None; // Cambia el vuelo seleccionado a None
                }
                return;
            }
        };

        // Actualiza la lista de vuelos 
        if let Ok(flights) = airport.get_flights() {
            self.flights.flights = flights;
        }
    }

    fn set_scroll_style(egui_ctx: &Context) {
        let mut style = egui::Style::default();
        style.spacing.scroll = egui::style::ScrollStyle::solid(); 
        egui_ctx.set_style(style);
    }
}

impl eframe::App for FlightApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.update_flights();
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