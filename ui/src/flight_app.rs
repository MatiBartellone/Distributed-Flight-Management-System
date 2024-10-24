use std::{
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use eframe::egui;
use egui::Context;
use walkers::{sources::OpenStreetMap, HttpTiles, MapMemory};

use crate::{
    airport::Airport,
    airports::{get_airports, Airports},
    flight::Flight,
    flights::Flights,
    information::InformationPanel,
    map::MapPanel,
};

pub struct FlightApp {
    pub airports: Airports,
    pub selected_airport: Arc<Mutex<Option<Airport>>>,
    pub flights: Flights,
    pub selected_flight: Arc<Mutex<Option<Flight>>>,
    // Map
    pub tiles: HttpTiles,
    pub map_memory: MapMemory,
}

impl FlightApp {
    pub fn new(egui_ctx: Context) -> Self {
        Self::set_scroll_style(&egui_ctx);

        let selected_airport = Arc::new(Mutex::new(None));
        let airports = Airports::new(get_airports(), Arc::clone(&selected_airport));

        let selected_flight = Arc::new(Mutex::new(None));
        let flights = Flights::new(Vec::new(), Arc::clone(&selected_flight));

        let mut map_memory = MapMemory::default();
        let _ = map_memory.set_zoom(2.);

        let mut app = Self {
            airports,
            selected_airport,
            flights,
            selected_flight,
            tiles: HttpTiles::new(OpenStreetMap, egui_ctx.clone()),
            map_memory,
        };
        app.loop_update_flights(egui_ctx);
        app
    }

    fn loop_update_flights(&mut self, ctx: egui::Context) {
        let selected_flight = Arc::clone(&self.selected_flight);
        let selected_airport = Arc::clone(&self.selected_airport);
        let flights = Arc::clone(&self.flights.flights);

        thread::spawn(move || loop {
            FlightApp::update_flights(&selected_flight, &selected_airport, &flights);
            ctx.request_repaint();
            thread::sleep(Duration::from_millis(500));
        });
    }

    fn update_flights(
        selected_flight: &Arc<Mutex<Option<Flight>>>,
        selected_airport: &Arc<Mutex<Option<Airport>>>,
        flights: &Arc<Mutex<Vec<Flight>>>,
    ) {
        // Intenta abrir el lock del aeropuerto seleccionado
        let selected_airport = match selected_airport.lock() {
            Ok(lock) => lock,
            Err(_) => return,
        };

        // Se fija si hay un aeropuerto seleccionado
        let airport = match &*selected_airport {
            Some(airport) => airport,
            None => {
                // Limpia los datos de vuelos
                if let Ok(mut flight_lock) = selected_flight.lock() {
                    *flight_lock = None;
                }
                if let Ok(mut flights_lock) = flights.lock() {
                    *flights_lock = Vec::new();
                }
                return;
            }
        };
        if let Ok(new_flights) = Airports::get_flights(airport) {
            // Actualiza el vuelo seleccionado si existe, sino lo deselecciona
            if let Ok(mut selected_flight_lock) = selected_flight.lock() {
                if let Some(selected_flight) = &*selected_flight_lock {
                    if let Some(updated_flight) = new_flights
                        .iter()
                        .find(|flight| flight.code == selected_flight.code)
                    {
                        *selected_flight_lock = Some(updated_flight.clone());
                    } else {
                        *selected_flight_lock = None;
                    }
                }
            }

            // Actualiza la lista de vuelos
            if let Ok(mut flights_lock) = flights.lock() {
                *flights_lock = new_flights;
            }
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
