use std::{
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use eframe::egui;
use egui::Context;
use walkers::{sources::OpenStreetMap, HttpTiles, MapMemory};

use crate::{airport::{airport::Airport, airports::Airports}, cassandra_client::CassandraClient, flight::{flight::Flight, flight_selected::FlightSelected, flights::Flights}, panels::{information::InformationPanel, map::MapPanel}};

pub struct FlightApp {
    pub airports: Airports,
    pub selected_airport: Arc<Mutex<Option<Airport>>>,
    pub flights: Flights,
    pub selected_flight: Arc<Mutex<Option<FlightSelected>>>,
    // Map
    pub tiles: HttpTiles,
    pub map_memory: MapMemory,
}

impl FlightApp {
    pub fn new(egui_ctx: Context, mut information: CassandraClient) -> Self {
        Self::set_scroll_style(&egui_ctx);

        let selected_airport = Arc::new(Mutex::new(None));
        let airports = Airports::new(information.get_airports(), Arc::clone(&selected_airport));

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
            map_memory
        };
        app.loop_update_flights(egui_ctx, information);
        app
    }

    fn loop_update_flights(&mut self, ctx: egui::Context, mut information: CassandraClient) {
        let selected_flight = Arc::clone(&self.selected_flight);
        let selected_airport = Arc::clone(&self.selected_airport);
        let flights = Arc::clone(&self.flights.flights);

        thread::spawn(move || loop {
            update_flights(&selected_flight, &selected_airport, &flights, &mut information);
            ctx.request_repaint();
            thread::sleep(Duration::from_millis(500));
        });
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



// FUNCIONES DEL LOOP PARA ACTUALIZAR

fn update_flights(
    selected_flight: &Arc<Mutex<Option<FlightSelected>>>,
    selected_airport: &Arc<Mutex<Option<Airport>>>,
    flights: &Arc<Mutex<Vec<Flight>>>,
    information: &mut CassandraClient
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
            clear_flight_data(selected_flight, flights);
            return;
        }
    };

    load_flights(flights, information, &airport.name);
    get_selected_flight(selected_flight, information);
}

// Carga todos los vuelos si la lista está vacía o actualiza solo los datos básicos.
fn load_flights(
    flights: &Arc<Mutex<Vec<Flight>>>,
    information: &mut CassandraClient,
    airport_name: &str
) {
    let mut flights_lock = match flights.lock() {
        Ok(lock) => lock,
        Err(_) => return,
    };
    *flights_lock = information.get_flights(airport_name);
}

fn get_selected_flight(
    selected_flight: &Arc<Mutex<Option<FlightSelected>>>,
    information: &mut CassandraClient
) {
    if let Ok(mut selected_flight_lock) = selected_flight.lock() {
        if let Some(selected_flight) = &*selected_flight_lock {
            *selected_flight_lock = Some(information.get_flight_selected(&selected_flight.code));
        }
    }
}

fn clear_flight_data(
    selected_flight: &Arc<Mutex<Option<FlightSelected>>>,
    flights: &Arc<Mutex<Vec<Flight>>>
) {
    if let Ok(mut flight_lock) = selected_flight.lock() {
        *flight_lock = None;
    }
    if let Ok(mut flights_lock) = flights.lock() {
        *flights_lock = Vec::new();
    }
}