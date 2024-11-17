use std::sync::{Arc, Mutex};

use eframe::egui;
use egui::Context;
use walkers::{sources::OpenStreetMap, HttpTiles, MapMemory};

use crate::{
    airport_implementation::{airport::Airport, airports::Airports}, cassandra_comunication::ui_client::UIClient, flight_implementation::{flight_selected::FlightSelected, flights::Flights}, panels::{information::InformationPanel, map::MapPanel}
};

use super::{app_updater::AppUpdater, thread_pool::ThreadPool};

// List of the airports codes to use in the app
pub fn get_airports_codes() -> Vec<String> {
    vec![
        "EZE".to_string(), // Aeropuerto Internacional Ministro Pistarini (Argentina)
        "JFK".to_string(), // John F. Kennedy International Airport (EE. UU.)
        "SCL".to_string(), // Aeropuerto Internacional Comodoro Arturo Merino Benítez (Chile)
        "MIA".to_string(), // Aeropuerto Internacional de Miami (EE. UU.)
        "DFW".to_string(), // Dallas/Fort Worth International Airport (EE. UU.)
        "GRU".to_string(), // Aeroporto Internacional de São Paulo/Guarulhos (Brasil)
        "MAD".to_string(), // Aeropuerto Adolfo Suárez Madrid-Barajas (España)
        "CDG".to_string(), // Aeropuerto Charles de Gaulle (Francia)
        "LAX".to_string(), // Los Angeles International Airport (EE. UU.)
        "AMS".to_string(), // Luchthaven Schiphol (Países Bajos)
        "NRT".to_string(), // Narita International Airport (Japón)
        "LHR".to_string(), // Aeropuerto de Heathrow (Reino Unido)
        "FRA".to_string(), // Aeropuerto de Frankfurt (Alemania)
        "SYD".to_string(), // Sydney Kingsford Smith Airport (Australia)
        "SFO".to_string(), // San Francisco International Airport (EE. UU.)
        "BOG".to_string(), // Aeropuerto Internacional El Dorado (Colombia)
        "MEX".to_string(), // Aeropuerto Internacional de la Ciudad de México (México)
        "YYC".to_string(), // Aeropuerto Internacional de Calgary (Canadá)
        "OSL".to_string(), // Aeropuerto de Oslo-Gardermoen (Noruega)
        "DEL".to_string(), // Aeropuerto Internacional Indira Gandhi (India)
        "PEK".to_string(), // Aeropuerto Internacional de Pekín-Capital (China)
        "SVO".to_string(), // Aeropuerto Internacional Sheremétievo (Rusia)
        "RUH".to_string(), // Aeropuerto Internacional Rey Khalid (Arabia Saudita)
        "CGK".to_string(), // Aeropuerto Internacional Soekarno-Hatta (Indonesia)
        "JNB".to_string(), // Aeropuerto Internacional O. R. Tambo (Sudáfrica)
        "BKO".to_string(), // Aeropuerto Internacional Modibo Keïta (Mali)
        "CAI".to_string(), // Aeropuerto Internacional de El Cairo (Egipto)
    ]
}

pub struct FlightApp {
    pub airports: Airports,
    pub selected_airport: Arc<Mutex<Option<Airport>>>,
    pub flights: Flights,
    pub selected_flight: Arc<Mutex<Option<FlightSelected>>>,
    // Map
    pub tiles: HttpTiles,
    pub map_memory: MapMemory
}

impl FlightApp {
    pub fn new(egui_ctx: Context, mut client: UIClient) -> Self {
        Self::set_scroll_style(&egui_ctx);
        let thread_pool = ThreadPool::new(8);
        let (selected_airport, airports) = Self::inicializate_airport_information(&mut client, &thread_pool);
        let (selected_flight, flights) = Self::inicializate_flights_information();
        let map_memory = Self::initialize_map_memory();

        let mut app = Self {
            airports,
            selected_airport,
            flights,
            selected_flight,
            tiles: HttpTiles::new(OpenStreetMap, egui_ctx.clone()),
            map_memory,
        };
        app.start_app_updater(egui_ctx, client, thread_pool);
        app
    }

    fn set_scroll_style(egui_ctx: &Context) {
        let mut style = egui::Style::default();
        style.spacing.scroll = egui::style::ScrollStyle::solid();
        egui_ctx.set_style(style);
    }

    fn inicializate_airport_information(client: &mut UIClient, thread_pool: &ThreadPool) -> (Arc<Mutex<Option<Airport>>>, Airports) {
        let selected_airport = Arc::new(Mutex::new(None));
        let airports = Airports::new(
            client.get_airports(get_airports_codes(), &thread_pool),
            Arc::clone(&selected_airport),
        );
        (selected_airport, airports)
    }

    fn inicializate_flights_information() -> (Arc<Mutex<Option<FlightSelected>>>, Flights) {
        let selected_flight = Arc::new(Mutex::new(None));
        let flights = Flights::new(Vec::new(), Arc::clone(&selected_flight));
        (selected_flight, flights)
    }

    fn initialize_map_memory() -> MapMemory {
        let mut map_memory = MapMemory::default();
        map_memory.set_zoom(2.).unwrap_or(());
        map_memory
    }

    // Start the app updater thread to update the app information
    fn start_app_updater(&mut self, ctx: egui::Context, information: UIClient, thread_pool: ThreadPool) {
        let selected_flight = Arc::clone(&self.selected_flight);
        let selected_airport = Arc::clone(&self.selected_airport);
        let flights = Arc::clone(&self.flights.flights);

        AppUpdater::new(
            selected_flight,
            selected_airport,
            flights,
            information,
            thread_pool,
        )
        .start(ctx);
    }

    pub fn get_airport_selected_name(&self) -> Option<String> {
        match self.selected_airport.lock() {
            Ok(lock) => match &*lock {
                Some(airport) => Some(airport.name.to_string()),
                None => None,
            },
            Err(_) => None,
        }
    }

    pub fn clear_selection(&self) {
        if let Ok(mut selected_airport) = self.selected_airport.lock() {
            *selected_airport = None;
        }
        if let Ok(mut selected_flight_lock) = self.selected_flight.lock() {
            *selected_flight_lock = None;
        }
        if let Ok(mut flights_lock) = self.flights.flights.lock() {
            *flights_lock = Vec::new();
        }
    }

    pub fn is_airport_selected(selected_airport: &Arc<Mutex<Option<Airport>>>) -> bool {
        match selected_airport.lock() {
            Ok(lock) => (*lock).is_some(),
            Err(_) => false,
        }
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