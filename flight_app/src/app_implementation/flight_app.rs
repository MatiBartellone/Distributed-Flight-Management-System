use std::sync::{Arc, Mutex};

use eframe::egui;
use egui::Context;
use walkers::{sources::{Mapbox, MapboxStyle}, HttpTiles, MapMemory};

use crate::{
    airport_implementation::airports::Airports, cassandra_comunication::ui_client::UIClient, flight_implementation::{flight::Flight, flight_selected::FlightSelected, flights::Flights}, panels::{information::InformationPanel, map::MapPanel}
};

use super::app_updater::AppUpdater;

pub struct FlightApp {
    pub airports: Airports,
    pub selected_airport_code: Arc<Mutex<Option<String>>>,
    pub flights: Flights,
    pub selected_flight: Arc<Mutex<Option<FlightSelected>>>,
    pub updater: AppUpdater,
    // Map
    pub tiles: HttpTiles,
    pub map_memory: MapMemory
}

impl FlightApp {
    /// Create a new FlightApp with the given Cassandra clients and the egui context
    /// Start the app updater thread to update the app information
    pub fn new(egui_ctx: Context, mut ui_client: UIClient) -> Self {
        Self::set_scroll_style(&egui_ctx);
        let (selected_flight, flights) = Self::initialize_flights_information();
        let (selected_airport_code, airports) = Self::initialize_airport_information(&mut ui_client, &selected_flight);
        let map_memory = Self::initialize_map_memory();
        let tile_source = Self::get_maxbox_tile();

        let updater = Self::initialize_app_updater(
            Arc::clone(&selected_flight),
            Arc::clone(&selected_airport_code),
            Arc::clone(&flights.flights),
            ui_client,
        );
        updater.start(egui_ctx.clone());

        Self {
            airports,
            selected_airport_code,
            flights,
            selected_flight,
            updater,
            tiles: HttpTiles::new(tile_source, egui_ctx.clone()),
            map_memory,
        }
    }

    fn get_maxbox_tile() -> Mapbox {
        Mapbox {
            style: MapboxStyle::NavigationNight,
            high_resolution: false,
            access_token: "pk.eyJ1IjoiaXZhbi1tYXhpbW9mZiIsImEiOiJjbTJnZXVpbTMwMGZiMmxvbnBtZmZrYzhxIn0.ML4CVWvfANu4abq_24r6Wg".to_string()
        }
    }

    fn set_scroll_style(egui_ctx: &Context) {
        let mut style = egui::Style::default();
        style.spacing.scroll = egui::style::ScrollStyle::solid();
        egui_ctx.set_style(style);
    }

    fn initialize_airport_information(ui_client: &mut UIClient, selected_flight: &Arc<Mutex<Option<FlightSelected>>>) -> (Arc<Mutex<Option<String>>>, Airports) {
        let selected_airport_code = Arc::new(Mutex::new(None));
        let airports = Airports::new(
            ui_client.get_airports(get_airports_codes()),
            Arc::clone(&selected_airport_code),
            Arc::clone(selected_flight),
        );
        (selected_airport_code, airports)
    }

    fn initialize_flights_information() -> (Arc<Mutex<Option<FlightSelected>>>, Flights) {
        let selected_flight = Arc::new(Mutex::new(None));
        let flights = Flights::new(Vec::new(), Arc::clone(&selected_flight));
        (selected_flight, flights)
    }

    fn initialize_map_memory() -> MapMemory {
        let mut map_memory = MapMemory::default();
        map_memory.set_zoom(2.).unwrap_or(());
        map_memory
    }

    fn initialize_app_updater(
        selected_flight: Arc<Mutex<Option<FlightSelected>>>,
        selected_airport_code: Arc<Mutex<Option<String>>>,
        flights: Arc<Mutex<Vec<Flight>>>,
        ui_client: UIClient,
    ) -> AppUpdater {
        AppUpdater::new(
            selected_flight,
            selected_airport_code,
            flights,
            ui_client,
        )
    }

    fn get_airport_code(&self) -> Option<String> {
        match self.selected_airport_code.lock() {
            Ok(lock) => (*lock).as_ref().map(|airport| airport.to_string()),
            Err(_) => None,
        }
    }

    /// Get the name of the selected airport code
    pub fn get_airport_selected_name(&self) -> Option<String> {
        self.get_airport_code().map(|airport_code| self.airports.get_aiport_name(&airport_code))
    }

    /// Clear the selected airport and flight information
    pub fn clear_selection(&self) {
        if let Ok(mut selected_airport) = self.selected_airport_code.lock() {
            *selected_airport = None;
        }
        if let Ok(mut selected_flight_lock) = self.selected_flight.lock() {
            *selected_flight_lock = None;
        }
        if let Ok(mut flights_lock) = self.flights.flights.lock() {
            *flights_lock = Vec::new();
        }
    }

    fn clear_flight_data(&self) {
        if let Ok(mut flight_lock) = self.selected_flight.lock() {
            *flight_lock = None;
        }
        if let Ok(mut flights_lock) = self.flights.flights.lock() {
            *flights_lock = Vec::new();
        }
    }

    fn check_selected_airport(&self) {
        if let Ok(selected_airport) = self.selected_airport_code.lock() {
            if selected_airport.is_none() {
                self.clear_flight_data();
            }
        }
    }

    /// Check if an airport is selected
    pub fn is_airport_selected(selected_airport: &Arc<Mutex<Option<String>>>) -> bool {
        match selected_airport.lock() {
            Ok(lock) => (*lock).is_some(),
            Err(_) => false,
        }
    }

    /// Restore the airports information and set it to the airports list of the app
    pub fn restore_airports(&mut self) {
        let Some(new_airports) = self
            .updater
            .restore_airports(get_airports_codes()) else {return;};

        self.airports.set_airports(new_airports);
    }
}

impl eframe::App for FlightApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.check_selected_airport();
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


/// List of the airports codes to use in the app
fn get_airports_codes() -> Vec<String> {
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
