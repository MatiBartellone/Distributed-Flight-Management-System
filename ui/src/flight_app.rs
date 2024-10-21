use std::{fmt::Error, sync::{Arc, Mutex}};

use eframe::egui;
use egui::Context;
use walkers::{sources::OpenStreetMap, HttpTiles, MapMemory};

use crate::{airport_selection::AirportSelection, airports::get_airports, flight::Flight, flight_status::FlightStatus, flights::Flights, information::InformationPanel, map::MapPanel};

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
        Self::set_scroll_style(&egui_ctx);
        let selected_flight = Arc::new(Mutex::new(None));
        let flights = Flights::new(
            Vec::new(),
            Arc::clone(&selected_flight),
        );
        let mut map_memory = MapMemory::default();
        let _ = map_memory.set_zoom(10.);
        Self {
            selected_airport: None,
            flights,
            selected_flight,
            tiles: HttpTiles::new(OpenStreetMap, egui_ctx),
            map_memory
        }
    }

    pub fn update_flights_for_airport(&mut self) {
        let Some(airport) = &self.selected_airport else {return};
        if let Ok(flights) = get_flights_for_airport(airport) {
            self.selected_airport = Some(airport.to_string());
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
            position: (-75.7787, 41.6413), // Más lejos de JFK
            altitude: 32000.0,
            speed: 560.0,
            fuel_level: 85.0, // Nivel de combustible (porcentaje)
            status: FlightStatus::OnTime, // Estado del vuelo
            departure_airport: "EZE".to_string(), // Ezeiza
            departure_time: "08:30".to_string(), // Hora de salida
            arrival_airport: "JFK".to_string(), // JFK
            arrival_time: "16:45".to_string(), // Hora estimada de llegada
        },
        Flight {
            code: "LA8050".to_string(), // LATAM Airlines
            position: (-82.2903, 27.7617), // Más lejos de MIA
            altitude: 34000.0,
            speed: 580.0,
            fuel_level: 78.0, // Nivel de combustible
            status: FlightStatus::Delayed, // Estado del vuelo retrasado
            departure_airport: "SCL".to_string(), // Santiago de Chile
            departure_time: "09:15".to_string(), // Hora de salida
            arrival_airport: "MIA".to_string(), // Miami
            arrival_time: "17:00".to_string(), // Hora estimada de llegada
        },
        Flight {
            code: "AA940".to_string(), // American Airlines
            position: (-45.6350, -22.5505), // Más lejos de GRU
            altitude: 30000.0,
            speed: 550.0,
            fuel_level: 65.0, // Nivel de combustible
            status: FlightStatus::OnTime, // Estado del vuelo
            departure_airport: "DFW".to_string(), // Dallas/Fort Worth
            departure_time: "07:00".to_string(), // Hora de salida
            arrival_airport: "GRU".to_string(), // Sao Paulo-Guarulhos
            arrival_time: "15:30".to_string(), // Hora estimada de llegada
        },
        Flight {
            code: "IB6844".to_string(), // Iberia
            position: (-62.3019, -37.6083), // Más lejos de EZE
            altitude: 31000.0,
            speed: 570.0,
            fuel_level: 72.0, // Nivel de combustible
            status: FlightStatus::OnTime, // Estado del vuelo
            departure_airport: "MAD".to_string(), // Madrid
            departure_time: "10:00".to_string(), // Hora de salida
            arrival_airport: "EZE".to_string(), // Ezeiza
            arrival_time: "18:00".to_string(), // Hora estimada de llegada
        },
        Flight {
            code: "AF2280".to_string(), // Air France
            position: (-120.4085, 35.9416), // Más lejos de LAX
            altitude: 33000.0,
            speed: 590.0,
            fuel_level: 80.0, // Nivel de combustible
            status: FlightStatus::Cancelled, // Estado del vuelo cancelado
            departure_airport: "CDG".to_string(), // París Charles de Gaulle
            departure_time: "12:30".to_string(), // Hora de salida
            arrival_airport: "LAX".to_string(), // Los Ángeles
            arrival_time: "20:45".to_string(), // Hora estimada de llegada
        },
        Flight {
            code: "KL7028".to_string(), // KLM
            position: (-123.4194, 38.7749), // Más lejos de SFO
            altitude: 32000.0,
            speed: 600.0,
            fuel_level: 60.0, // Nivel de combustible
            status: FlightStatus::OnTime, // Estado del vuelo
            departure_airport: "AMS".to_string(), // Ámsterdam
            departure_time: "11:45".to_string(), // Hora de salida
            arrival_airport: "SFO".to_string(), // San Francisco
            arrival_time: "20:10".to_string(), // Hora estimada de llegada
        },
        Flight {
            code: "BA246".to_string(), // British Airways
            position: (-3.4543, 51.4700), // Más lejos de LHR
            altitude: 31000.0,
            speed: 575.0,
            fuel_level: 77.0, // Nivel de combustible
            status: FlightStatus::OnTime, // Estado del vuelo
            departure_airport: "LHR".to_string(), // Londres Heathrow
            departure_time: "14:00".to_string(), // Hora de salida
            arrival_airport: "EZE".to_string(), // Ezeiza
            arrival_time: "17:30".to_string(), // Hora estimada de llegada
        },
        Flight {
            code: "JL704".to_string(), // Japan Airlines
            position: (140.3929, 35.6735), // Más lejos de NRT
            altitude: 33000.0,
            speed: 580.0,
            fuel_level: 70.0, // Nivel de combustible
            status: FlightStatus::OnTime, // Estado del vuelo
            departure_airport: "NRT".to_string(), // Tokio Narita
            departure_time: "16:00".to_string(), // Hora de salida
            arrival_airport: "LAX".to_string(), // Los Ángeles
            arrival_time: "11:00".to_string(), // Hora estimada de llegada
        },
    ])
}
