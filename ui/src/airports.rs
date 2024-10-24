use std::sync::{Arc, Mutex};

use eframe::Error;
use egui::scroll_area::ScrollBarVisibility::VisibleWhenNeeded;
use egui::{Painter, Pos2, Response, ScrollArea};
use walkers::{Plugin, Position, Projector};

use crate::flight_status::FlightStatus;
use crate::{airport::Airport, flight::Flight};

#[derive(Clone)]
pub struct Airports {
    pub airports: Vec<Airport>,
    pub on_airport_selected: Arc<Mutex<Option<Airport>>>,
}

impl Airports {
    pub fn new(airports: Vec<Airport>, on_airport_selected: Arc<Mutex<Option<Airport>>>) -> Self {
        Self {
            airports,
            on_airport_selected,
        }
    }

    pub fn get_flights(airport: &Airport) -> Result<Vec<Flight>, Error> {
        Ok(vec![
            Flight {
                code: "AR1130".to_string(),    // Aerolíneas Argentinas
                position: (-75.7787, 41.6413), // Más lejos de JFK
                altitude: 32000.0,
                speed: 560.0,
                fuel_level: 85.0,             // Nivel de combustible (porcentaje)
                status: FlightStatus::OnTime, // Estado del vuelo
                departure_airport: "EZE".to_string(), // Ezeiza
                departure_time: "08:30".to_string(), // Hora de salida
                arrival_airport: "JFK".to_string(), // JFK
                arrival_time: "16:45".to_string(), // Hora estimada de llegada
            },
            Flight {
                code: "LA8050".to_string(),    // LATAM Airlines
                position: (-82.2903, 27.7617), // Más lejos de MIA
                altitude: 34000.0,
                speed: 580.0,
                fuel_level: 78.0,                     // Nivel de combustible
                status: FlightStatus::Delayed,        // Estado del vuelo retrasado
                departure_airport: "SCL".to_string(), // Santiago de Chile
                departure_time: "09:15".to_string(),  // Hora de salida
                arrival_airport: "MIA".to_string(),   // Miami
                arrival_time: "17:00".to_string(),    // Hora estimada de llegada
            },
            Flight {
                code: "AA940".to_string(),      // American Airlines
                position: (-45.6350, -22.5505), // Más lejos de GRU
                altitude: 30000.0,
                speed: 550.0,
                fuel_level: 65.0,                     // Nivel de combustible
                status: FlightStatus::OnTime,         // Estado del vuelo
                departure_airport: "DFW".to_string(), // Dallas/Fort Worth
                departure_time: "07:00".to_string(),  // Hora de salida
                arrival_airport: "GRU".to_string(),   // Sao Paulo-Guarulhos
                arrival_time: "15:30".to_string(),    // Hora estimada de llegada
            },
            Flight {
                code: "IB6844".to_string(),     // Iberia
                position: (-62.3019, -37.6083), // Más lejos de EZE
                altitude: 31000.0,
                speed: 570.0,
                fuel_level: 72.0,                     // Nivel de combustible
                status: FlightStatus::OnTime,         // Estado del vuelo
                departure_airport: "MAD".to_string(), // Madrid
                departure_time: "10:00".to_string(),  // Hora de salida
                arrival_airport: "EZE".to_string(),   // Ezeiza
                arrival_time: "18:00".to_string(),    // Hora estimada de llegada
            },
            Flight {
                code: "AF2280".to_string(),     // Air France
                position: (-120.4085, 35.9416), // Más lejos de LAX
                altitude: 33000.0,
                speed: 590.0,
                fuel_level: 80.0,                     // Nivel de combustible
                status: FlightStatus::Cancelled,      // Estado del vuelo cancelado
                departure_airport: "CDG".to_string(), // París Charles de Gaulle
                departure_time: "12:30".to_string(),  // Hora de salida
                arrival_airport: "LAX".to_string(),   // Los Ángeles
                arrival_time: "20:45".to_string(),    // Hora estimada de llegada
            },
            Flight {
                code: "KL7028".to_string(),     // KLM
                position: (-123.4194, 38.7749), // Más lejos de SFO
                altitude: 32000.0,
                speed: 600.0,
                fuel_level: 60.0,                     // Nivel de combustible
                status: FlightStatus::OnTime,         // Estado del vuelo
                departure_airport: "AMS".to_string(), // Ámsterdam
                departure_time: "11:45".to_string(),  // Hora de salida
                arrival_airport: "SFO".to_string(),   // San Francisco
                arrival_time: "20:10".to_string(),    // Hora estimada de llegada
            },
            Flight {
                code: "BA246".to_string(),    // British Airways
                position: (-3.4543, 51.4700), // Más lejos de LHR
                altitude: 31000.0,
                speed: 575.0,
                fuel_level: 77.0,                     // Nivel de combustible
                status: FlightStatus::OnTime,         // Estado del vuelo
                departure_airport: "LHR".to_string(), // Londres Heathrow
                departure_time: "14:00".to_string(),  // Hora de salida
                arrival_airport: "EZE".to_string(),   // Ezeiza
                arrival_time: "17:30".to_string(),    // Hora estimada de llegada
            },
            Flight {
                code: "JL704".to_string(),     // Japan Airlines
                position: (140.3929, 35.6735), // Más lejos de NRT
                altitude: 33000.0,
                speed: 580.0,
                fuel_level: 70.0,                     // Nivel de combustible
                status: FlightStatus::OnTime,         // Estado del vuelo
                departure_airport: "NRT".to_string(), // Tokio Narita
                departure_time: "16:00".to_string(),  // Hora de salida
                arrival_airport: "LAX".to_string(),   // Los Ángeles
                arrival_time: "11:00".to_string(),    // Hora estimada de llegada
            },
        ])
    }

    pub fn list_airports(&self, ui: &mut egui::Ui) {
        ScrollArea::vertical()
            .scroll_bar_visibility(VisibleWhenNeeded)
            .show(ui, |ui| {
                ui.set_width(ui.available_width());
                for airport in &self.airports {
                    airport.list_information(ui);
                    ui.separator();
                }
            });
    }
}

impl Plugin for &mut Airports {
    fn run(&mut self, response: &Response, painter: Painter, projector: &Projector) {
        // Intenta abrir el lock del aeropuerto seleccionado
        let selected_airport_lock = match self.on_airport_selected.lock() {
            Ok(lock) => lock,
            Err(_) => return,
        };

        if let Some(airport) = &*selected_airport_lock {
            // Si hay un aeropuerto seleccionado dibuja solo ese
            let airport = airport.clone();
            drop(selected_airport_lock);
            airport.draw(
                response,
                painter.clone(),
                projector,
                &self.on_airport_selected,
            );
        } else {
            // Sino dibuja todos los aeropuertos
            drop(selected_airport_lock);
            for airport in &self.airports {
                airport.draw(
                    response,
                    painter.clone(),
                    projector,
                    &self.on_airport_selected,
                );
            }
        }
    }
}

pub fn get_airports() -> Vec<Airport> {
    vec![
        Airport::new(
            "Aeropuerto Internacional Ministro Pistarini".to_string(),
            "EZE".to_string(),
            (-58.535, -34.812),
        ), // EZE
        Airport::new(
            "Aeropuerto Internacional John F. Kennedy".to_string(),
            "JFK".to_string(),
            (-73.7781, 40.6413),
        ), // JFK
        Airport::new(
            "Aeropuerto Internacional Comodoro Arturo Merino Benítez".to_string(),
            "SCL".to_string(),
            (-70.7859, -33.3928),
        ), // SCL
        Airport::new(
            "Aeropuerto Internacional de Miami".to_string(),
            "MIA".to_string(),
            (-80.2870, 25.7959),
        ), // MIA
        Airport::new(
            "Aeropuerto Internacional de Dallas/Fort Worth".to_string(),
            "DFW".to_string(),
            (-97.0382, 32.8968),
        ), // DFW
        Airport::new(
            "Aeroporto Internacional de São Paulo/Guarulhos".to_string(),
            "GRU".to_string(),
            (-46.4731, -23.4255),
        ), // GRU
        Airport::new(
            "Aeropuerto Adolfo Suárez Madrid-Barajas".to_string(),
            "MAD".to_string(),
            (-3.5706, 40.4935),
        ), // MAD
        Airport::new(
            "Aéroport de Paris-Charles-de-Gaulle".to_string(),
            "CDG".to_string(),
            (2.5479, 49.0097),
        ), // CDG
        Airport::new(
            "Aeropuerto Internacional de Los Ángeles".to_string(),
            "LAX".to_string(),
            (-118.4108, 33.9428),
        ), // LAX
        Airport::new(
            "Luchthaven Schiphol".to_string(),
            "AMS".to_string(),
            (4.7642, 52.3086),
        ), // AMS
        Airport::new(
            "Narita International Airport".to_string(),
            "NRT".to_string(),
            (140.3851, 35.7653),
        ), // NRT
        Airport::new(
            "Aeropuerto de Heathrow".to_string(),
            "LHR".to_string(),
            (-0.4543, 51.4700),
        ), // LHR
        Airport::new(
            "Aeropuerto de Fráncfort del Meno".to_string(),
            "FRA".to_string(),
            (8.5706, 50.0333),
        ), // FRA
        Airport::new(
            "Aeropuerto de Sídney".to_string(),
            "SYD".to_string(),
            (151.1772, -33.9461),
        ), // SYD
        Airport::new(
            "Aeropuerto Internacional de San Francisco".to_string(),
            "SFO".to_string(),
            (-122.3790, 37.6213),
        ), // SFO
    ]
}

pub fn get_airport_coordinates(airport: &Option<String>) -> (f64, f64) {
    let Some(airport) = airport else {
        return (0.0, 0.0);
    };
    match airport.as_str() {
        "JFK" => (-73.7781, 40.6413),  // JFK, Nueva York
        "LAX" => (-118.4085, 33.9416), // LAX, Los Ángeles
        "EZE" => (-58.5358, -34.8222), // EZE, Buenos Aires (Ezeiza)
        "CDG" => (2.55, 49.0097),      // CDG, París Charles de Gaulle
        "LHR" => (-0.4543, 51.4700),   // LHR, Londres Heathrow
        "NRT" => (140.3929, 35.7735),  // NRT, Tokio Narita
        "FRA" => (8.5706, 50.0333),    // FRA, Frankfurt
        "SYD" => (151.1772, -33.9399), // SYD, Sídney Kingsford Smith
        "SCL" => (-70.7853, -33.3929), // SCL, Santiago de Chile
        "MIA" => (-80.2906, 25.7957),  // MIA, Miami
        "DFW" => (-97.0379, 32.8968),  // DFW, Dallas/Fort Worth
        "GRU" => (-46.6506, -23.4253), // GRU, São Paulo-Guarulhos
        "MAD" => (-3.5705, 40.4719),   // MAD, Madrid
        "AMS" => (4.7600, 52.3081),    // AMS, Ámsterdam
        "SFO" => (-122.4194, 37.6213), // SFO, San Francisco
        _ => (0.0, 0.0),
    }
}

pub fn get_arrival_airport_position(flight: &Flight, projector: &Projector) -> Pos2 {
    let airport_coordinates = get_airport_coordinates(&Some(flight.arrival_airport.to_string()));
    let airport_position = Position::from_lon_lat(airport_coordinates.0, airport_coordinates.1);
    projector.project(airport_position).to_pos2()
}

pub fn calculate_angle_to_airport(flight_position: Pos2, airport_position: Pos2) -> f32 {
    let delta_x = airport_position.x - flight_position.x;
    let delta_y = airport_position.y - flight_position.y;
    delta_y.atan2(delta_x)
}
