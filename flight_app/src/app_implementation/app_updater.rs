use std::{
    sync::{Arc, Mutex},
    thread, time::Duration,
};

use crate::{cassandra_comunication::ui_client::UIClient, flight_implementation::{flight::Flight, flight_selected::FlightSelected}};

pub struct AppUpdater {
    selected_flight: Arc<Mutex<Option<FlightSelected>>>,
    selected_airport_code: Arc<Mutex<Option<String>>>,
    flights: Arc<Mutex<Vec<Flight>>>,
    ui_client: UIClient
}

impl AppUpdater {
    pub fn new(
        selected_flight: Arc<Mutex<Option<FlightSelected>>>,
        selected_airport_code: Arc<Mutex<Option<String>>>,
        flights: Arc<Mutex<Vec<Flight>>>,
        ui_client: UIClient,
    ) -> Self {
        Self {
            selected_flight,
            selected_airport_code,
            flights,
            ui_client
        }
    }

    /// Start the app updater thread
    pub fn start(mut self, ctx: egui::Context) {
        thread::spawn(move || loop {
            self.update_flights();
            ctx.request_repaint();
            thread::sleep(Duration::from_millis(200));
        });
    }

    fn get_airport_code(&self) -> Option<String> {
        if let Ok(lock) = self.selected_airport_code.lock() {
            if let Some(code) = &*lock {
                return Some(code.to_string())
            }
        }
        None
    }

    fn update_flights(&mut self) {
        let Some(airport_code) = self.get_airport_code() else {return;};
        self.load_selected_flight(&airport_code);
        self.load_flights(&airport_code);
    }

    fn get_selected_flight(&self) -> Option<String> {
        if let Ok(selected_flight_lock) = self.selected_flight.lock() {
            if let Some(selected_flight) = &*selected_flight_lock {
                return Some(selected_flight.get_code())
            }
        }
        None
    }

    fn load_selected_flight(&mut self, airport_code: &str) {
        let Some(selected_flight_code) = self.get_selected_flight() else { return };
        let selected_flight = self.ui_client.get_flight_selected(&selected_flight_code, airport_code);
        if let Ok(mut selected_flight_lock) = self.selected_flight.lock() {
            *selected_flight_lock = selected_flight;
        }
    }

    fn update_flights_data(&self, flights_information: Vec<Flight>) {
        if let Ok(mut flights_lock) = self.flights.lock() {
            *flights_lock = flights_information;
        }
    }

    fn load_flights(&mut self, airport_code: &str) {
        let flights_information = self
            .ui_client
            .get_flights(airport_code);
        
        match self.get_airport_code() {
            Some(actual_airport_code) if actual_airport_code == airport_code => self.update_flights_data(flights_information),
            Some(actual_airport_code) => self.load_flights(&actual_airport_code),
            None => self.update_flights_data(flights_information)
        }
    }
}
