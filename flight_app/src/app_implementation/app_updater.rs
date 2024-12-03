use std::{
    sync::{Arc, Mutex},
    thread,
};

use crate::{cassandra_comunication::{thread_pool_client::ThreadPoolClient, ui_client::UIClient}, flight_implementation::{flight::Flight, flight_selected::FlightSelected}};

pub struct AppUpdater {
    selected_flight: Arc<Mutex<Option<FlightSelected>>>,
    selected_airport_code: Arc<Mutex<Option<String>>>,
    flights: Arc<Mutex<Vec<Flight>>>,
    client: UIClient,
    thread_pool: ThreadPoolClient,
}

impl AppUpdater {
    pub fn new(
        selected_flight: Arc<Mutex<Option<FlightSelected>>>,
        selected_airport_code: Arc<Mutex<Option<String>>>,
        flights: Arc<Mutex<Vec<Flight>>>,
        client: UIClient,
        thread_pool: ThreadPoolClient,
    ) -> Self {
        Self {
            selected_flight,
            selected_airport_code,
            flights,
            client,
            thread_pool,
        }
    }

    /// Start the app updater thread
    pub fn start(self, ctx: egui::Context) {
        thread::spawn(move || loop {
            self.update_flights();
            ctx.request_repaint();
            //thread::sleep(Duration::from_millis(1000));
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

    fn update_flights(&self) {
        let Some(airport_code) = self.get_airport_code() else {return;};
        self.load_selected_flight();
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

    fn load_selected_flight(&self){
        let Some(selected_flight_code) = self.get_selected_flight() else { return };
        let selected_flight = self.client.get_flight_selected(&selected_flight_code, &self.thread_pool);
        if let Ok(mut selected_flight_lock) = self.selected_flight.lock() {
            *selected_flight_lock = selected_flight;
        }
    }

    fn update_flights_data(&self, flights_information: Vec<Flight>) {
        if let Ok(mut flights_lock) = self.flights.lock() {
            *flights_lock = flights_information;
        }
    }

    fn load_flights(&self, airport_code: &str) {
        let mut flights_information = self
            .client
            .get_flights(airport_code, &self.thread_pool);
        flights_information.sort_by_key(|flight| flight.code.to_string());
        
        match self.get_airport_code() {
            Some(actual_airport_code) if actual_airport_code == airport_code => self.update_flights_data(flights_information),
            Some(actual_airport_code) => self.load_flights(&actual_airport_code),
            None => self.update_flights_data(flights_information)
        }
    }
}
