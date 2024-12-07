use std::{
    collections::HashMap, sync::{Arc, Mutex}, thread, time::Duration
};

use crate::{airport_implementation::airport::Airport, cassandra_comunication::ui_client::UIClient, flight_implementation::{flight::Flight, flight_selected::FlightSelected}};

#[derive(Clone)]
pub struct AppUpdater {
    selected_flight: Arc<Mutex<Option<FlightSelected>>>,
    selected_airport_code: Arc<Mutex<Option<String>>>,
    flights: Arc<Mutex<Vec<Flight>>>,
    ui_client: Arc<Mutex<UIClient>>,
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
            ui_client: Arc::new(Mutex::new(ui_client)),
        }
    }

    pub fn restore_airports(&self, airport_codes: Vec<String>) -> Option<HashMap<String, Airport>> {
        let ui_client_lock = self.ui_client.lock();
        match ui_client_lock {
            Ok(mut ui_client) => Some(ui_client.get_airports(airport_codes)),
            Err(_) => None,
        }
    }

    /// Start the app updater thread
    pub fn start(&self, ctx: egui::Context) {
        let mut self_clone = self.clone();
        thread::spawn(move || loop {
            self_clone.update_flights();
            ctx.request_repaint();
            thread::sleep(Duration::from_millis(200));
        });
    }

    fn get_airport_code(&self) -> Option<String> {
        let lock = self.selected_airport_code.lock();
        match lock {
            Ok(lock) => lock.clone(),
            Err(_) => None,
        }
    }

    fn update_flights(&mut self) {
        let Some(airport_code) = self.get_airport_code() else {return;};
        self.load_selected_flight(&airport_code);
        self.load_flights(&airport_code);
    }

    fn get_selected_flight(&self) -> Option<String> {
        let selected_flight_lock = self.selected_flight.lock();
        if let Ok(selected_flight_lock) = selected_flight_lock {
            if let Some(selected_flight) = &*selected_flight_lock {
                return Some(selected_flight.get_code());
            }
        }
        None
    }

    // Gets the selected flight code, locks de ui_client to get the selected flight information,
    // sees if the selected flight code is the same as the one that was selected before, and if it is not,
    // it returns without doing anything. If it is the same, it updates the selected flight information.
    fn load_selected_flight(&mut self, airport_code: &str) {
        if let Some(selected_flight_code) = self.get_selected_flight() {
            let ui_client_lock = self.ui_client.lock();
            if let Ok(mut ui_client) = ui_client_lock {
                let selected_flight = ui_client.get_flight_selected(&selected_flight_code, airport_code);
                let selected_flight_lock = self.selected_flight.lock();
                if let Ok(mut selected_flight_lock) = selected_flight_lock {
                    if let Some(existing_flight) = &*selected_flight_lock {
                        if existing_flight.get_code() != selected_flight_code {
                            return;
                        }
                    }
                    *selected_flight_lock = selected_flight;
                }
            }
        }
    }

    fn update_flights_data(&self, flights_information: Vec<Flight>) {
        if let Ok(mut flights_lock) = self.flights.lock() {
            *flights_lock = flights_information;
        }
    }

    fn load_flights(&mut self, airport_code: &str) {
        let mut ui_client = match self.ui_client.lock() {
            Ok(ui_client) => ui_client,
            Err(_) => return,
        };
        let flights_information = ui_client.get_flights(airport_code);
        drop(ui_client);
        
        match self.get_airport_code() {
            Some(actual_airport_code) if actual_airport_code == airport_code => self.update_flights_data(flights_information),
            Some(actual_airport_code) => self.load_flights(&actual_airport_code),
            None => self.update_flights_data(Vec::new())
        }
    }
}
