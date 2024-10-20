use eframe::egui;
use walkers::{Map, Position};
use crate::flight_app::FlightApp;

pub struct MapPanel;

impl MapPanel {
    pub fn ui(&self, ui: &mut egui::Ui, app: &mut FlightApp) {
        self.draw_map(ui, app);
    }

    fn draw_map(&self, ui: &mut egui::Ui, app: &mut FlightApp) {
        let (lon, lat) = get_airport_coordinates(&app.selected_airport);
        let my_position = Position::from_lon_lat(lon, lat);
        let mut map_widget = Map::new(Some(&mut app.tiles), &mut app.map_memory, my_position);
        map_widget = map_widget.with_plugin(app.flights.clone());
        ui.add(map_widget);
    }
}

fn get_airport_coordinates(airport: &Option<String>) -> (f64, f64) {
    let Some(airport) = airport else {return (0.0, 0.0)}; 
    match airport.as_str() {
        "JFK" => (-73.7781, 40.6413),  // JFK, Nueva York
        "LAX" => (-118.4085, 33.9416), // LAX, Los Ángeles
        "EZE" => (-58.5358, -34.8222), // EZE, Buenos Aires (Ezeiza)
        "CDG" => (2.55, 49.0097),      // CDG, París Charles de Gaulle
        "LHR" => (-0.4543, 51.4700),   // LHR, Londres Heathrow
        "NRT" => (140.3929, 35.7735),  // NRT, Tokio Narita
        "FRA" => (8.5706, 50.0333),    // FRA, Frankfurt
        "SYD" => (151.1772, -33.9399), // SYD, Sídney Kingsford Smith
        _ => (0.0, 0.0)
    }
}