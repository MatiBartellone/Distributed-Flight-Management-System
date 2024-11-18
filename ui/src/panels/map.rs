use eframe::egui;
use walkers::{Map, Position};

use crate::app_implementation::flight_app::FlightApp;

use super::windows::{go_to_my_position, zoom};

pub struct MapPanel;

impl MapPanel {
    pub fn ui(&self, ui: &mut egui::Ui, app: &mut FlightApp) {
        self.draw_map(ui, app);
    }

    fn draw_map(&self, ui: &mut egui::Ui, app: &mut FlightApp) {
        let (lon, lat) = self.get_inicial_coordinates(app);
        let my_position = Position::from_lon_lat(lon, lat);
        let mut map_widget = Map::new(Some(&mut app.tiles), &mut app.map_memory, my_position);
        map_widget = map_widget.with_plugin(&mut app.airports);

        if FlightApp::is_airport_selected(&app.selected_airport_code) {
            map_widget = map_widget.with_plugin(&mut app.flights);
        }

        ui.add(map_widget);
        zoom(ui, &mut app.map_memory);
        go_to_my_position(ui, &mut app.map_memory);
    }

    fn get_inicial_coordinates(&self, app: &mut FlightApp) -> (f64, f64) {
        let selected_airport_code = match app.selected_airport_code.lock() {
            Ok(lock) => lock,
            Err(_) => return (0., 0.),
        };
        if let Some(code) = &*selected_airport_code {
            return app.airports.get_airport_coordinates(code);
        }
        (0., 0.)
    }
}
