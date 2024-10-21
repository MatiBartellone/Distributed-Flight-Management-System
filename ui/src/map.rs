use eframe::egui;
use walkers::{Map, Position};
use crate::{airports::get_airport_coordinates, flight_app::FlightApp};

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