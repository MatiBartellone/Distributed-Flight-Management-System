use crate::{
    flight_app::FlightApp,
    windows::{go_to_my_position, zoom},
};
use eframe::egui;
use walkers::{Map, Position};

pub struct MapPanel;

impl MapPanel {
    pub fn ui(&self, ui: &mut egui::Ui, app: &mut FlightApp) {
        self.draw_map(ui, app);
    }

    fn draw_map(&self, ui: &mut egui::Ui, app: &mut FlightApp) {
        let (lon, lat) = self.get_inicial_coordinates(app);
        let my_position = Position::from_lon_lat(lon, lat);
        let mut map_widget = Map::new(Some(&mut app.tiles), &mut app.map_memory, my_position);

        // Intenta abrir el lock del aeropuerto seleccionado
        let selected_airport = match app.selected_airport.lock() {
            Ok(lock) => lock,
            Err(_) => return,
        };

        let aiport_is_some = (&*selected_airport).is_some();
        drop(selected_airport);
        map_widget = map_widget.with_plugin(&mut app.airports);

        if aiport_is_some {
            map_widget = map_widget.with_plugin(&mut app.flights);
        }

        ui.add(map_widget);
        zoom(ui, &mut app.map_memory);
        go_to_my_position(ui, &mut app.map_memory);
    }

    fn get_inicial_coordinates(&self, app: &mut FlightApp) -> (f64, f64) {
        let selected_airport = match app.selected_airport.lock() {
            Ok(lock) => lock,
            Err(_) => return (0., 0.),
        };
        if let Some(airport) = &*selected_airport {
            return airport.position;
        }
        (0., 0.)
    }
}
