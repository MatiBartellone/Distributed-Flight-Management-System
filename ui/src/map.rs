use walkers::{HttpTiles, MapMemory, sources::OpenStreetMap};
use egui::Context;
use eframe::egui;
use crate::flight_app::FlightApp;

pub struct RightPanel{
    tiles: HttpTiles,
    map_memory: MapMemory,
}

impl RightPanel {
    pub fn new(egui_ctx: Context) -> Self {
        Self {
            tiles: HttpTiles::new(OpenStreetMap, egui_ctx),
            map_memory: MapMemory::default(),
        }
    }
}

impl RightPanel {
    pub fn ui(&self, ui: &mut egui::Ui, app: &mut FlightApp) {
        self.draw_map(ui, app);
    }

    fn draw_map(&self, ui: &mut egui::Ui, app: &mut FlightApp) {
        unimplemented!()
    }
}