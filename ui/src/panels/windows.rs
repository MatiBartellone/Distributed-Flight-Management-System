use egui::{Align2, RichText, Ui, Window};
use egui::{Color32, Frame, Shadow};
use walkers::MapMemory;

/// Simple GUI to zoom in and out.
pub fn zoom(ui: &Ui, map_memory: &mut MapMemory) {
    Window::new("Map")
        .collapsible(false)
        .resizable(false)
        .title_bar(false)
        .anchor(Align2::RIGHT_BOTTOM, [10., -10.])
        .show(ui.ctx(), |ui| {
            ui.horizontal(|ui| {
                if ui.button(RichText::new("➕").heading()).clicked() {
                    let _ = map_memory.zoom_in();
                }

                if ui.button(RichText::new("➖").heading()).clicked() {
                    let _ = map_memory.zoom_out();
                }
            });
        });
}

/// When map is "detached", show a windows with an option to go back to my position.
pub fn go_to_my_position(ui: &Ui, map_memory: &mut MapMemory) {
    if map_memory.detached().is_some() {
        Window::new("Center")
            .collapsible(false)
            .resizable(false)
            .title_bar(false)
            .anchor(Align2::RIGHT_TOP, [-10., -10.])
            .frame(Frame {
                shadow: Shadow::NONE,
                fill: Color32::from_black_alpha(0),
                ..Default::default()
            })
            .show(ui.ctx(), |ui| {
                if ui.button(RichText::new("Center Map").heading()).clicked() {
                    map_memory.follow_my_position();
                }
            });
    }
}
