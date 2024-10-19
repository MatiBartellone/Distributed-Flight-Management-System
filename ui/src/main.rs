use app::flight_app::FlightApp;

fn main() -> Result<(), eframe::Error> {
    //env_logger::init();
    let options = eframe::NativeOptions::default();
    eframe::run_native(
        "Flight App",
        options,
        Box::new(|_cc| Box::new(FlightApp::default())),
    )
}