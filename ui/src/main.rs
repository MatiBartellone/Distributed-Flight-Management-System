use app::flight_app::FlightApp;

fn main() -> Result<(), eframe::Error> {
    //env_logger::init();
    //let cassandra_client = inicialite_cassandra_client();
    let options = eframe::NativeOptions::default();
    eframe::run_native(
        "Flight App",
        options,
        Box::new(|cc| Ok(Box::new(FlightApp::new(cc.egui_ctx.clone())))),
    )
}

// fn inicialite_cassandra_client() -> CassandraClient {
//     todo!()
// }
