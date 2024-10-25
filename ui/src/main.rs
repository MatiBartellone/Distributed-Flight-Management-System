use app::{cassandra_client::CassandraClient, flight_app::FlightApp, utils::errors::Errors};

fn main() -> Result<(), Errors> {
    let cassandra_client = CassandraClient::new("127.0.0.1", "8080")?;
    cassandra_client.inicializate;

    let options = eframe::NativeOptions::default();
    eframe::run_native(
        "Flight App",
        options,
        Box::new(|cc| Ok(Box::new(FlightApp::new(cc.egui_ctx.clone(), cassandra_client)))),
    )
}
