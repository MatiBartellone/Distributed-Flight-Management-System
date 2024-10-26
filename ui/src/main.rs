use app::{cassandra_client::CassandraClient, flight_app::FlightApp};

fn main() -> Result<(), eframe::Error> {
    let mut cassandra_client = match CassandraClient::new("127.0.0.1", 8080){
        Ok(cliente) => cliente,
        Err(_) => return {
            println!("Error conectando al servidor");
            Ok(())
        }
    };
    cassandra_client.inicializate();

    let options = eframe::NativeOptions::default();
    eframe::run_native(
        "Flight App",
        options,
        Box::new(|cc| Ok(Box::new(FlightApp::new(cc.egui_ctx.clone(), cassandra_client)))),
    )
}
