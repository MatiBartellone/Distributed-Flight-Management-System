use app::{app_implementation::flight_app::FlightApp, cassandra_comunication::{cassandra_client::CassandraClient, ui_client::UIClient}, utils::system_functions::{clear_screen, get_user_data}};

fn main() -> Result<(), eframe::Error> {
    clear_screen();
    let clients = match inicializate_clients() {
        Ok(clients) => clients,
        Err(e) => {
            println!("{}", e);
            return Ok(());
        }
    };
    clear_screen();

    let options = eframe::NativeOptions::default();
    eframe::run_native(
        "Flight App",
        options,
        Box::new(|cc| Ok(Box::new(FlightApp::new(cc.egui_ctx.clone(), clients)))),
    )
}

fn inicializate_clients() -> Result<Vec<CassandraClient>, String> {
    let cant_clients = get_user_data("Enter the number of clients: ").parse::<usize>()
        .or_else(|_| Err("Error parsing the number of clients".to_string()))?;
    
    //env_logger::init();
    let simulator = UIClient;
    let mut clients = Vec::new();
    for _ in 0..cant_clients {
        let node = get_user_data("FULL IP (ip:port): ");
        let client = CassandraClient::new(&node)?;
        client.inicializate()?;
        simulator.use_aviation_keyspace(&client)?;
        clients.push(client);
    }
    Ok(clients)
}