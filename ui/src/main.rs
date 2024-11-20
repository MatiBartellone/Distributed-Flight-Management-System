use std::io::{self, Write};

use app::{app_implementation::flight_app::FlightApp, cassandra_comunication::{cassandra_client::CassandraClient, ui_client::UIClient}, utils::system_functions::clear_screen};

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
    let cant_clients = get_input("Enter the number of clients: ").parse::<usize>().unwrap();
    
    let simulator = UIClient;
    let mut clients = Vec::new();
    for _ in 0..cant_clients {
        let node = get_input("FULL IP (ip:port): ");
        let client = CassandraClient::new(&node)?;
        client.inicializate()?;
        simulator.use_aviation_keyspace(&client)?;
        clients.push(client);
    }
    Ok(clients)
}


// Gets the user input with a message
pub fn get_input(message: &str) -> String {
    println!("{}", message);
    io::stdout().flush().unwrap();
    let mut node = String::new();
    io::stdin().read_line(&mut node).expect("Error reading");
    node.trim().to_string()
}
