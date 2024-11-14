use std::io::{self, Write};

use app::{cassandra_comunication::ui_client::UIClient, flight_app::FlightApp};

fn main() -> Result<(), eframe::Error> {
    let node = get_input("FULL IP (ip:port): ");
    let cassandra = match inicializate_client(&node) {
        Ok(cliente) => cliente,
        Err(e) => {
            println!("{}", e);
            return Ok(());
        }
    };

    let options = eframe::NativeOptions::default();
    eframe::run_native(
        "Flight App",
        options,
        Box::new(|cc| Ok(Box::new(FlightApp::new(cc.egui_ctx.clone(), cassandra)))),
    )
}

// Gets the user input with a message
fn get_input(message: &str) -> String {
    println!("{}", message);
    io::stdout().flush().unwrap();
    let mut node = String::new();
    io::stdin().read_line(&mut node).expect("Error reading");
    node.trim().to_string()
}

fn inicializate_client(node: &str) -> Result<UIClient, String> {
    let client = UIClient::new(node)?;
    client.inicializate()?;
    client.use_aviation_keyspace()?;
    Ok(client)
}
