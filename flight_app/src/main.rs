use flight_app::{app_implementation::{flight_app::FlightApp, login_app::run_login_app}, cassandra_comunication::{cassandra_client::CassandraClient, ui_client::UIClient}, utils::system_functions::{clear_screen, get_user_data}};

fn main() -> Result<(), eframe::Error> {
    clear_screen();
    let client = match inicializate_client() {
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
        Box::new(|cc| Ok(Box::new(FlightApp::new(cc.egui_ctx.clone(), client)))),
    )
}

fn inicializate_client() -> Result<UIClient, String> {
    let node = get_user_data("FULL IP (ip:port): ");
    let mut client = CassandraClient::new(&node)?;
    client.start_up()?;
    run_login_app(&mut client)?;
    let mut ui_client = UIClient::new(client);
    ui_client.use_aviation_keyspace()?;
    Ok(ui_client)
}