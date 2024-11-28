use simple_client::cassandra_connector;
use std::io::Write;
use std::io;

/// Get user data from the terminal
pub fn get_user_data(msg: &str) -> String {
    print!("{}", msg);
    io::stdout().flush().expect("Failed to flush stdout");
    let mut data = String::new();
    io::stdin()
        .read_line(&mut data)
        .expect("Error reading data");
    data.trim().to_string()
}

fn main() -> Result<(), String> {
    let node = get_user_data("FULL IP (ip:port): ");
    let mut cassandra_connector = cassandra_connector::CassandraConnection::new(&node)?;
    loop {
        let frame = get_user_data("Frame:");
        println!("{}", cassandra_connector.send_and_receive(frame)?);
    }
}