use serde::{Deserialize, Serialize};
use std::io::Write;
use std::net::IpAddr;
use std::fmt::Display;
use std::thread::sleep;
use std::time::Duration;
use termion::{color, style};
use std::env;


fn clear_screen() {
    print!("\x1B[2J\x1B[1;1H");
    std::io::stdout().flush().unwrap();
}

// Función para imprimir la lista de nodos activos en la terminal
fn print_node_list(nodes_vec: &mut Vec<Node>) {
    clear_screen();

    // Convertir los valores del HashMap en un vector y ordenarlos por posición
    nodes_vec.sort_by_key(|node| node.position); // Ordenar por posición ascendente

    // Imprimir la lista de nodos ordenados
    println!(
        "{}{}Nodos activos:{}{}",
        color::Fg(color::Green),
        style::Bold,
        style::Reset,
        color::Fg(color::Reset)
    );

    for node in nodes_vec {
        let color: Box<dyn termion::color::Color> = match node.state {
            State::Active => Box::new(color::Green),
            State::Inactive => Box::new(color::Red),
            State::Booting => Box::new(color::Blue),
            State::StandBy => Box::new(color::Yellow),
            State::ShuttingDown => Box::new(color::Rgb(255, 128, 00)),
            State::Recovering => Box::new(color::Magenta),
        };

        let (seed_color, is_seed): (Box<dyn termion::color::Color>, &str) = match node.is_seed {
            true => (Box::new(color::Green), "Y"),
            false => (Box::new(color::Red), "N")
        };

        println!(
            "IP: {} | Port: {} | Position: {} | is_seed: {}{}{} | STATE: {}{}{}",
            node.ip.ip,
            node.ip.port,
            node.position,
            color::Fg(&*seed_color),
            is_seed,
            color::Fg(color::Reset),
            color::Fg(&*color),
            node.state,
            color::Fg(color::Reset)
        );
    }
    println!(); // Línea en blanco
}

fn main() {

    let args: Vec<String> = env::args().collect();
    let path = &args[1];
    loop {
        let p = path.clone();
        print_node_list(&mut read_cluster(p.as_str()));
        sleep(Duration::from_secs(1))
    }

}

fn read_cluster(path: &str) -> Vec<Node> {
    let content = std::fs::read_to_string(path).expect("Failed to read file");
    let cluster: Cluster = serde_json::from_slice(content.as_bytes()).expect("Failed to deserialize");
    let mut nodes_vec: Vec<Node> = Vec::new();
    nodes_vec.extend(cluster.other_nodes);
    nodes_vec.push(cluster.own_node);
    nodes_vec
}

#[derive(Serialize, Deserialize, Debug)]
struct Cluster {
    own_node: Node,
    other_nodes: Vec<Node>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub enum State {
    Active,
    Inactive,
    Booting,
    StandBy,
    ShuttingDown,
    Recovering,
}

impl Display for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        match self {
            State::Active => write!(f, "Active"),
            State::Inactive => write!(f, "Inactive"),
            State::Booting => write!(f, "Booting"),
            State::StandBy => write!(f, "StandBy"),
            State::ShuttingDown => write!(f, "ShuttingDown"),
            State::Recovering => write!(f, "Recovering"),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Node {
    pub ip: NodeIp,
    pub position: usize,
    pub is_seed: bool,
    pub state: State,
    pub timestamp: Timestamp,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct NodeIp {
    ip: IpAddr,
    port: u16,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Timestamp {
    pub timestamp: i64,
}
