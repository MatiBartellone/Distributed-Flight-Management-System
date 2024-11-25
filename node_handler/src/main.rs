use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{IpAddr, TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::{io, thread};
use termion::{color, style};
use termion::color::{Color, Fg};
// Para el color verde

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
        };

        println!(
            "IP: {} | Port: {} | Position: {} - {}STATE: {}{}",
            node.ip.ip.to_string(),
            node.ip.port.to_string(),
            node.position,
            color,
            node.state.to_string(),
            color::Fg(color::Reset)
        );
    }
    println!(); // Línea en blanco
}

fn main() {
    print!("Enter a node meta data path");
    io::stdout().flush().expect("Failed to flush stdout");
    let mut path = String::new();
    io::stdin()
        .read_line(&mut path)
        .expect("Error reading data");
    path.trim().to_string();

}

fn read_cluster(path: &str) -> Cluster {
    let content = std::fs::read_to_string(path).expect("Failed to read file");
    let cluster: Cluster = serde_json::from_slice(content.as_bytes()).expect("Failed to deserialize");
    cluster
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
