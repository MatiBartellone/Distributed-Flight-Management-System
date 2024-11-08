use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::{io, thread};
use termion::{color, style}; // Para el color verde

// Estructura para guardar la información de los nodos (IP, posición y número de clientes)
#[derive(Clone)]
struct NodeInfo {
    ip: String,
    port: String,
    position: usize,
    clients: i32, // Número de clientes conectados al nodo
}

#[derive(Clone, Serialize, Deserialize)]
struct Node {
    ip: String,
    port: String,
    position: usize,
}

fn handle_client(mut stream: TcpStream, nodes: Arc<Mutex<HashMap<String, NodeInfo>>>) {
    let mut buffer = [0; 1024];
    let size = stream.read(&mut buffer).unwrap();
    let new_node: Node = serde_json::from_slice(&buffer[..size]).unwrap();

    // Agregar el nodo a la lista con su IP, posición y clientes
    {
        let mut nodes_guard = nodes.lock().unwrap();

        // Enviar la lista de nodos activos al nodo recién conectado
        let nodes_list = nodes_guard
            .values()
            .map(|node| Node {
                ip: node.ip.to_string(),
                port: node.port.to_string(),
                position: node.position,
            })
            .collect::<Vec<_>>();

        stream
            .write_all(&serde_json::to_vec(&nodes_list).unwrap())
            .unwrap();
        let full_ip = format!("{}:{}", new_node.ip, new_node.port);
        nodes_guard.insert(
            full_ip.to_string(),
            NodeInfo {
                ip: new_node.ip.to_string(),
                port: new_node.port.to_string(),
                position: new_node.position,
                clients: 0, // Iniciamos con 0 clientes conectados
            },
        );
    } // Aquí se libera el lock

    // Mostrar la lista de nodos activos en la terminal
    clear_screen();
    print_node_list(nodes.clone());

    // Informar a los demás nodos sobre la nueva IP y su posición
    broadcast_new_node(
        new_node.ip.to_string(),
        new_node.port.to_string(),
        new_node.position,
        nodes.clone(),
    );

    // Bucle de manejo de clientes
    let mut buffer = [0; 1024];
    loop {
        match stream.read(&mut buffer) {
            Ok(0) => {
                {
                    let mut nodes_guard = nodes.lock().unwrap();
                    nodes_guard.remove(&format!("{}:{}", new_node.ip, new_node.port));
                }
                clear_screen();
                print_node_list(Arc::clone(&nodes)); // Pasar el Arc<Mutex<_>> completo
                break;
            }
            Ok(1) => {
                {
                    let mut nodes_guard = nodes.lock().unwrap();
                    if let Some(node) =
                        nodes_guard.get_mut(&format!("{}:{}", new_node.ip, new_node.port))
                    {
                        node.clients += 1; // Incrementar el contador de clientes
                    }
                }

                print_node_list(Arc::clone(&nodes)); // Pasar el Arc<Mutex<_>> completo
            }
            Ok(_) => {
                handle_client_connection_notification(
                    format!("{}:{}", new_node.ip, new_node.port).to_string(),
                    nodes.clone(),
                    -1,
                );
            }
            Err(_) => break,
        }
    }
}

fn clear_screen() {
    print!("\x1B[2J\x1B[1;1H");
    std::io::stdout().flush().unwrap();
}

fn broadcast_new_node(
    new_node_ip: String,
    new_node_port: String,
    new_node_position: usize,
    nodes: Arc<Mutex<HashMap<String, NodeInfo>>>,
) {
    let nodes_guard = nodes.lock().unwrap(); // Obtener el lock
    for (full_ip, node_info) in nodes_guard.iter() {
        if full_ip != &format!("{}:{}", new_node_ip, new_node_port) {
            let port = (new_node_port.parse::<i32>().unwrap() + 4).to_string();
            if let Ok(mut stream) = TcpStream::connect(format!("{}:{}", node_info.ip, port)) {
                stream
                    .write_all(
                        &serde_json::to_vec(&Node {
                            ip: new_node_ip.to_string(),
                            port: new_node_port.to_string(),
                            position: new_node_position,
                        })
                        .unwrap(),
                    )
                    .unwrap();
            }
        }
    }
}

fn handle_client_connection_notification(
    full_ip: String,
    nodes: Arc<Mutex<HashMap<String, NodeInfo>>>,
    client: i32,
) {
    // Actualizar el número de clientes de un nodo específico
    {
        let mut nodes_guard = nodes.lock().unwrap();
        if let Some(node) = nodes_guard.get_mut(&full_ip) {
            node.clients += client; // Incrementar o reducir el contador de clientes
        }
    } // El lock se libera aquí

    // Mostrar la lista de nodos actualizada en la terminal
    print_node_list(Arc::clone(&nodes));
}

// Función para imprimir la lista de nodos activos en la terminal
fn print_node_list(nodes: Arc<Mutex<HashMap<String, NodeInfo>>>) {
    clear_screen();
    let nodes_guard = nodes.lock().unwrap(); // Obtener el lock

    // Convertir los valores del HashMap en un vector y ordenarlos por posición
    let mut nodes_vec: Vec<&NodeInfo> = nodes_guard.values().collect();
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
        println!(
            "IP: {} | Port: {} | Position: {} | Clients: {} - {}ACTIVE{}",
            node.ip,
            node.port,
            node.position,
            node.clients,
            color::Fg(color::Green),
            color::Fg(color::Reset)
        );
    }
    println!(); // Línea en blanco
}

fn main() {
    print!("Will this be used across netowrk? [Y][N]: ");
    io::stdout().flush().expect("Failed to flush stdout");
    let mut network = String::new();
    io::stdin()
        .read_line(&mut network)
        .expect("Error reading data");
    network.trim().to_string();
    let ip = match network.as_str() {
        "Y" => "0.0.0.0:7878".to_string(),
        _ => "127.0.0.1:7878".to_string(),
    };


    clear_screen();
    let listener = TcpListener::bind(ip).unwrap();
    let nodes = Arc::new(Mutex::new(HashMap::new()));

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let nodes = Arc::clone(&nodes);
                thread::spawn(move || {
                    handle_client(stream, nodes);
                });
            }
            Err(e) => {
                eprintln!("Connection failed: {}", e);
            }
        }
    }
}
