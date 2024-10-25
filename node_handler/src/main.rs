use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;
use termion::{color, style}; // Para el color verde
use serde::{Deserialize, Serialize};

// Estructura para guardar la información de los nodos (IP, posición y número de clientes)
#[derive(Clone)]
struct NodeInfo {
    ip: String,
    position: usize,
    clients: i32, // Número de clientes conectados al nodo
}

#[derive(Clone, Serialize, Deserialize)]
struct Node {
    ip: String,
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
                position: node.position,
            })
            .collect::<Vec<_>>();

        stream.write_all(&serde_json::to_vec(&nodes_list).unwrap()).unwrap();

        nodes_guard.insert(
            new_node.ip.to_string(),
            NodeInfo {
                ip: new_node.ip.to_string(),
                position: new_node.position,
                clients: 0, // Iniciamos con 0 clientes conectados
            },
        );
    } // Aquí se libera el lock

    // Mostrar la lista de nodos activos en la terminal
    clear_screen();
    print_node_list(nodes.clone());

    // Informar a los demás nodos sobre la nueva IP y su posición
    broadcast_new_node(new_node.ip.to_string(), new_node.position, nodes.clone());

    // Bucle de manejo de clientes
    let mut buffer = [0; 1024];
    loop {
        match stream.read(&mut buffer) {
            Ok(0) => {
                {
                    let mut nodes_guard = nodes.lock().unwrap();
                    nodes_guard.remove(&new_node.ip);
                }
                clear_screen();
                print_node_list(Arc::clone(&nodes)); // Pasar el Arc<Mutex<_>> completo
                break;
            }
            Ok(1) => {
                {
                    let mut nodes_guard = nodes.lock().unwrap();
                    if let Some(node) = nodes_guard.get_mut(&new_node.ip) {
                        node.clients += 1; // Incrementar el contador de clientes
                    }
                }

                print_node_list(Arc::clone(&nodes)); // Pasar el Arc<Mutex<_>> completo
            },
            Ok(_) => {
                handle_client_connection_notification(new_node.ip.to_string(), nodes.clone(), -1);
            },
            Err(_) => break,
        }
    }
}

fn clear_screen() {
    // Código ANSI para limpiar la pantalla y mover el cursor al inicio
    print!("\x1B[2J\x1B[1;1H");
    std::io::stdout().flush().unwrap(); // Asegura que se envía el comando inmediatamente
}

fn broadcast_new_node(new_node_ip: String, new_node_position: usize, nodes: Arc<Mutex<HashMap<String, NodeInfo>>>) {
    let nodes_guard = nodes.lock().unwrap(); // Obtener el lock
    for (node_ip, _) in nodes_guard.iter() {
        if node_ip != &new_node_ip {
            if let Ok(mut stream) = TcpStream::connect(format!("{}:{}", node_ip, 7676)) {
                stream.write_all(
                    &serde_json::to_vec(&Node {
                        ip: new_node_ip.to_string(),
                        position: new_node_position,
                    })
                        .unwrap(),
                ).unwrap();
            }
        }
    }
}

fn handle_client_connection_notification(
    node_ip: String,
    nodes: Arc<Mutex<HashMap<String, NodeInfo>>>,
    client: i32,
) {
    // Actualizar el número de clientes de un nodo específico
    {
        let mut nodes_guard = nodes.lock().unwrap();
        if let Some(node) = nodes_guard.get_mut(&node_ip) {
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
            "IP: {} | Posición: {} | Clientes: {} - {}ACTIVE{}",
            node.ip,
            node.position,
            node.clients,
            color::Fg(color::Green),
            color::Fg(color::Reset)
        );
    }
    println!(); // Línea en blanco
}

fn main() {
    clear_screen();
    let listener = TcpListener::bind("127.0.0.1:7878").unwrap();
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
