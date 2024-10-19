use node::frame::Frame;
use node::meta_data::nodes::cluster::Cluster;
use node::meta_data::nodes::node::Node;
use node::meta_data::nodes::node_meta_data_acces::NodesMetaDataAccess;
use node::node_communication::query_receiver::QueryReceiver;
use node::parsers::parser_factory::ParserFactory;
use node::response_builders::error_builder::ErrorBuilder;
use node::utils::constants::{CLIENTS_PORT, NODES_METADATA};
use node::utils::errors::Errors;
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;

fn main() {
    print!("node's ip: ");
    io::stdout().flush().unwrap();
    let mut ip = String::new();
    io::stdin().read_line(&mut ip).expect("Error reading ip");
    let ip = ip.trim();

    //let node = Node::new(ip.to_string(), 8080);
    //node.write_to_file("src/node_info.json");

    let nodes = vec![
        Node::new(String::from("127.0.0.1"), 1),
        Node::new(String::from("127.0.0.2"), 2),
        Node::new(String::from("127.0.0.3"), 3),
    ];
    let mut own_node = Node::new(String::from(ip), 1);
    let mut other_nodes = Vec::new();
    for node in nodes {
        if node.get_ip() != ip {
            other_nodes.push(node);
        } else {
            own_node = Node::new(node.get_ip().to_string(), node.get_pos());
        }
    }
    let cluster = Cluster::new(own_node, other_nodes);

    if let Err(e) = NodesMetaDataAccess::write_cluster(NODES_METADATA, &cluster) {
        dbg!(e);
    }

    thread::spawn(move || {
        let _ = QueryReceiver::start_listening();
    });
    let Ok(ip) = NodesMetaDataAccess::get_own_ip(NODES_METADATA) else {
        panic!("No metadata found");
    };
    let listener =
        TcpListener::bind(format!("{}:{}", ip, CLIENTS_PORT)).expect("Error binding socket");
    println!("Servidor escuchando en {}", ip);

    for incoming in listener.incoming() {
        match incoming {
            Ok(stream) => {
                println!("Cliente conectado: {:?}", stream.peer_addr());

                // Mover la conexión a un hilo
                thread::spawn(move || {
                    handle_client(stream);
                });
            }
            Err(e) => {
                println!("Error aceptando la conexión: {}", e);
            }
        }
    }
}

fn handle_client(mut stream: TcpStream) {
    let mut buffer = [0; 1024];
    loop {
        match stream.read(&mut buffer) {
            Ok(0) => {
                println!("Cliente desconectado");
                break;
            }
            Ok(_) => match execute_request(buffer.to_vec()) {
                Ok(response) => {
                    stream
                        .write_all(response.as_slice())
                        .expect("Error writing response");
                }
                Err(e) => {
                    let frame = ErrorBuilder::build_error_frame(
                        Frame::parse_frame(buffer.as_slice()).unwrap(),
                        e,
                    )
                    .unwrap();
                    stream
                        .write_all(frame.to_bytes().as_slice())
                        .expect("Error writing response");
                }
            },
            Err(e) => {
                println!("Error leyendo del socket: {}", e);
                break; // Sal del bucle si hay un error en la lectura
            }
        }
    }
}

fn execute_request(bytes: Vec<u8>) -> Result<Vec<u8>, Errors> {
    let frame = Frame::parse_frame(bytes.as_slice())?;
    frame.validate_request_frame()?;
    let parser = ParserFactory::get_parser(frame.opcode)?;
    let executable = parser.parse(frame.body.as_slice())?;
    let frame = executable.execute(frame)?;

    Ok(frame.to_bytes())
}
