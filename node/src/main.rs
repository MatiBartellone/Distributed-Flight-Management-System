use node::data_access::data_access_handler::DataAccessHandler;
use node::gossip::seed_listener::SeedListener;
use node::meta_data::meta_data_handler::MetaDataHandler;
use node::meta_data::nodes::cluster::Cluster;
use node::meta_data::nodes::node::Node;
use node::meta_data::nodes::node_meta_data_acces::NodesMetaDataAccess;
use node::node_communication::query_receiver::QueryReceiver;
use node::parsers::parser_factory::ParserFactory;
use node::response_builders::error_builder::ErrorBuilder;
use node::utils::constants::{nodes_meta_data_path, SEED_LISTENER_MOD};
use node::utils::errors::Errors;
use node::utils::frame::Frame;
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;
use std::thread::sleep;
use std::time::Duration;
use node::gossip::gossip_emitter::GossipEmitter;
use node::gossip::gossip_listener::GossipListener;

fn main() {
    //let server_addr = "127.0.0.1:7878";

    let (ip, uses_network) =
        match get_user_data("Will this be used across netowrk? [Y][N]: ").as_str() {
            "Y" => (get_user_data("Device's ip (e.g. tailscale): "), true),
            _ => (get_user_data("Node's ip: "), false),
        };
    let (ip, network_ip) = match uses_network {
        true => ("0.0.0.0".to_string(), ip),
        false => (ip.to_string(), ip),
    };
    let port = get_user_data("Node's port ([port, port+4] are used): ");
    let position = get_user_data("Node's position in cluster: ")
        .parse::<i32>()
        .expect("Error in parsing position to int") as usize;

    let (seed_ip, seed_port, is_first) = match get_user_data("Is this the fisrst node? [Y][N]: ").as_str() {
        "Y" => ("".to_string(), "".to_string(), true),
        _ => (get_user_data("Seed node ip: "),get_user_data("Seed node port: "), false),
    };

    let is_seed = match is_first {
        true => true,
        _ => match get_user_data("Is this a seed node? [Y][N]: ").as_str() {
            "Y" => true,
            _ => false,
        },
    };

    let node = Node::new(network_ip.to_string(), port.to_string(), position, is_seed)
        .expect("Error creating node");

    set_cluster(node, seed_ip, seed_port, is_first);

    //listen_incoming_new_nodes(ip.to_string(), port.to_string());

    start_listeners(&ip, &port, is_seed);

    start_gossip().expect("Error starting gossip");

    set_node_listener(ip.to_string(), port.to_string());
}

// fn add_node_to_cluster(node: Node) -> Result<(), Errors> {
//     let mut stream = MetaDataHandler::establish_connection()?;
//     MetaDataHandler::get_instance(&mut stream)?
//         .get_nodes_metadata_access()
//         .append_new_node(nodes_meta_data_path().as_ref(), node)?;
//     Ok(())
// }

fn get_user_data(msg: &str) -> String {
    print!("{}", msg);
    io::stdout().flush().expect("Failed to flush stdout");
    let mut data = String::new();
    io::stdin()
        .read_line(&mut data)
        .expect("Error reading data");
    data.trim().to_string()
}

fn start_listeners(ip: &String, port: &String, is_seed: bool) {
    let (meta_data_ip, meta_data_port) = (ip.to_string(), port.to_string());
    let (data_access_ip, data_access_port) = (ip.to_string(), port.to_string());
    let (query_receiver_ip, query_receiver_port) = (ip.to_string(), port.to_string());
    let (gossip_listener_ip, gossip_listener_port) = (ip.to_string(), port.to_string());
    thread::spawn(move || {
        MetaDataHandler::start_listening(meta_data_ip, meta_data_port)
            .expect("Failed to start metadata listener");
    });
    thread::spawn(move || {
        DataAccessHandler::start_listening(data_access_ip, data_access_port)
            .expect("Failed to start data access");
    });
    thread::spawn(move || {
        QueryReceiver::start_listening(query_receiver_ip, query_receiver_port)
            .expect("Failed to start query receiver");
    });
    thread::spawn(move || {
        GossipListener::start_listening(gossip_listener_ip, gossip_listener_port)
            .expect("Failed to start gossip listener");
    });
    if is_seed {
        let (seed_ip, seed_port) = (ip.to_string(), port.to_string());
        thread::spawn(move || SeedListener::start_listening(seed_ip, seed_port));
    }
}

fn start_gossip() -> Result<(), Errors> {
    thread::spawn(move || -> Result<(), Errors> {
        loop {
            sleep(Duration::from_secs(1));
            GossipEmitter::start_gossip()?;
        }
    });
    Ok(())
}

fn set_cluster(node: Node, seed_ip: String, seed_port: String, is_first: bool) {
    let mut nodes = Vec::<Node>::new();
    if !is_first {
        let seed_listener_port = seed_port.parse::<i32>().expect("Error in parsing seed port") + SEED_LISTENER_MOD;
        let mut stream = TcpStream::connect(format!("{}:{}", seed_ip, seed_listener_port)).expect("Error connecting to seed");
        stream
            .write_all(serde_json::to_string(&node).expect("").as_bytes())
            .expect("Error writing to seed");
        let mut buffer = [0; 1024];
        let size = stream
            .read(&mut buffer)
            .expect("Failed to read from server stream");
        nodes = serde_json::from_slice(&buffer[..size]).expect("Failed to deserialize json");
    }
    let cluster = Cluster::new(node, nodes);
    if let Err(e) = NodesMetaDataAccess::write_cluster(nodes_meta_data_path().as_ref(), &cluster) {
        println!("{}", e);
    }
}

// fn listen_incoming_new_nodes(ip: String, port: String) {
//     thread::spawn(move || {
//         // Mantener el nodo corriendo y escuchando nuevos mensajes
//         let port = (port.parse::<i32>().expect("Failed to parse port into int") + 4).to_string();
//         let listener =
//             TcpListener::bind(format!("{}:{}", ip, port)).expect("Failed to bind TCP listener");
//         for mut stream in listener.incoming().flatten() {
//             // Leer nuevos mensajes del servidor (nuevos nodos que se conectan)
//             let mut buffer = [0; 1024];
//             let size = stream
//                 .read(&mut buffer)
//                 .expect("Failed to read from server stream");
//             if size > 0 {
//                 let node: Node =
//                     serde_json::from_slice(&buffer[..size]).expect("Failed to deserialize json");
//                 {
//                     add_node_to_cluster(node).expect("Failed to add node to cluster");
//                 }
//             }
//         }
//     });
// }

fn set_node_listener(ip: String, port: String) {
    let listener = TcpListener::bind(format!("{}:{}", ip, port)).expect("Error binding socket");
    println!("Servidor escuchando en {}:{}", ip, port);
    for incoming in listener.incoming() {
        match incoming {
            Ok(stream) => {
                println!("Cliente conectado: {:?}", stream.peer_addr());
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
        stream.flush().expect("Failed to flush client");
        match stream.read(&mut buffer) {
            Ok(0) => {
                println!("Cliente desconectado");
                break;
            }
            Ok(n) => match execute_request(buffer[0..n].to_vec()) {
                Ok(response) => {
                    stream.flush().expect("Failed to flush client");
                    stream
                        .write_all(response.as_slice())
                        .expect("Error writing response");
                }
                Err(e) => {
                    let frame = ErrorBuilder::build_error_frame(
                        Frame::parse_frame(buffer.as_slice()).expect("Failed to parse frame"),
                        e,
                    )
                    .expect("Failed to build error frame");
                    stream.flush().expect("Failed to flush client");
                    stream
                        .write_all(frame.to_bytes().as_slice())
                        .expect("Error writing response");
                }
            },
            Err(e) => {
                println!("Error leyendo del socket: {}", e);
                break;
            }
        }
    }
}

fn execute_request(bytes: Vec<u8>) -> Result<Vec<u8>, Errors> {
    let frame = Frame::parse_frame(bytes.as_slice())?;
    frame.validate_request_frame()?;
    let parser = ParserFactory::get_parser(frame.opcode)?;
    let mut executable = parser.parse(frame.body.as_slice())?;
    let frame = executable.execute(frame)?;
    Ok(frame.to_bytes())
}
