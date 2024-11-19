use node::data_access::data_access_handler::DataAccessHandler;
use node::gossip::gossip_emitter::GossipEmitter;
use node::gossip::gossip_listener::GossipListener;
use node::gossip::seed_listener::SeedListener;
use node::hinted_handoff::hints_receiver::HintsReceiver;
use node::hinted_handoff::hints_sender::HintsSender;
use node::meta_data::meta_data_handler::MetaDataHandler;
use node::meta_data::nodes::cluster::Cluster;
use node::meta_data::nodes::node::Node;
use node::meta_data::nodes::node_meta_data_acces::NodesMetaDataAccess;
use node::parsers::parser_factory::ParserFactory;
use node::query_delegation::query_receiver::QueryReceiver;
use node::response_builders::error_builder::ErrorBuilder;
use node::utils::constants::{nodes_meta_data_path, NODES_METADATA};
use node::utils::errors::Errors;
use node::utils::frame::Frame;
use node::utils::node_ip::NodeIp;
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;
use std::thread::sleep;
use std::time::Duration;

fn main() {
    let (ip, uses_network) =
        match get_user_data("Will this be used across netowrk? [Y][N]: ").as_str() {
            "Y" => (get_user_data("Device's ip (e.g. tailscale): "), true),
            _ => (get_user_data("Node's ip: "), false),
        };
    let (ip, network_ip) = match uses_network {
        true => ("0.0.0.0".to_string(), ip),
        false => (ip.to_string(), ip),
    };
    let port = get_user_data("Node's port ([port, port+5] are used): ");
    let port = port.parse::<u16>().expect("Could not parse port");

    let (seed_ip, seed_port, is_first) =
        match get_user_data("Is this the fisrst node? [Y][N]: ").as_str() {
            "Y" => (ip.to_string(), port.to_string(), true),
            _ => (
                get_user_data("Seed node ip: "),
                get_user_data("Seed node port: "),
                false,
            ),
        };
    let seed_port = seed_port.parse::<u16>().expect("Could not parse port");

    let is_seed = match is_first {
        true => true,
        _ => matches!(get_user_data("Is this a seed node? [Y][N]: ").as_str(), "Y"),
    };

    let network_ip = NodeIp::new_from_string(network_ip.as_str(), port).unwrap();
    let ip = NodeIp::new_from_string(ip.as_str(), port).unwrap();
    let seed_ip = NodeIp::new_from_string(seed_ip.as_str(), seed_port).unwrap();
    let mut node = Node::new(&network_ip, 1, is_seed).expect("Error creating node");

    let needs_booting = set_cluster(&mut node, seed_ip, is_first);

    start_listeners(&ip, is_seed);

    if needs_booting {
        HintsReceiver::start_listening(network_ip).expect("Error starting Hints listener");
    };

    start_gossip().expect("Error starting gossip");

    set_node_listener(ip);
}

fn get_user_data(msg: &str) -> String {
    print!("{}", msg);
    io::stdout().flush().expect("Failed to flush stdout");
    let mut data = String::new();
    io::stdin()
        .read_line(&mut data)
        .expect("Error reading data");
    data.trim().to_string()
}

fn start_listeners(ip: &NodeIp, is_seed: bool) {
    let meta_data_ip = NodeIp::new_from_ip(ip);
    let data_access_ip = NodeIp::new_from_ip(ip);
    let query_receiver_ip = NodeIp::new_from_ip(ip);
    let gossip_listener_ip = NodeIp::new_from_ip(ip);
    thread::spawn(move || {
        MetaDataHandler::start_listening(meta_data_ip).expect("Failed to start metadata listener");
    });
    thread::spawn(move || {
        DataAccessHandler::start_listening(data_access_ip).expect("Failed to start data access");
    });
    thread::spawn(move || {
        QueryReceiver::start_listening(query_receiver_ip).expect("Failed to start query receiver");
    });
    thread::spawn(move || {
        GossipListener::start_listening(gossip_listener_ip)
            .expect("Failed to start gossip listener");
    });
    if is_seed {
        let seed_ip = NodeIp::new_from_ip(ip);
        thread::spawn(move || SeedListener::start_listening(seed_ip));
    }
}

fn start_gossip() -> Result<(), Errors> {
    thread::spawn(move || -> Result<(), Errors> {
        loop {
            sleep(Duration::from_secs(1));
            GossipEmitter::start_gossip()?;
            {
                let mut stream = MetaDataHandler::establish_connection()?;
                let node_metadata =
                    MetaDataHandler::get_instance(&mut stream)?.get_nodes_metadata_access();
                for ip in node_metadata.get_booting_nodes(NODES_METADATA)? {
                    HintsSender::send_hints(ip)?;
                }
            }
        }
    });
    Ok(())
}

fn set_cluster(node: &mut Node, seed_ip: NodeIp, is_first: bool) -> bool {
    let mut nodes = Vec::<Node>::new();
    let mut needs_booting = false;
    if !is_first {
        let mut stream = TcpStream::connect(seed_ip.get_seed_listener_socket())
            .expect("Error connecting to seed");
        let mut buffer = [0; 1024];
        let size = stream
            .read(&mut buffer)
            .expect("Failed to read from server stream");
        nodes = serde_json::from_slice(&buffer[..size]).expect("Failed to deserialize json");
        needs_booting = set_node_pos(node, &nodes);
        stream
            .write_all(serde_json::to_string(&node).expect("").as_bytes())
            .expect("Error writing to seed");
    }
    let cluster = Cluster::new(Node::new_from_node(node), nodes);
    if let Err(e) = NodesMetaDataAccess::write_cluster(nodes_meta_data_path().as_ref(), &cluster) {
        println!("{}", e);
    }
    needs_booting
}

fn set_node_pos(node: &mut Node, nodes: &Vec<Node>) -> bool {
    let mut higher_position = 1;
    for received_node in nodes {
        if received_node.get_ip() == node.get_ip() {
            node.position = received_node.get_pos();
            node.set_booting();
            return true;
        }
        if received_node.get_pos() > higher_position {
            higher_position = received_node.get_pos();
        }
    }
    node.position = higher_position + 1;
    false
}

fn set_node_listener(ip: NodeIp) {
    let listener = TcpListener::bind(ip.get_std_socket()).expect("Error binding socket");
    println!("Servidor escuchando en {}", ip.get_string_ip());
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
