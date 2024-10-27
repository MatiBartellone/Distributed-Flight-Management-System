use node::data_access::data_access_handler::DataAccessHandler;
use node::utils::frame::Frame;
use node::meta_data::meta_data_handler::MetaDataHandler;
use node::meta_data::nodes::cluster::Cluster;
use node::meta_data::nodes::node::Node;
use node::meta_data::nodes::node_meta_data_acces::NodesMetaDataAccess;
use node::node_communication::query_receiver::QueryReceiver;
use node::parsers::parser_factory::ParserFactory;
use node::response_builders::error_builder::ErrorBuilder;
use node::utils::constants::nodes_meta_data_path;
use node::utils::errors::Errors;
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;

fn main() {
    let server_addr = "127.0.0.1:7878";
    let network = get_user_data("Will this be used across netowrk? [Y][N]: ");
    let (ip, uses_network)= match network.as_str() {
        "Y" => (get_user_data("Device's ip (e.g. tailscale): "), true),
        _ => (get_user_data("Node's ip: "), false)
    };
    let (ip, network_ip, server_addr) = match uses_network {
        true => ("0.0.0.0".to_string(), ip, format!("{}:{}",get_user_data("Server's decive ip: "),7878)),
        false => (ip.to_string(), ip, server_addr.to_string())
    };
    let port = get_user_data("Node's port ([port, port+4] are used): ");
    let position = get_user_data("Node's position in cluster: ").parse::<i32>().unwrap() as usize;

    let node = Node::new(network_ip.to_string(), port.to_string(), position);
    let mut server_stream = TcpStream::connect(server_addr).expect("Failed to connect to server");
    server_stream.write_all(serde_json::to_string(&node).unwrap().as_bytes()).unwrap();

    set_cluster(&mut server_stream, node);

    listen_incoming_new_nodes(ip.to_string(), port.to_string());

    start_listeners(&ip, &port);

    set_node_listener(ip.to_string(), port.to_string(), &mut server_stream);
}

fn add_node_to_cluster(node: Node) -> Result<(), Errors> {
    let mut stream = MetaDataHandler::establish_connection()?;
    MetaDataHandler::get_instance(&mut stream).unwrap().get_nodes_metadata_access()
        .append_new_node(nodes_meta_data_path().as_ref(), node)?;
    Ok(())
}

fn get_user_data(msg: &str) -> String {
    print!("{}", msg);
    io::stdout().flush().unwrap();
    let mut data = String::new();
    io::stdin().read_line(&mut data).expect("Error reading data");
    data.trim().to_string()
}

fn start_listeners(ip: &String, port: &String) {
    let mtdt_ip = ip.to_string();
    let dtas_ip = ip.to_string();
    let qyrv_ip = ip.to_string();
    let mtdt_port = port.to_string();
    let dtas_port = port.to_string();
    let qyrv_port = port.to_string();
    thread::spawn(move || {
        MetaDataHandler::start_listening(mtdt_ip, mtdt_port).unwrap();
    });
    thread::spawn(move || {
        DataAccessHandler::start_listening(dtas_ip, dtas_port).unwrap();
    });
    thread::spawn(move || {
        QueryReceiver::start_listening(qyrv_ip, qyrv_port).unwrap();
    });
}

fn set_cluster(server_stream: &mut TcpStream, node: Node) {
    // Leer la lista de nodos activos del servidor
    let mut buffer = [0; 1024];
    let size = server_stream.read(&mut buffer).unwrap();
    let nodes: Vec<Node> = serde_json::from_slice(&buffer[..size]).unwrap();
    let cluster = Cluster::new(node, nodes);
    if let Err(e) = NodesMetaDataAccess::write_cluster(nodes_meta_data_path().as_ref(), &cluster) {
        println!("{}", e);
    }
}

fn listen_incoming_new_nodes(ip: String, port: String) {
    thread::spawn(move || {
        // Mantener el nodo corriendo y escuchando nuevos mensajes
        let port = (port.parse::<i32>().unwrap() + 4).to_string();
        let listener = TcpListener::bind(format!("{}:{}", ip, port)).unwrap();
        for mut stream in listener.incoming().flatten(){
            // Leer nuevos mensajes del servidor (nuevos nodos que se conectan)
            let mut buffer = [0; 1024];
            let size = stream.read(&mut buffer).unwrap();
            if size > 0{
                let node: Node = serde_json::from_slice(&buffer[..size]).unwrap();
                {
                    add_node_to_cluster(node).unwrap();
                }
            }

        }
    });
}

fn set_node_listener(ip: String, port: String, server_stream: &mut TcpStream) {
    let listener =
        TcpListener::bind(format!("{}:{}", ip, port)).expect("Error binding socket");
    println!("Servidor escuchando en {}:{}", ip, port);
    for incoming in listener.incoming() {
        match incoming {
            Ok(stream) => {
                println!("Cliente conectado: {:?}", stream.peer_addr());
                server_stream.write_all(b"1").unwrap();
                let mut value = server_stream.try_clone().unwrap();
                thread::spawn(move || {
                    handle_client(stream);
                    value.write_all(b"__").unwrap();
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
        stream.flush().unwrap();
        match stream.read(&mut buffer) {
            Ok(0) => {
                println!("Cliente desconectado");
                break;
            }
            Ok(n) => match execute_request(buffer[0..n].to_vec()) {
                Ok(response) => {
                    stream.flush().unwrap();
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
                    stream.flush().unwrap();
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
