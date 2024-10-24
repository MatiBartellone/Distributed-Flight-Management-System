use node::data_access::data_access_handler::DataAccessHandler;
use node::frame::Frame;
use node::meta_data::meta_data_handler::MetaDataHandler;
use node::meta_data::nodes::cluster::Cluster;
use node::meta_data::nodes::node::Node;
use node::meta_data::nodes::node_meta_data_acces::NodesMetaDataAccess;
use node::node_communication::query_receiver::QueryReceiver;
use node::parsers::parser_factory::ParserFactory;
use node::response_builders::error_builder::ErrorBuilder;
use node::utils::constants::{nodes_meta_data_path, CLIENTS_PORT};
use node::utils::errors::Errors;
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;
use serde::{Deserialize, Serialize};

#[derive(Debug ,Serialize, Deserialize)]
struct NodeInfo{
    ip: String,
    position: usize
}

fn add_node_to_cluster(node: Node) -> Result<(), Errors> {
    let mut stream = MetaDataHandler::establish_connection()?;
    let _ = MetaDataHandler::get_instance(&mut stream).unwrap().get_nodes_metadata_access()
        .append_new_node(nodes_meta_data_path().as_ref(), node)?;
    Ok(())
}

fn main() {
    let server_addr = "127.0.0.1:7878";

    print!("node's ip: ");
    io::stdout().flush().unwrap();
    let mut ip = String::new();
    io::stdin().read_line(&mut ip).expect("Error reading ip");
    let ip = ip.trim();

    print!("node's position in cluster: ");
    io::stdout().flush().unwrap();
    let mut position = String::new();
    io::stdin().read_line(&mut position).expect("Error reading position");
    let position = position.trim();

    let node_info = NodeInfo{ip: ip.to_string(), position: position.parse::<i32>().unwrap() as usize};
    let mut server_stream = TcpStream::connect(server_addr).expect("Failed to connect to server");

    server_stream.write(serde_json::to_string(&node_info).unwrap().as_bytes()).unwrap();

    // Leer la lista de nodos activos del servidor
    let mut buffer = [0; 1024];
    let size = server_stream.read(&mut buffer).unwrap();
    let nodes: Vec<NodeInfo> = serde_json::from_slice(&buffer[..size]).unwrap();
    let nodes : Vec<Node> = nodes.iter().map(|n| Node::new(n.ip.to_string(), n.position)).collect();
    dbg!(&nodes);
    let cluster = Cluster::new(Node::new(ip.to_string(), node_info.position), nodes);
    if let Err(e) = NodesMetaDataAccess::write_cluster(nodes_meta_data_path().as_ref(), &cluster) {
        dbg!(e);
    }

    thread::spawn(move || {
        let _ = QueryReceiver::start_listening();
    });
    thread::spawn(move || {
        let _ = DataAccessHandler::start_listening();
    });
    thread::spawn(move || {
        let _ = MetaDataHandler::start_listening();
    });

    thread::spawn(move || {
        // Mantener el nodo corriendo y escuchando nuevos mensajes
        let listener = TcpListener::bind(format!("{}:{}", node_info.ip, 7676)).unwrap();
        for incoming in listener.incoming(){
            match incoming {
                Ok(mut stream) => {
                    // Leer nuevos mensajes del servidor (nuevos nodos que se conectan)
                    let size = stream.read(&mut buffer).unwrap();
                    if size > 0{
                        let new_node: NodeInfo = serde_json::from_slice(&buffer[..size]).unwrap();
                        dbg!(&new_node);
                        let node = Node::new(new_node.ip.to_string(), new_node.position);
                        {
                            add_node_to_cluster(node).unwrap();
                        }
                    }
                },
                _ => {}
            }


        }
    });








    let listener =
        TcpListener::bind(format!("{}:{}", ip, CLIENTS_PORT)).expect("Error binding socket");
    println!("Servidor escuchando en {}", ip);

    for incoming in listener.incoming() {
        match incoming {
            Ok(stream) => {
                println!("Cliente conectado: {:?}", stream.peer_addr());


                // Mover la conexión a un hilo
                server_stream.write(b"1").unwrap();
                thread::spawn(move || {
                    handle_client(stream);
                    //server_stream.write(b"-").unwrap();
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
    let mut executable = parser.parse(frame.body.as_slice())?;
    let frame = executable.execute(frame)?;

    Ok(frame.to_bytes())
}
