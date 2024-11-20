use node::client_handler::ClientHandler;
use node::gossip::gossip_emitter::GossipEmitter;
use node::hinted_handoff::hints_receiver::HintsReceiver;
use node::hinted_handoff::hints_sender::HintsSender;
use node::meta_data::meta_data_handler::MetaDataHandler;
use node::node_initializer::NodeInitializer;
use node::utils::constants::NODES_METADATA_PATH;
use node::utils::errors::Errors;
use node::utils::node_ip::NodeIp;
use std::net::TcpListener;
use std::thread;
use std::thread::sleep;
use std::time::Duration;

fn main() {
    let node_data = NodeInitializer::new().unwrap();

    let needs_booting = node_data.set_cluster();

    node_data.start_listeners();

    if needs_booting {
        HintsReceiver::start_listening(node_data.get_network_ip())
            .expect("Error starting Hints listener");
    };

    start_gossip().expect("Error starting gossip");

    set_node_listener(node_data.get_ip());
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
                for ip in node_metadata.get_booting_nodes(NODES_METADATA_PATH)? {
                    HintsSender::send_hints(ip)?;
                }
            }
        }
    });
    Ok(())
}

fn set_node_listener(ip: NodeIp) {
    let listener = TcpListener::bind(ip.get_std_socket()).expect("Error binding socket");
    println!("Server listening in {}", ip.get_string_ip());
    for incoming in listener.incoming() {
        match incoming {
            Ok(stream) => {
                println!("Client connected: {:?}", stream.peer_addr());
                thread::spawn(move || {
                    if let Err(e) = ClientHandler::handle_client(stream) {
                        println!("{}", e);
                    }
                });
            }
            Err(e) => {
                println!("Error accepting connection: {}", e);
            }
        }
    }
}
