use node::client_handler::ClientHandler;
use node::gossip::gossip_emitter::GossipEmitter;
use node::hinted_handoff::hints_receiver::HintsReceiver;
use node::hinted_handoff::hints_sender::HintsSender;
use node::node_initializer::NodeInitializer;
use node::utils::constants::{MAX_CLIENTS, NODES_METADATA_PATH};
use node::utils::errors::Errors;
use node::utils::functions::use_node_meta_data;
use node::utils::node_ip::NodeIp;
use std::net::{TcpListener, TcpStream};
use std::sync::{mpsc, Arc, Mutex};
use std::{env, thread};
use std::thread::sleep;
use std::time::Duration;

fn main() {
    let (uses_congig, config_file) = get_args();
    let node_data = NodeInitializer::new(uses_congig, config_file).unwrap();

    let needs_booting = node_data.set_cluster().unwrap();

    node_data.start_listeners();

    if needs_booting {
        HintsReceiver::start_listening(node_data.get_network_ip())
            .expect("Error starting Hints listener");
    };

    start_gossip().expect("Error starting gossip");

    set_node_listener(node_data.get_ip());
}

fn get_args() -> (bool, String) {
    let args: Vec<String> = env::args().collect();
    match args.len() {
        2 => {
            let first_arg = &args[1];
            match first_arg.as_str() {
                "config" => {
                    (true, String::new())
                }
                x => (true, x.to_string()),
            }
        }
        _ => (false, String::new())
    }
}

fn start_gossip() -> Result<(), Errors> {
    thread::spawn(move || -> Result<(), Errors> {
        loop {
            sleep(Duration::from_secs(1));
            GossipEmitter::start_gossip()?;
            {
                use_node_meta_data(|handler| {
                    for ip in handler.get_booting_nodes(NODES_METADATA_PATH)? {
                        HintsSender::send_hints(ip)?;
                    }
                    Ok(())
                })?
            }
        }
    });
    Ok(())
}

fn set_node_listener(ip: NodeIp) {
    let listener = TcpListener::bind(ip.get_std_socket()).expect("Error binding socket");
    println!("Server listening on {}", ip.get_string_ip());

    let (tx, rx) = mpsc::channel();
    let rx = Arc::new(Mutex::new(rx));

    start_thread_pool(Arc::clone(&rx));

    accept_connections(listener, tx);
}

fn accept_connections(listener: TcpListener, tx: mpsc::Sender<TcpStream>) {
    for incoming in listener.incoming() {
        match incoming {
            Ok(stream) => {
                println!("Client connected: {:?}", stream.peer_addr());
                if let Err(e) = tx.send(stream) {
                    println!("Error sending stream to thread pool: {}", e);
                }
            }
            Err(e) => {
                println!("Error accepting connection: {}", e);
            }
        }
    }
}

fn start_thread_pool(rx: Arc<Mutex<mpsc::Receiver<TcpStream>>>) {
    for _ in 0..MAX_CLIENTS {
        let rx = Arc::clone(&rx);
        thread::spawn(move || {
            loop {
                let stream = {
                    let lock = rx.lock().unwrap();
                    lock.recv()
                };

                match stream {
                    Ok(stream) => {
                        if let Err(e) = ClientHandler::handle_client(stream) {
                            println!("Error handling client: {}", e);
                        }
                    }
                    _ => break,
                }
            }
        });
    }
}
