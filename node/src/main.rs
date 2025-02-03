use node::client_handler::ClientHandler;
use node::gossip::gossip_emitter::GossipEmitter;
use node::hinted_handoff::handler::Handler;
use node::hinted_handoff::hints_receiver::HintsReceiver;
use node::hinted_handoff::hints_sender::HintsSender;
use node::logger::Logger;
use node::meta_data::meta_data_handler::use_node_meta_data;
use node::node_initializer::NodeInitializer;
use node::terminal_input::TerminalInput;
use node::utils::constants::{LOGGER_PATH, NODES_METADATA_PATH};
use node::utils::config_constants::{BOOTING_TIMEOUT_SECS, MAX_CLIENTS};
use node::utils::errors::Errors;
use node::utils::types::node_ip::NodeIp;
use node::utils::types::tls_stream::{create_server_config, get_stream_owned};
use rustls::ServerConfig;
use std::net::{TcpListener, TcpStream};
use std::sync::{mpsc, Arc, Mutex};
use std::thread::sleep;
use std::time::Duration;
use std::{env, thread};
use node::utils::functions::redistribute_data;

fn main() -> Result<(), Errors> {
    let (uses_config, config_file) = get_args();
    let node_data = NodeInitializer::new(uses_config, config_file)?;

    let (needs_recovering, needs_booting) = node_data.set_cluster()?;

    node_data.start_listeners();

    TerminalInput::new().start_listening();

    if needs_recovering {
        HintsReceiver::start_listening(node_data.get_ip())?;
    } else if needs_booting {
        sleep(Duration::from_secs(BOOTING_TIMEOUT_SECS));
        use_node_meta_data(|handler| handler.set_own_node_active(NODES_METADATA_PATH))?;
        let logger = Logger::new("server.log");
        logger.log_message("Booting Finished");
    }
    use_node_meta_data(|handler| handler.update_ranges(NODES_METADATA_PATH))?;
    start_gossip()?;

    set_node_listener(node_data.get_ip())
}

fn get_args() -> (bool, String) {
    let args: Vec<String> = env::args().collect();
    match args.len() {
        2 => {
            let first_arg = &args[1];
            match first_arg.as_str() {
                "config" => (true, String::new()),
                x => (true, x.to_string()),
            }
        }
        _ => (false, String::new()),
    }
}

fn start_gossip() -> Result<(), Errors> {
    thread::spawn(move || -> Result<(), Errors> {
        let logger = Logger::new(LOGGER_PATH);
        loop {
            let result = gossip();
            if let Err(e) = result {
                logger.log_error(format!("Failed to gossip: {}", e).as_str());
            }
        }
    });
    Ok(())
}

fn gossip() -> Result<(), Errors> {
    sleep(Duration::from_secs(1));
    let node_added_or_removed = GossipEmitter::start_gossip()?;
    if node_added_or_removed {
        redistribute_data()?
    }
    use_node_meta_data(|handler| handler.check_for_perished_shutting_down_nodes())?;
    Handler::check_for_perished()?;
    {
        use_node_meta_data(|handler| {
            for ip in handler.get_recovering_nodes(NODES_METADATA_PATH)? {
                HintsSender::send_hints(ip)?;
            }
            Ok(())
        })?
    }
    Ok(())
}

fn set_node_listener(ip: NodeIp) -> Result<(), Errors> {
    let listener = TcpListener::bind(ip.get_std_socket()).expect("Error binding socket");
    let logger = Logger::new(LOGGER_PATH);
    logger.log_message(format!("Server listening on {}", ip.get_string_ip()).as_str());

    let (tx, rx) = mpsc::channel();
    let rx = Arc::new(Mutex::new(rx));

    start_thread_pool(Arc::clone(&rx))?;

    accept_connections(listener, tx);
    Ok(())
}

fn accept_connections(listener: TcpListener, tx: mpsc::Sender<TcpStream>) {
    let logger = Logger::new(LOGGER_PATH);
    for incoming in listener.incoming() {
        match incoming {
            Ok(stream) => {
                logger.log_message(format!("Client connected: {:?}", stream.peer_addr()).as_str());
                if let Err(e) = tx.send(stream) {
                    logger.log_error(format!("Error sending stream to thread pool: {}", e).as_str());
                }
            }
            Err(e) => {
                logger.log_error(format!("Error accepting connection: {}", e).as_str());
            }
        }
    }
}

fn start_thread_pool(rx: Arc<Mutex<mpsc::Receiver<TcpStream>>>) -> Result<(), Errors> {
    let server_config = create_server_config()?;
    for _ in 0..MAX_CLIENTS {
        let rx = Arc::clone(&rx);
        let server_config = Arc::new(server_config.clone());
        thread::spawn(move || loop {
            let logger = Logger::new(LOGGER_PATH);
            let stream = {
                let lock = rx.lock().unwrap();
                lock.recv()
            };
            match stream {
                Ok(stream) => {
                    if let Err(e) = handle_client(stream, server_config.clone()) {
                        logger.log_error(format!("Error handling client: {}", e).as_str());
                    }
                }
                _ => break,
            }
        });
    }
    Ok(())
}

fn handle_client(stream: TcpStream, server_config: Arc<ServerConfig>) -> Result<(), Errors> {
    let tls_stream = get_stream_owned(stream, server_config)?;
    ClientHandler::handle_client(tls_stream)
}
