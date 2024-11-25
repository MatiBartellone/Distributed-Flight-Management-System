use crate::data_access::data_access_handler::DataAccessHandler;
use crate::gossip::gossip_listener::GossipListener;
use crate::gossip::seed_listener::SeedListener;
use crate::meta_data::meta_data_handler::MetaDataHandler;
use crate::meta_data::nodes::cluster::Cluster;
use crate::meta_data::nodes::node::Node;
use crate::meta_data::nodes::node_meta_data_acces::NodesMetaDataAccess;
use crate::query_delegation::query_receiver::QueryReceiver;
use crate::utils::constants::{CONFIG_FILE, IP_FILE, NODES_METADATA_PATH};
use crate::utils::errors::Errors;
use crate::utils::errors::Errors::ServerError;
use crate::utils::functions::{
    connect_to_socket, deserialize_from_slice, read_exact_from_stream, serialize_to_string,
    write_to_stream,
};
use crate::utils::node_ip::NodeIp;
use serde::Deserialize;
use std::fs::File;
use std::io::Write;
use std::{fs, io, thread};

pub struct NodeInitializer {
    pub ip: NodeIp,
    pub network_ip: NodeIp,
    pub seed_ip: NodeIp,
    pub node: Node,
    pub is_first: bool,
    pub is_seed: bool,
}

#[derive(Deserialize)]
struct Config {
    ip: NodeIp,
    network_ip: NodeIp,
    seed_ip: NodeIp,
    uses_network: bool,
    is_seed: bool,
    is_first: bool,
}

impl NodeInitializer {
    pub fn new(uses_congig: bool, config_file: String) -> Result<Self, Errors> {
        match uses_congig {
            false => Self::get_data_by_user(),
            true => match config_file.as_str() {
                "default" => Self::read_config_file(CONFIG_FILE),
                file => Self::read_config_file(file),
            },
        }
    }

    fn get_data_by_user() -> Result<Self, Errors> {
        let (ip, uses_network) =
            match get_user_data("Will this be used across network? [Y][N]: ").as_str() {
                "Y" => (get_user_data("Device's ip (e.g. tail scale): "), true),
                _ => (get_user_data("Node's ip: "), false),
            };
        let (ip, network_ip) = match uses_network {
            true => ("0.0.0.0".to_string(), ip),
            false => (ip.to_string(), ip),
        };
        let port = get_user_data("Node's port ([port, port+5] are used): ");
        let port = port.parse::<u16>().expect("Could not parse port");

        let (seed_ip, seed_port, is_first) =
            match get_user_data("Is this the first node? [Y][N]: ").as_str() {
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

        let node_ip = NodeIp::new_from_string(network_ip.as_str(), port)?;
        store_ip(&NodeIp::new_from_string(ip.as_str(), port)?)?;

        Ok(Self {
            ip: NodeIp::new_from_string(ip.as_str(), port)?,
            network_ip: NodeIp::new_from_string(network_ip.as_str(), port)?,
            seed_ip: NodeIp::new_from_string(seed_ip.as_str(), seed_port)?,
            node: Node::new(&node_ip, 1, is_seed).expect("Error creating node"),
            is_first,
            is_seed,
        })
    }

    fn read_config_file(path: &str) -> Result<Self, Errors> {
        let contents = fs::read_to_string(path)
            .map_err(|_| ServerError(String::from("Could not read config file")))?;
        let mut config: Config = serde_yaml::from_str(&contents)
            .map_err(|_| ServerError(String::from("Could not deserialize config info")))?;
        if config.uses_network {
            config.ip = NodeIp::new_from_string("0.0.0.0", config.ip.get_port())?;
        } else {
            config.network_ip = NodeIp::new_from_ip(&config.ip);
        }
        if config.is_first {
            config.is_seed = true;
        }
        if config.is_seed {
            config.seed_ip = NodeIp::new_from_ip(&config.ip);
        }
        store_ip(&NodeIp::new_from_ip(&config.ip))?;
        Ok(Self {
            node: Node::new(&config.network_ip, 1, config.is_seed).expect("Error creating node"),
            ip: config.ip,
            network_ip: config.network_ip,
            seed_ip: config.seed_ip,
            is_first: config.is_first,
            is_seed: config.is_seed,
        })
    }

    pub fn get_ip(&self) -> NodeIp {
        NodeIp::new_from_ip(&self.ip)
    }

    pub fn get_network_ip(&self) -> NodeIp {
        NodeIp::new_from_ip(&self.network_ip)
    }

    pub fn get_seed_ip(&self) -> NodeIp {
        NodeIp::new_from_ip(&self.seed_ip)
    }

    pub fn get_node(&self) -> Node {
        Node::new_from_node(&self.node)
    }

    pub fn is_first(&self) -> bool {
        self.is_first
    }

    pub fn is_seed(&self) -> bool {
        self.is_seed
    }

    pub fn start_listeners(&self) {
        let metadata_ip = self.get_ip();
        let data_access_ip = self.get_ip();
        let query_receiver_ip = self.get_network_ip();
        let gossip_ip = self.get_network_ip();
        thread::spawn(move || {
            MetaDataHandler::start_listening(metadata_ip)
                .expect("Failed to start metadata listener");
        });
        thread::spawn(move || {
            DataAccessHandler::start_listening(data_access_ip)
                .expect("Failed to start data access");
        });
        thread::spawn(move || {
            QueryReceiver::start_listening(query_receiver_ip)
                .expect("Failed to start query receiver");
        });
        thread::spawn(move || {
            GossipListener::start_listening(gossip_ip).expect("Failed to start gossip listener");
        });
        if self.is_seed {
            let seed_ip = NodeIp::new_from_ip(&self.get_network_ip());
            thread::spawn(move || SeedListener::start_listening(seed_ip));
        }
    }

    pub fn set_cluster(&self) -> Result<bool, Errors> {
        let mut nodes = Vec::<Node>::new();
        let mut node = self.get_node();
        let mut needs_booting = false;
        if !self.is_first {
            let mut stream = connect_to_socket(self.get_seed_ip().get_seed_listener_socket())?;
            nodes = deserialize_from_slice(read_exact_from_stream(&mut stream)?.as_slice())?;
            needs_booting = set_node_pos(&mut node, &nodes);
            if needs_booting {
                nodes = eliminate_node_by_ip(&nodes, node.get_ip())
            }
            write_to_stream(&mut stream, serialize_to_string(&node)?.as_bytes())?
        }
        let cluster = Cluster::new(Node::new_from_node(&node), nodes);
        if let Err(e) = NodesMetaDataAccess::write_cluster(NODES_METADATA_PATH, &cluster) {
            println!("{}", e);
        }
        Ok(needs_booting)
    }
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

fn store_ip(ip: &NodeIp) -> Result<(), Errors> {
    let mut file = File::create(IP_FILE).expect("Error creating file");
    file.write_all(ip.get_string_ip().as_bytes())
        .expect("Error writing to file");
    Ok(())
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

fn eliminate_node_by_ip(nodes: &Vec<Node>, ip: &NodeIp) -> Vec<Node> {
    let mut new_nodes = Vec::<Node>::new();
    for node in nodes {
        if &node.ip != ip {
            new_nodes.push(Node::new_from_node(node));
        }
    }
    new_nodes
}
