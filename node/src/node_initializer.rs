use std::fs::File;
use std::{io, thread};
use std::io::{Read, Write};
use std::net::TcpStream;
use crate::data_access::data_access_handler::DataAccessHandler;
use crate::gossip::gossip_listener::GossipListener;
use crate::gossip::seed_listener::SeedListener;
use crate::meta_data::meta_data_handler::MetaDataHandler;
use crate::meta_data::nodes::cluster::Cluster;
use crate::meta_data::nodes::node::Node;
use crate::meta_data::nodes::node_meta_data_acces::NodesMetaDataAccess;
use crate::query_delegation::query_receiver::QueryReceiver;
use crate::utils::constants::{nodes_meta_data_path, IP_FILE};
use crate::utils::errors::Errors;
use crate::utils::node_ip::NodeIp;

pub struct NodeInitializer {
    pub ip: NodeIp,
    pub network_ip: NodeIp,
    pub seed_ip: NodeIp,
    pub node: Node,
    pub is_first: bool,
    pub is_seed: bool,
}

impl NodeInitializer {
    pub fn new() -> Result<Self, Errors> {
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
            MetaDataHandler::start_listening(metadata_ip).expect("Failed to start metadata listener");
        });
        thread::spawn(move || {
            DataAccessHandler::start_listening(data_access_ip).expect("Failed to start data access");
        });
        thread::spawn(move || {
            QueryReceiver::start_listening(query_receiver_ip).expect("Failed to start query receiver");
        });
        thread::spawn(move || {
            GossipListener::start_listening(gossip_ip)
                .expect("Failed to start gossip listener");
        });
        if self.is_seed {
            let seed_ip = NodeIp::new_from_ip(&self.get_network_ip());
            thread::spawn(move || SeedListener::start_listening(seed_ip));
        }
    }

    pub fn set_cluster(&self) -> bool {
        let mut nodes = Vec::<Node>::new();
        let mut node = self.get_node();
        let mut needs_booting = false;
        if !self.is_first {
            let mut stream = TcpStream::connect(self.get_seed_ip().get_seed_listener_socket())
                .expect("Error connecting to seed");
            let mut buffer = [0; 1024];
            let size = stream
                .read(&mut buffer)
                .expect("Failed to read from server stream");
            nodes = serde_json::from_slice(&buffer[..size]).expect("Failed to deserialize json");
            needs_booting = set_node_pos(&mut node, &nodes);
            if needs_booting {
                nodes = eliminate_node_by_ip(&nodes, node.get_ip())
            }
            stream
                .write_all(serde_json::to_string(&node).expect("").as_bytes())
                .expect("Error writing to seed");
        }
        let cluster = Cluster::new(Node::new_from_node(&node), nodes);
        if let Err(e) = NodesMetaDataAccess::write_cluster(nodes_meta_data_path().as_ref(), &cluster) {
            println!("{}", e);
        }
        needs_booting
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