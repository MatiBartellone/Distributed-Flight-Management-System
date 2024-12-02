use crate::utils::constants::{
    DATA_ACCESS_PORT_MOD, GOSSIP_MOD, HINTS_RECEIVER_MOD, META_DATA_ACCESS_MOD,
    QUERY_DELEGATION_PORT_MOD, SEED_LISTENER_MOD,
};
use crate::utils::errors::Errors;
use crate::utils::errors::Errors::ServerError;
use serde::{Deserialize, Serialize};
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;

#[derive(Debug, PartialEq, Serialize, Deserialize, Hash, Eq, Clone)]
pub struct NodeIp {
    ip: IpAddr,
    port: u16,
}

impl NodeIp {
    pub fn new(ip: IpAddr, port: u16) -> NodeIp {
        NodeIp { ip, port }
    }

    pub fn new_from_string(ip_string: &str, port: u16) -> Result<NodeIp, Errors> {
        Ok(NodeIp {
            ip: IpAddr::from_str(ip_string)
                .map_err(|_| ServerError(String::from("Could not parse ip")))?,
            port,
        })
    }

    pub fn new_from_single_string(ip_string: &str) -> Result<NodeIp, Errors> {
        // Intentamos separar la cadena por el carácter ':' (usamos `split_once` para eso)
        if let Some((ip_str, port_str)) = ip_string.split_once(":") {
            let ip = IpAddr::from_str(ip_str)
                .map_err(|_| ServerError("Could not parse IP".to_string()))?;
            let port = port_str
                .parse::<u16>()
                .map_err(|_| ServerError("Could not parse port".to_string()))?;
            Ok(NodeIp { ip, port })
        } else {
            Err(ServerError("Invalid IP format".to_string()))
        }
    }

    pub fn new_from_ip(node_ip: &NodeIp) -> NodeIp {
        Self {
            ip: node_ip.ip,
            port: node_ip.port,
        }
    }

    pub fn get_port(&self) -> u16 {
        self.port
    }

    pub fn get_std_socket(&self) -> SocketAddr {
        SocketAddr::new(self.ip, self.port)
    }

    pub fn get_query_delegation_socket(&self) -> SocketAddr {
        SocketAddr::new(self.ip, self.port + QUERY_DELEGATION_PORT_MOD as u16)
    }

    pub fn get_data_access_socket(&self) -> SocketAddr {
        SocketAddr::new(self.ip, self.port + DATA_ACCESS_PORT_MOD as u16)
    }

    pub fn get_meta_data_access_socket(&self) -> SocketAddr {
        SocketAddr::new(self.ip, self.port + META_DATA_ACCESS_MOD as u16)
    }

    pub fn get_seed_listener_socket(&self) -> SocketAddr {
        SocketAddr::new(self.ip, self.port + SEED_LISTENER_MOD as u16)
    }

    pub fn get_gossip_socket(&self) -> SocketAddr {
        SocketAddr::new(self.ip, self.port + GOSSIP_MOD as u16)
    }

    pub fn get_hints_receiver_socket(&self) -> SocketAddr {
        SocketAddr::new(self.ip, self.port + HINTS_RECEIVER_MOD as u16)
    }

    pub fn get_string_ip(&self) -> String {
        format!("{}:{}", self.ip, self.port)
    }
}
