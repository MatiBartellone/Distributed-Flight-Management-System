use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use crate::utils::constants::{DATA_ACCESS_PORT_MOD, META_DATA_ACCESS_MOD, QUERY_DELEGATION_PORT_MOD, SEED_LISTENER_MOD};
use crate::utils::errors::Errors;
use crate::utils::errors::Errors::ServerError;

pub struct Ip{
    ip: IpAddr,
    port: u16,
}

impl Ip {
    pub fn new(ip: IpAddr, port: u16) -> Ip {
        Ip{ip, port}
    }

    pub fn new_from_string(ip_string: &str, port: u16) -> Result<Ip, Errors> {
        Ok(Ip{ip: IpAddr::from_str(ip_string).map_err(|_| ServerError(String::from("Could not parse ip")))?, port})
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

    pub fn get_string_ip(&self) -> String {
        format!("{}:{}", self.ip.to_string(), self.port)
    }
}