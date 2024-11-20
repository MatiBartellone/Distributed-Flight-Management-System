use crate::meta_data::meta_data_handler::MetaDataHandler;
use crate::parsers::tokens::data_type::DataType;
use crate::queries::where_logic::where_clause::WhereClause;
use crate::utils::constants::{CLIENT_METADATA_PATH, IP_FILE, KEYSPACE_METADATA_PATH};
use crate::utils::errors::Errors;
use crate::utils::errors::Errors::ServerError;
use crate::utils::node_ip::NodeIp;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::time::{SystemTime, UNIX_EPOCH};

pub fn get_long_string_from_str(str: &str) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice((str.len() as u32).to_be_bytes().as_ref());
    bytes.extend_from_slice(str.as_bytes());
    bytes
}

pub fn get_timestamp() -> Result<u64, Errors> {
    if let Ok(timestamp) = SystemTime::now().duration_since(UNIX_EPOCH) {
        return Ok(timestamp.as_secs());
    }
    Err(Errors::ServerError(String::from("Time went backwards")))
}

pub fn generate_random_number(limit: u64, offset: u64) -> Result<u64, Errors> {
    Ok((get_timestamp()? % limit) + offset)
}

pub fn check_table_name(table_name: &String) -> Result<String, Errors> {
    let mut stream = MetaDataHandler::establish_connection()?;
    let client_meta_data =
        MetaDataHandler::get_instance(&mut stream)?.get_client_meta_data_access();
    if table_name.is_empty() {
        return Err(Errors::SyntaxError(String::from("Table is empty")));
    }
    if !table_name.contains('.')
        && client_meta_data
            .get_keyspace(CLIENT_METADATA_PATH.to_string())?
            .is_none()
    {
        return Err(Errors::SyntaxError(String::from(
            "Keyspace not defined and non keyspace in usage",
        )));
    } else if table_name.contains('.') {
        return Ok(table_name.to_string());
    };
    let Some(kp) = client_meta_data.get_keyspace(CLIENT_METADATA_PATH.to_string())? else {
        return Err(Errors::SyntaxError(String::from("Keyspace not in usage")));
    };
    println!("setting tablwe!!");
    Ok(format!("{}.{}", kp, table_name))
}

pub fn get_columns_from_table(table_name: &str) -> Result<HashMap<String, DataType>, Errors> {
    let mut stream = MetaDataHandler::establish_connection()?;
    let keyspace_meta_data =
        MetaDataHandler::get_instance(&mut stream)?.get_keyspace_meta_data_access();
    let binding = table_name.split('.').collect::<Vec<&str>>();
    let identifiers = &binding.as_slice();
    keyspace_meta_data.get_columns_type(
        KEYSPACE_METADATA_PATH.to_string(),
        identifiers[0],
        identifiers[1],
    )
}

pub fn get_table_pk(table_name: &str) -> Result<HashSet<String>, Errors> {
    let binding = table_name.split('.').collect::<Vec<&str>>();
    let identifiers = &binding.as_slice();
    let mut stream = MetaDataHandler::establish_connection()?;
    let keyspace_meta_data =
        MetaDataHandler::get_instance(&mut stream)?.get_keyspace_meta_data_access();
    Ok(keyspace_meta_data
        .get_primary_key(
            KEYSPACE_METADATA_PATH.to_string(),
            identifiers[0],
            identifiers[1],
        )?
        .get_full_pk_in_hash())
}

pub fn get_table_partition(table_name: &str) -> Result<HashSet<String>, Errors> {
    let binding = table_name.split('.').collect::<Vec<&str>>();
    let identifiers = &binding.as_slice();
    let mut stream = MetaDataHandler::establish_connection()?;
    let keyspace_meta_data =
        MetaDataHandler::get_instance(&mut stream)?.get_keyspace_meta_data_access();
    let pk = keyspace_meta_data.get_primary_key(
        KEYSPACE_METADATA_PATH.to_string(),
        identifiers[0],
        identifiers[1],
    )?;
    Ok(pk.partition_keys.into_iter().collect())
}
pub fn get_table_clustering_columns(table_name: &str) -> Result<HashSet<String>, Errors> {
    let binding = table_name.split('.').collect::<Vec<&str>>();
    let identifiers = &binding.as_slice();
    let mut stream = MetaDataHandler::establish_connection()?;
    let keyspace_meta_data =
        MetaDataHandler::get_instance(&mut stream)?.get_keyspace_meta_data_access();
    let pk = keyspace_meta_data.get_primary_key(
        KEYSPACE_METADATA_PATH.to_string(),
        identifiers[0],
        identifiers[1],
    )?;
    Ok(pk.clustering_columns.into_iter().collect())
}

pub fn split_keyspace_table(input: &str) -> Result<(&str, &str), Errors> {
    let mut parts = input.split('.');
    let keyspace = parts
        .next()
        .ok_or_else(|| Errors::SyntaxError("Missing keyspace".to_string()))?;
    let table = parts
        .next()
        .ok_or_else(|| Errors::SyntaxError("Missing table".to_string()))?;
    if parts.next().is_some() {
        return Err(Errors::SyntaxError(
            "Too many parts, expected only keyspace and table".to_string(),
        ));
    }
    Ok((keyspace, table))
}

pub fn get_int_from_string(string: &String) -> Result<i32, Errors> {
    string
        .parse::<i32>()
        .map_err(|_| Errors::SyntaxError(format!("Could not parse int: {}", string)))
}

pub fn get_partition_key_from_where(
    table_name: &str,
    where_clause: &Option<WhereClause>,
) -> Result<Vec<String>, Errors> {
    let Some(where_clause) = where_clause else {
        return Err(Errors::SyntaxError(String::from(
            "Where clause must be defined",
        )));
    };
    let mut partition_key = Vec::new();
    let table_partition = get_table_partition(table_name)?;
    where_clause.get_primary_key(&mut partition_key, &table_partition)?;
    if partition_key.len() != table_partition.len() {
        return Err(Errors::SyntaxError(String::from(
            "Full partition key must be defined in where clause",
        )));
    }
    Ok(partition_key)
}

pub fn get_own_ip() -> Result<NodeIp, Errors> {
    let content = fs::read_to_string(IP_FILE).map_err(|e| ServerError(e.to_string()))?; // Lee el contenido completo como un String
    let split = content.split(":").collect::<Vec<&str>>();
    let port = split[1].parse::<u16>().unwrap();
    NodeIp::new_from_string(split[0], port)
    //NodesMetaDataAccess::get_own_ip_(nodes_meta_data_path().as_ref())
}

pub fn start_listener<F>(socket: SocketAddr, handle_connection: F) -> Result<(), Errors>
where
    F: Fn(&mut TcpStream) -> Result<(), Errors>,
{
    let listener = TcpListener::bind(socket)
        .map_err(|_| Errors::ServerError(String::from("Failed to set listener")))?;
    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => handle_connection(&mut stream)?,
            Err(_) => {
                return Err(Errors::ServerError(String::from(
                    "Failed to connect to listener",
                )))
            }
        }
    }
    Ok(())
}
