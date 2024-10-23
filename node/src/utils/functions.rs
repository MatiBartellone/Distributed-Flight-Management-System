use crate::meta_data::meta_data_handler::MetaDataHandler;
use crate::meta_data::nodes::node_meta_data_acces::NodesMetaDataAccess;
use crate::utils::constants::{
    nodes_meta_data_path, CLIENT_METADATA_PATH, DATA_ACCESS_PORT, META_DATA_ACCESS_PORT,
};
use crate::utils::errors::Errors;
use std::time::{SystemTime, UNIX_EPOCH};

pub fn get_long_string_from_str(str: &str) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice((str.len() as u32).to_be_bytes().as_ref());
    bytes.extend_from_slice(str.as_bytes());
    bytes
}

pub fn get_timestamp() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        .to_string()
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
    Ok(format!("{}.{}", kp, table_name))
}

pub fn get_data_access_ip() -> Result<String, Errors> {
    Ok(format!(
        "{}:{}",
        NodesMetaDataAccess::get_own_ip_(nodes_meta_data_path().as_ref())?,
        DATA_ACCESS_PORT
    ))
}
pub fn get_meta_data_handler_ip() -> Result<String, Errors> {
    Ok(format!(
        "{}:{}",
        NodesMetaDataAccess::get_own_ip_(nodes_meta_data_path().as_ref())?,
        META_DATA_ACCESS_PORT
    ))
}
