use crate::meta_data::meta_data_handler::MetaDataHandler;
use crate::meta_data::nodes::node_meta_data_acces::NodesMetaDataAccess;
use crate::parsers::tokens::data_type::DataType;
use crate::utils::constants::{
    nodes_meta_data_path, CLIENT_METADATA_PATH, DATA_ACCESS_PORT, KEYSPACE_METADATA,
    META_DATA_ACCESS_PORT,
};
use crate::utils::errors::Errors;
use std::collections::{HashMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};
use crate::queries::where_logic::where_clause::WhereClause;

pub fn get_long_string_from_str(str: &str) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice((str.len() as u32).to_be_bytes().as_ref());
    bytes.extend_from_slice(str.as_bytes());
    bytes
}

pub fn get_timestamp() -> Result<String, Errors> {
    if let Ok(timestamp) = SystemTime::now().duration_since(UNIX_EPOCH) {
        return Ok(timestamp.as_secs().to_string());
    }
    Err(Errors::ServerError(String::from("Time went backwards")))
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

pub fn get_columns_from_table(table_name: &str) -> Result<HashMap<String, DataType>, Errors> {
    let mut stream = MetaDataHandler::establish_connection()?;
    let keyspace_meta_data =
        MetaDataHandler::get_instance(&mut stream)?.get_keyspace_meta_data_access();
    let binding = table_name.split('.').collect::<Vec<&str>>();
    let identifiers = &binding.as_slice();
    keyspace_meta_data.get_columns_type(
        KEYSPACE_METADATA.to_string(),
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
            KEYSPACE_METADATA.to_string(),
            identifiers[0],
            identifiers[1],
        )?
        .into_iter()
        .collect())
}

pub fn get_int_from_string(string: &String) -> Result<i32, Errors> {
    string
        .parse::<i32>()
        .map_err(|_| Errors::SyntaxError(format!("Could not parse int: {}", string)))
}

pub fn get_primary_key_from_where(table_name: &str, where_clause: &Option<WhereClause>) -> Result<Option<Vec<String>>, Errors> {
    let Some(where_clause) = where_clause else {
        return Err(Errors::SyntaxError(String::from(
            "Where clause must be defined",
        )));
    };
    let mut primary_key = Vec::new();
    let table_pk = get_table_pk(table_name)?;
    if where_clause.get_primary_key(&mut primary_key, &table_pk)? {
        if primary_key.len() != table_pk.len() {
            return Err(Errors::SyntaxError(String::from(
                "Full primary key must be defined in where clause",
            )));
        }
        Ok(Some(primary_key))
    } else {
        Ok(None)
    }
}