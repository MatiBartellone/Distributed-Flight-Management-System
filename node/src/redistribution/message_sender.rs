use crate::{data_access::{data_access_handler::use_data_access, row::Row}, utils::{errors::Errors, constants::{DATA_ACCESS_PATH, NODES_METADATA_PATH}, types::node_ip}, queries::{insert_query, delete_query}, query_delegation::query_delegator::QueryDelegator, meta_data::meta_data_handler::{use_keyspace_meta_data, use_node_meta_data}};
use std::fs;

use super::builder_message::BuilderMessage;

pub struct MessageSender;

impl MessageSender {
    pub fn redistribute() -> Result<(), Errors> {
        if std::path::Path::new(DATA_ACCESS_PATH).exists() {
            for entry in fs::read_dir(DATA_ACCESS_PATH)
                .map_err(|_| Errors::ServerError("Failed to read directory".to_string()))?
            {
                let entry = entry.map_err(|_| Errors::ServerError("Failed to open table file".to_string()))?;
                let file_name = entry.file_name();
                let file_name_str = file_name.to_string_lossy();
                let path = format!("{}{}", DATA_ACCESS_PATH, file_name_str);
                
                if let Some(keyspace_table) = file_name_str.strip_suffix(".json") {
                    let rows = use_data_access(|data_access| {
                        data_access.get_deserialized_stream(&path)
                    })?;
                    MessageSender::redistribute_table(rows, keyspace_table)?;
                }
            }
        }
    
        Ok(())
    }

    fn redistribute_table<I>(rows: I, table: &str) -> Result<(), Errors>
    where
        I: Iterator<Item = Row>,
    {
        let keyspace = get_keyspace(&table);
        let own_node = use_node_meta_data(|handler| handler.get_own_ip(NODES_METADATA_PATH))?;
        for row in rows {
            let nodes_list = use_node_meta_data(|handler| handler.get_partition_full_ips(NODES_METADATA_PATH, &Some(row.primary_key.to_vec()), keyspace.to_owned()))?;
            if !nodes_list.contains(&own_node) {
                for node_ip in nodes_list {
                    let insert_query = BuilderMessage::build_insert(row.clone(), table.to_string())?;
                    QueryDelegator::send_to_node(node_ip, insert_query)?;
                }
                let delete_query = BuilderMessage::build_delete(row, table.to_owned())?;
                QueryDelegator::send_to_node(own_node.clone(), delete_query)?;
            }   
        }
        Ok(())
    }
}

fn get_keyspace(word: &str) -> &str {
    word.split('.').next().unwrap_or(word)
}