use crate::{data_access::{data_access_handler::use_data_access, row::Row}, utils::{errors::Errors, constants::{DATA_ACCESS_PATH, NODES_METADATA_PATH, KEYSPACE_METADATA_PATH}, types::node_ip::NodeIp}, query_delegation::query_delegator::QueryDelegator, meta_data::meta_data_handler::{use_keyspace_meta_data, use_node_meta_data}, queries::drop_keyspace_query};
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

    pub fn send_meta_data(new_node: NodeIp) -> Result<(), Errors> {
        let keyspaces = use_keyspace_meta_data(|handler| {
            handler.get_keyspaces_names(KEYSPACE_METADATA_PATH.to_owned())
        })?;
        for keyspace in keyspaces {
            let create_keyspace_query = BuilderMessage::build_keyspace(keyspace.to_string())?;
            QueryDelegator::send_to_node(new_node.clone(), create_keyspace_query)?;
            let tables = use_keyspace_meta_data(|handler| {
                handler.get_tables_from_keyspace(KEYSPACE_METADATA_PATH.to_owned(), &keyspace)
            })?;
            for table in tables {
                let path = format!("{}.{}", keyspace, table);
                let create_table_query = BuilderMessage::build_table(path)?;
                QueryDelegator::send_to_node(new_node.clone(), create_table_query)?;
            }
        }
        Ok(())
    }

    pub fn send_drop_keyspace() -> Result<(), Errors> {
        let keyspaces = use_keyspace_meta_data(|handler| {
            handler.get_keyspaces_names(KEYSPACE_METADATA_PATH.to_owned())
        })?;
        for keyspace in keyspaces {
            let drop_keyspace_query = BuilderMessage::build_drop(keyspace)?;
            let _ = drop_keyspace_query.run();
        }
        Ok(())
    }


    fn redistribute_table<I>(rows: I, table: &str) -> Result<(), Errors>
    where
        I: Iterator<Item = Row>,
    {
        let keyspace = get_keyspace(table);
        let own_node = use_node_meta_data(|handler| handler.get_own_ip(NODES_METADATA_PATH))?;
        for row in rows {
            let nodes_list = use_node_meta_data(|handler| handler.get_partition_full_ips(NODES_METADATA_PATH, &Some(row.primary_key.to_vec()), keyspace.to_owned()))?;
            if !nodes_list.contains(&own_node) {
                for node_ip in nodes_list {
                    let insert_query = BuilderMessage::build_insert(row.clone(), table.to_string())?;
                    QueryDelegator::send_to_node(node_ip, insert_query)?;
                }
                let delete_query = BuilderMessage::build_delete(row, table.to_owned())?;
                let _ = delete_query.run();
            }   
        }
        Ok(())
    }
}

fn get_keyspace(word: &str) -> &str {
    word.split('.').next().unwrap_or(word)
}