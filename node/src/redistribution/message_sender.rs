use crate::{data_access::{data_access_handler::use_data_access, row::Row}, utils::{errors::Errors, constants::DATA_ACCESS_PATH}};
use std::fs;

pub struct MessageSender;

impl MessageSender {
    fn redistribute() -> Result<(), Errors> {
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
                    MessageSender::redistribute_table(rows, keyspace_table, &path)?;
                }
            }
        }
    
        Ok(())
    }

    fn redistribute_table<I>(rows: I, table: &str, path: &String) -> Result<(), Errors>
    where
        I: Iterator<Item = Row>,
    {
        for row in rows {
            
        }
        Ok(())
    }
}