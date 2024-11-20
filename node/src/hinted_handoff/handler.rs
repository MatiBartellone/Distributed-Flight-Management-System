use crate::hinted_handoff::stored_query::StoredQuery;
use crate::utils::constants::HINTED_HANDOFF_DATA;
use crate::utils::errors::Errors;
use crate::utils::errors::Errors::ServerError;
use crate::utils::node_ip::NodeIp;
use std::fs;
use std::fs::{rename, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;

pub struct Handler;

impl Handler {
    pub fn store_query(query: StoredQuery, ip: NodeIp) -> Result<(), Errors> {
        fs::create_dir_all(HINTED_HANDOFF_DATA).map_err(|e| ServerError(e.to_string()))?;
        let path = format!("{}/{}.txt", HINTED_HANDOFF_DATA, ip.get_string_ip());
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|_| ServerError(String::from("failed to open file")))?;

        let line = format!(
            "{}\n",
            serde_json::to_string(&query)
                .map_err(|_| ServerError(String::from("Failed to serialize query")))?
        );
        file.write_all(line.as_bytes())
            .map_err(|_| ServerError(String::from("Failed to write to file")))?;
        Ok(())
    }

    pub fn check_for_perished(&self) -> Result<(), Errors> {
        for entry in fs::read_dir(HINTED_HANDOFF_DATA)
            .map_err(|_| ServerError(String::from("cannot read directory")))?
        {
            let entry = entry.map_err(|_| ServerError(String::from("cannot read directory")))?;
            let path = entry.path();
            let file =
                File::open(&path).map_err(|_| ServerError(String::from("cannot open file")))?;
            let mut reader = BufReader::new(file);
            let mut first_line = String::new();
            if reader
                .read_line(&mut first_line)
                .map_err(|_| ServerError(String::from("cannot open file")))?
                > 0
            {
                let stored_query: StoredQuery =
                    serde_json::from_slice(first_line.trim().as_bytes())
                        .map_err(|_| ServerError(String::from("invalid query")))?;
                if stored_query.has_perished() {
                    Self::eliminate_perished(path)?
                }
            }
        }
        Ok(())
    }

    fn eliminate_perished(path: PathBuf) -> Result<(), Errors> {
        let file = File::open(&path).map_err(|_| ServerError(String::from("cannot open file")))?;
        let reader = BufReader::new(file);

        let mut temp_path = path.clone();
        temp_path.set_extension("tmp");
        let mut temp_file =
            File::create(&temp_path).map_err(|_| ServerError(String::from("cannot open file")))?;

        for line in reader.lines().map_while(Result::ok) {
            let stored_query: StoredQuery = serde_json::from_slice(line.trim().as_bytes())
                .map_err(|_| ServerError(String::from("invalid query")))?;
            if !stored_query.has_perished() {
                temp_file
                    .write_all(line.as_bytes())
                    .map_err(|_| ServerError(String::from("invalid query")))?;
            }
        }
        rename(temp_path, path).map_err(|_| ServerError(String::from("cannot rename file")))?;
        Ok(())
    }
}
