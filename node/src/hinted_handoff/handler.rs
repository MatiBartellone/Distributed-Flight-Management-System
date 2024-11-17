use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use crate::hinted_handoff::stored_query::StoredQuery;
use crate::utils::constants::HINTED_HANDOF_DATA;
use crate::utils::errors::Errors;
use crate::utils::errors::Errors::ServerError;
use crate::utils::ip::Ip;

pub struct Handler;

impl Handler {
    pub fn store_query(&self, query: StoredQuery, ip: Ip) -> Result<(), Errors> {
        let path = format!("{}{}.txt", HINTED_HANDOF_DATA ,ip.get_string_ip());
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path).map_err(|_| ServerError(String::from("failed to open file")))?;

        let line = format!("{}\n", serde_json::to_string(&query).map_err(|_| ServerError(String::from("Failed to serialize query")))?);
        file.write_all(line.as_bytes()).map_err(|_| ServerError(String::from("Failed to write to file")))?;
        Ok(())
    }

    pub fn check_for_perished(&self) -> Result<(), Errors> {
        for entry in fs::read_dir(HINTED_HANDOF_DATA).map_err(|_| ServerError(String::from("cannot read directory")))? {
            let entry = entry.map_err(|_| ServerError(String::from("cannot read directory")))?;
            let path = entry.path();
            let file = File::open(path).map_err(|_| ServerError(String::from("cannot open file")))?;
            let mut reader = BufReader::new(&file);
            let mut first_line = String::new();
            if reader.read_line(&mut first_line).map_err(|_| ServerError(String::from("cannot open file")))? > 0 {
                let stored_query : StoredQuery = serde_json::from_slice(first_line.trim().as_bytes()).map_err(|_| ServerError(String::from("invalid query")))?;
                if stored_query.has_perished() {
                    Self::eliminate_perished(file)?
                }
            }
        }
        Ok(())
    }

    fn eliminate_perished(file: File) -> Result<(), Errors> {

    }
}