use crate::executables::executable::Executable;
use crate::queries::query::{Query, QueryEnum};
use crate::response_builders::frame_builder::FrameBuilder;
use crate::utils::constants::PREPARE_FILE;
use crate::utils::errors::Errors;
use crate::utils::errors::Errors::ServerError;
use crate::utils::functions::{deserialize_from_str, serialize_to_string};
use crate::utils::parser_constants::RESULT;
use crate::utils::types::frame::Frame;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::io::{BufRead, BufReader};

pub struct PrepareExecutable {
    query: Box<dyn Query>,
}

impl PrepareExecutable {
    pub fn new(query: Box<dyn Query>) -> Self {
        Self { query }
    }

    fn store_query(&self) -> Result<i16, Errors> {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(PREPARE_FILE)
            .map_err(|_| ServerError(String::from("Unable to open file")))?;
        let query = self.get_new_query()?;
        let serialized = serialize_to_string(&query)?;
        writeln!(file, "{}", serialized)
            .map_err(|_| ServerError(String::from("could not write prepare query")))?;
        Ok(query.id)
    }

    fn get_new_query(&self) -> Result<PrepareQuery, Errors> {
        let file = OpenOptions::new()
            .read(true)
            .open(&PREPARE_FILE)
            .map_err(|_| ServerError(String::from("Unable to open file")))?;
        let mut reader = BufReader::new(file);
        let mut last_line = None;
        for line in reader.lines() {
            if let Ok(line) = line {
                last_line = Some(line);
            }
        }
        if let Some(last_line) = last_line {
            let last_query: PrepareQuery = deserialize_from_str(&last_line.trim())?;
            PrepareQuery::new(&self.query, last_query.id + 1)
        } else {
            PrepareQuery::new(&self.query, 0)
        }
    }
}

impl Executable for PrepareExecutable {
    fn execute(&mut self, request: Frame) -> Result<Frame, Errors> {
        let id = self.store_query()?;
        FrameBuilder::build_response_frame(request, RESULT, id.to_be_bytes().to_vec())
    }
}

#[derive(Deserialize, Serialize)]
pub struct PrepareQuery {
    pub id: i16,
    pub query: QueryEnum,
}
impl PrepareQuery {
    pub fn new(query: &Box<dyn Query>, id: i16) -> Result<Self, Errors> {
        let Some(query_enum) = QueryEnum::from_query(query) else {
            return Err(Errors::ServerError(String::from("")));
        };
        Ok(Self {
            query: query_enum,
            id,
        })
    }
}
