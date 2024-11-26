use crate::executables::executable::Executable;
use crate::executables::prepare_executable::PrepareQuery;
use crate::query_delegation::query_delegator::QueryDelegator;
use crate::response_builders::frame_builder::FrameBuilder;
use crate::utils::consistency_level::ConsistencyLevel;
use crate::utils::constants::PREPARE_FILE;
use crate::utils::errors::Errors;
use crate::utils::errors::Errors::{Invalid, ServerError};
use crate::utils::functions::deserialize_from_str;
use crate::utils::parser_constants::RESULT;
use crate::utils::types::frame::Frame;
use std::fs::{rename, File, OpenOptions};
use std::io::Write;
use std::io::{BufRead, BufReader};
use std::path::Path;

pub struct ExecuteExecutable {
    id: i16,
    consistency_integer: i16,
}

impl ExecuteExecutable {
    pub fn new(id: i16, consistency_integer: i16) -> Self {
        ExecuteExecutable {
            id,
            consistency_integer,
        }
    }

    pub fn get_query(&self) -> Result<PrepareQuery, Errors> {
        if !Path::new(PREPARE_FILE).exists() {
            File::create(&PREPARE_FILE)
                .map_err(|_| ServerError(String::from("Unable to create file")))?;
        }
        let file = OpenOptions::new()
            .read(true)
            .open(&PREPARE_FILE)
            .map_err(|_| ServerError(String::from("Unable to open file")))?;

        let reader = BufReader::new(file);
        for line in reader.lines() {
            if let Ok(line) = line {
                let deserialzied: PrepareQuery = deserialize_from_str(&line.trim())?;
                if deserialzied.id == self.id {
                    return Ok(deserialzied);
                }
            }
        }
        Err(Invalid(String::from("Query id not found")))
    }

    pub fn delete_id(&self) -> Result<(), Errors> {
        let file = File::open(PREPARE_FILE)
            .map_err(|_| ServerError(String::from("could not open prepare file")))?;
        let temp_path = format!("{}.tmp", PREPARE_FILE);
        let mut temp_file = File::create(temp_path.as_str())
            .map_err(|_| ServerError(String::from("could not create temp file")))?;
        let mut reader = BufReader::new(file).lines();
        while let Some(Ok(line)) = reader.next() {
            let deserialzied: PrepareQuery = deserialize_from_str(&line.trim())?;
            if deserialzied.id != self.id {
                writeln!(temp_file, "{}", line.trim())
                    .map_err(|_| ServerError(String::from("could not write prepare query")))?;
            }
        }
        rename(temp_path, PREPARE_FILE)
            .map_err(|_| ServerError(String::from("could not replace files")))?;
        Ok(())
    }
}

impl Executable for ExecuteExecutable {
    fn execute(&mut self, request: Frame) -> Result<Frame, Errors> {
        let mut query = self.get_query()?.query.into_query();
        self.delete_id()?;
        query.set_table()?;
        let pk = query.get_partition()?;
        let delegator = QueryDelegator::new(
            pk,
            query,
            ConsistencyLevel::from_i16(self.consistency_integer)?,
        );
        let response_msg = delegator.send()?;
        let response_frame = FrameBuilder::build_response_frame(request, RESULT, response_msg)?;
        Ok(response_frame)
    }
}
