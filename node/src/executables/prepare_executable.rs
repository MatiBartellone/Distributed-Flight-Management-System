use std::fs::OpenOptions;
use std::io::Write;

use crate::executables::executable::Executable;
use crate::frame::Frame;
use crate::response_builders::frame_builder::FrameBuilder;
use crate::utils::errors::Errors;
use crate::utils::parser_constants::RESULT;

const PREPARED_QUERYS: &str = "prepared_querys.csv";
const ACTIVE: &str = "active";

pub struct PrepareExecutable {
    id: uuid::Uuid,
    query: String,
}
impl PrepareExecutable {
    pub fn new(id: uuid::Uuid, query: String) -> PrepareExecutable {
        PrepareExecutable { id, query }
    }

    fn format_query_for_csv(&self) -> String {
        format!("{},{},{}", self.id, self.query, ACTIVE)
    }

    fn save_query(&self) -> Result<(), Errors> {
        let mut file = OpenOptions::new()
            .write(true)
            .append(true)
            .open(PREPARED_QUERYS)
            .map_err(|_| Errors::ProtocolError(String::from("Something went wrong")))?;

        let line = self.format_query_for_csv();
        writeln!(file, "{}", line)
            .map_err(|_| Errors::ProtocolError(String::from("Something went wrong")))?;

        Ok(())
    }

    fn build_metadata(&self) -> Vec<u8> {
        todo!();
    }

    fn build_result_metadata(&self) -> Vec<u8> {
        todo!();
    }

    fn build_kind_response(&self) -> Vec<u8> {
        let mut body = Vec::new();
        let kind = 0x0004;
        let id = self.id.as_bytes();
        let metadata = self.build_metadata();
        let result_metadata = self.build_result_metadata();
        body.push(kind);
        body.push(id.len() as u8);
        body.extend(id);
        body.extend(metadata);
        body.extend(result_metadata);
        body
    }
}

impl Executable for PrepareExecutable {
    fn execute(&self, request: Frame) -> Result<Frame, Errors> {
        self.save_query()?;
        let body = self.build_kind_response(); // Tal vez necesite la request para acceder a las flags
        let frame = FrameBuilder::build_response_frame(request, RESULT, body)?;
        Ok(frame)
    }
}
