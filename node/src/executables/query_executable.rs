use crate::executables::executable::Executable;
use crate::frame::Frame;
use crate::queries::query::Query;
use crate::response_builders::frame_builder::FrameBuilder;
use crate::utils::consistency_level::ConsistencyLevel;
use crate::utils::errors::Errors;
use crate::utils::parser_constants::RESULT;

pub struct QueryExecutable{
    query: Box<dyn Query>,
    consistency: ConsistencyLevel
}

impl QueryExecutable {
    pub fn new(query: Box<dyn Query>, consistency: ConsistencyLevel) -> QueryExecutable {
        QueryExecutable { query, consistency}
    }
}

impl Executable for QueryExecutable {
    fn execute(&self, request: Frame) -> Result<Frame, Errors> {
        let _ = self.consistency;
        self.query.run()?;
        FrameBuilder::build_response_frame(request, RESULT, Vec::new())
    }
}