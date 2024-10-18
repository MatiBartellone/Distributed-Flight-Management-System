use crate::executables::executable::Executable;
use crate::frame::Frame;
use crate::node_communication::query_delegator::QueryDelegator;
use crate::queries::query::{Query, QueryEnum};
use crate::response_builders::frame_builder::FrameBuilder;
use crate::utils::consistency_level::ConsistencyLevel;
use crate::utils::errors::Errors;
use crate::utils::parser_constants::RESULT;
use serde::{Deserialize, Serialize};
use std::fs::File;

pub struct QueryExecutable {
    query: Box<dyn Query>,
    consistency: ConsistencyLevel,
}

impl QueryExecutable {
    pub fn new(query: Box<dyn Query>, consistency: ConsistencyLevel) -> QueryExecutable {
        QueryExecutable { query, consistency }
    }
}

impl Executable for QueryExecutable {
    fn execute(&self, request: Frame) -> Result<Frame, Errors> {
        //let pk = query.get_primary_key()
        //let node = hash(pk)
        //let delegator = QueryDelegator::new(node, query_enum.into_query(), self.consistency);
        //let text = delegator.send()

        // let pk = query.get_primary_key()
        // let node = get_delegation()
        // let delegator = QueryDelegator::new(node, query_enum.into_query(), self.consistency);
        // let response_msg = delegator.send()
        // let response_frame = FrameBuilder::build_response_frame(request, RESULT, response_msg)
        Ok(response_frame)
    }
}
