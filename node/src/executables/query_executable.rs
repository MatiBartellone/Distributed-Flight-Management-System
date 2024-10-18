use crate::executables::executable::Executable;
use crate::frame::Frame;
use crate::meta_data::nodes::node_meta_data_acces::NodesMetaDataAccess;
use crate::node_communication::query_delegator::QueryDelegator;
use crate::queries::query::{Query, QueryEnum};
use crate::response_builders::frame_builder::FrameBuilder;
use crate::utils::consistency_level::ConsistencyLevel;
use crate::utils::constants::NODES_METADATA;
use crate::utils::errors::Errors;
use crate::utils::parser_constants::RESULT;

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

        let Some(pk) = self.query.get_primary_key() else {
            return Err(Errors::ServerError(String::from("")))
        };;
        let Some(node) = NodesMetaDataAccess::get_delegation(NODES_METADATA, pk)? else {
            return Err(Errors::ServerError(String::from("")))
        }; ;
        let Some(query_enum) = QueryEnum::from_query(&self.query) else {
            return Err(Errors::ServerError(String::from("")))
        };
        let delegator = QueryDelegator::new(node.get_pos() as i32, query_enum.into_query(), ConsistencyLevel::One);
        let response_msg = delegator.send()?;
        let response_frame = FrameBuilder::build_response_frame(request, RESULT, response_msg)?;
        Ok(response_frame)
    }
}
