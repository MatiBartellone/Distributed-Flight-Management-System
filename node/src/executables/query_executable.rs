use crate::executables::executable::Executable;
use crate::queries::query::{Query, QueryEnum};
use crate::query_delegation::query_delegator::QueryDelegator;
use crate::response_builders::frame_builder::FrameBuilder;
use crate::utils::consistency_level::ConsistencyLevel;
use crate::utils::errors::Errors;
use crate::utils::parser_constants::RESULT;
use crate::utils::types::frame::Frame;

pub struct QueryExecutable {
    query: Box<dyn Query>,
    consistency_integer: i16,
}

impl QueryExecutable {
    pub fn new(query: Box<dyn Query>, consistency_integer: i16) -> QueryExecutable {
        QueryExecutable {
            query,
            consistency_integer,
        }
    }
}

impl Executable for QueryExecutable {
    fn execute(&mut self, request: Frame) -> Result<Frame, Errors> {
        self.query.set_table()?;
        let pk = self.query.get_partition()?;
        let Some(query_enum) = QueryEnum::from_query(&self.query) else {
            return Err(Errors::ServerError(String::from("")));
        };
        let response_msg = match query_enum {
            QueryEnum::Use(use_query) => use_query.run()?,
            _ => {
                let delegator = QueryDelegator::new(
                    pk,
                    query_enum.into_query(),
                    ConsistencyLevel::from_i16(self.consistency_integer)?,
                );
                delegator.send()?
            }
        };
        let response_frame = FrameBuilder::build_response_frame(request, RESULT, response_msg)?;
        Ok(response_frame)
    }
}
