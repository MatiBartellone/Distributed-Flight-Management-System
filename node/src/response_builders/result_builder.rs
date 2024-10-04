use crate::frame::Frame;
use crate::response_builders::frame_builder::FrameBuilder;
use crate::utils::errors::Errors;
use crate::utils::parser_constants::RESULT;

pub struct ResultBuilder;

impl ResultBuilder {
    pub fn build_result_frame(request_frame: Frame) -> Result<Frame, Errors> {
        let result = vec![];
        FrameBuilder::build_response_frame(request_frame, RESULT, result)
    }
}