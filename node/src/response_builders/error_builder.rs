use crate::response_builders::frame_builder::FrameBuilder;
use crate::utils::errors::Errors;
use crate::utils::parser_constants::ERROR;
use crate::utils::types::frame::Frame;

pub struct ErrorBuilder;

impl ErrorBuilder {
    pub fn build_error_frame(request_frame: Frame, error: Errors) -> Result<Frame, Errors> {
        let error_msg = error.get_bytes_body();
        FrameBuilder::build_response_frame(request_frame, ERROR, error_msg)
    }
}
