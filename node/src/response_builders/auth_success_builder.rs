use crate::frame::Frame;
use crate::response_builders::frame_builder::FrameBuilder;
use crate::utils::errors::Errors;
use crate::utils::parser_constants:: AUTH_SUCCESS;

pub struct AuthSuccessBuilder;

impl AuthSuccessBuilder {
    pub fn build_auth_success_frame(request_frame: Frame) -> Result<Frame, Errors> {
        let token = vec![]; // token en bytes
        FrameBuilder::build_response_frame(request_frame, AUTH_SUCCESS, token)
    }
}