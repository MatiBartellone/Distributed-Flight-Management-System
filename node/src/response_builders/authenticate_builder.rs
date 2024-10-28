use crate::response_builders::frame_builder::FrameBuilder;
use crate::utils::errors::Errors;
use crate::utils::frame::Frame;
use crate::utils::parser_constants::AUTHENTICATE;

pub struct AuthenticateBuilder;

impl AuthenticateBuilder {
    pub fn build_authenticate_frame(request_frame: Frame) -> Result<Frame, Errors> {
        let auth_class = vec![]; // HAY QUE ELEGIR AUTH CLASS Y PASARLA EN BYTES
        FrameBuilder::build_response_frame(request_frame, AUTHENTICATE, auth_class)
    }
}
