use crate::utils::errors::Errors;
use crate::utils::types::frame::Frame;

pub trait Executable {
    fn execute(&mut self, request: Frame) -> Result<Frame, Errors>;
}
