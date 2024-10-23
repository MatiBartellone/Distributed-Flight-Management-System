use crate::{frame::Frame, utils::errors::Errors};

pub trait Executable {
    fn execute(&mut self, request: Frame) -> Result<Frame, Errors>;
}
