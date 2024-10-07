use crate::{frame::Frame, utils::errors::Errors};

pub trait Executable {
    fn execute(&self, request: Frame) -> Result<Frame, Errors>;
}
