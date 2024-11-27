use crate::executables::executable::Executable;
use crate::utils::errors::Errors;
use crate::utils::errors::Errors::ServerError;
use crate::utils::types::frame::Frame;

pub struct BatchExecutable;

impl Executable for BatchExecutable {
    fn execute(&mut self, _request: Frame) -> Result<Frame, Errors> {
        Err(ServerError(String::from("Batch is not implemented.")))
    }
}
