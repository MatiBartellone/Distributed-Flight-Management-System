use crate::executables::executable::Executable;
use crate::utils::errors::Errors;
use crate::utils::errors::Errors::ServerError;
use crate::utils::types::frame::Frame;

pub struct PrepareExecutable;

impl Executable for PrepareExecutable {
    fn execute(&mut self, _request: Frame) -> Result<Frame, Errors> {
        Err(ServerError(String::from("Prepare is not implemented.")))
    }
}
