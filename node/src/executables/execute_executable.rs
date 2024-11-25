use crate::executables::executable::Executable;
use crate::utils::errors::Errors;
use crate::utils::errors::Errors::ServerError;
use crate::utils::types::frame::Frame;

pub struct ExecuteExecutable;

impl Executable for ExecuteExecutable {
    fn execute(&mut self, _request: Frame) -> Result<Frame, Errors> {
        Err(ServerError(String::from("Execute is not implemented.")))
    }
}
