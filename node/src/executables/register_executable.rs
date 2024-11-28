use crate::executables::executable::Executable;
use crate::utils::errors::Errors;
use crate::utils::errors::Errors::ServerError;
use crate::utils::types::frame::Frame;

pub struct RegisterExecutable;

impl Executable for RegisterExecutable {
    fn execute(&mut self, _request: Frame) -> Result<Frame, Errors> {
        Err(ServerError(String::from("Register is not implemented.")))
    }
}
