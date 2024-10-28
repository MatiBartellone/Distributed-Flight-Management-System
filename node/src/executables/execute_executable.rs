use crate::executables::executable::Executable;
use crate::utils::errors::Errors;
use crate::utils::frame::Frame;

pub struct ExecuteExecutable;

impl Executable for ExecuteExecutable {
    fn execute(&mut self, _request: Frame) -> Result<Frame, Errors> {
        todo!()
    }
}
