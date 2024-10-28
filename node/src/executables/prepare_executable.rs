use crate::executables::executable::Executable;
use crate::utils::errors::Errors;
use crate::utils::frame::Frame;

pub struct PrepareExecutable;

impl Executable for PrepareExecutable {
    fn execute(&mut self, _request: Frame) -> Result<Frame, Errors> {
        todo!()
    }
}
