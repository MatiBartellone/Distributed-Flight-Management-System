use crate::executables::executable::Executable;
use crate::frame::Frame;
use crate::utils::errors::Errors;

pub struct PrepareExecutable;

impl Executable for PrepareExecutable {
    fn execute(&self, _request: Frame) -> Result<Frame, Errors> {
        todo!()
    }
}
