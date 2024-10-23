use crate::executables::executable::Executable;
use crate::frame::Frame;
use crate::utils::errors::Errors;

pub struct RegisterExecutable;

impl Executable for RegisterExecutable {
    fn execute(&mut self, _request: Frame) -> Result<Frame, Errors> {
        todo!()
    }
}
