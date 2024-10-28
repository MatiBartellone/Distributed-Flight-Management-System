use crate::executables::executable::Executable;
use crate::utils::errors::Errors;
use crate::utils::frame::Frame;

pub struct EventExecutable;

impl Executable for EventExecutable {
    fn execute(&mut self, _request: Frame) -> Result<Frame, Errors> {
        todo!()
    }
}
