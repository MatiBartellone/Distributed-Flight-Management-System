use crate::executables::executable::Executable;
use crate::frame::Frame;
use crate::utils::errors::Errors;

pub struct EventExecutable;

impl Executable for EventExecutable {
    fn execute(&mut self, _request: Frame) -> Result<Frame, Errors> {
        todo!()
    }
}
