use crate::executables::executable::Executable;
use crate::frame::Frame;
use crate::utils::errors::Errors;

pub struct AuthResponseExecutable;

impl Executable for AuthResponseExecutable {
    fn execute(&self, _request: Frame) -> Result<Frame, Errors> {
        todo!()
    }
}
