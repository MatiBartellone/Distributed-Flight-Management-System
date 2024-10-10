use crate::executables::executable::Executable;
use crate::frame::Frame;
use crate::utils::errors::Errors;

pub struct QueryExecutable;

impl Executable for QueryExecutable {
    fn execute(&self, _request: Frame) -> Result<Frame, Errors> {
        todo!()
    }
}
