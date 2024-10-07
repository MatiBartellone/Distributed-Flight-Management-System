use crate::executables::executable::Executable;
use crate::frame::Frame;

pub struct QueryExecutable;

impl Executable for QueryExecutable {
    fn execute(&self, _request: Frame) -> Frame {
        todo!()
    }
}

