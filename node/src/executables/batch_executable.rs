use crate::executables::executable::Executable;
use crate::frame::Frame;

struct BatchExecutable {}

impl Executable for BatchExecutable {
    fn execute(&self) -> Frame {
        todo!()
    }
}