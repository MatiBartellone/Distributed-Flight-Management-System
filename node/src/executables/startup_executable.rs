use crate::executables::executable::Executable;
use crate::frame::Frame;

pub struct StartupExecutable;

impl Executable for StartupExecutable {
    fn execute(&self) -> Frame {
        todo!()
    }
}
