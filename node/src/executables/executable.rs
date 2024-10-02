use crate::frame::Frame;

pub trait Executable {
    fn execute(&self) -> Frame;
}