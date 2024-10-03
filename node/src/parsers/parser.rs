use crate::executables::executable::Executable;

pub trait  Parser {
    fn parse(&self, bytes : &[u8]) -> Box<dyn Executable>;
}