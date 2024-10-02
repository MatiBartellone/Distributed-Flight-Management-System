use node::executables::executable::Executable;

pub trait  Parser {
    fn parse(&self, bytes : &[u8]) -> dyn Executable;
}