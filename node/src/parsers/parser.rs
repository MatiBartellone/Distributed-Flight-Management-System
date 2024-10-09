use crate::{executables::executable::Executable, utils::errors::Errors};

pub trait Parser {
    fn parse(&self, body: &[u8]) -> Result<Box<dyn Executable>, Errors>;
}
