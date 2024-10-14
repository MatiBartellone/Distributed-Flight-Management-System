use crate::utils::errors::Errors;

pub trait Query {
    fn run(&self) -> Result<String, Errors>;
}
