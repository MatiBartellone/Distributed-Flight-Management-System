use crate::utils::errors::Errors;

pub trait Query {
    fn run(&mut self);
}
