use crate::parsers::tokens::{literal::Literal, terms::ArithMath};

#[derive(Debug, PartialEq)]
pub enum AssignmentValue {
    Simple(Literal),
    Column(String),
    Arithmetic(String, ArithMath, Literal),
}
