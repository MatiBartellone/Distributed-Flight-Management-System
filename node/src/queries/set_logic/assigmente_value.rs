use crate::parsers::tokens::{literal::Literal, terms::ArithMath};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub enum AssignmentValue {
    Simple(Literal),
    Column(String),
    Arithmetic(String, ArithMath, Literal),
}
