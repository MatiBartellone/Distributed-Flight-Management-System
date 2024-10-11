use crate::parsers::tokens::token::{AritmeticasMath, Literal};


#[derive(Debug, PartialEq)]
pub enum AssignmentValue {
    Simple(Literal),
    Column(String),
    Arithmetic(String, AritmeticasMath, Literal),
}