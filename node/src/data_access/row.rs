use serde::{Deserialize, Serialize};
use crate::parsers::tokens::literal::Literal;

#[derive(Debug, Deserialize, Serialize)]
pub struct Row {
    columns: Vec<Column>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Column {
    column_name: String,
    value: Literal,
    time_stamp: String,
}