use super::token::Token;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub enum DataType {
    Int,
    Boolean,
    Date,
    Decimal,
    Text,
    Duration,
    Time,
}

pub fn string_to_data_type(word: &str) -> Option<Token> {
    match word {
        "int" => Some(Token::DataType(DataType::Int)),
        "boolean" => Some(Token::DataType(DataType::Boolean)),
        "date" => Some(Token::DataType(DataType::Date)),
        "decimal" => Some(Token::DataType(DataType::Decimal)),
        "text" => Some(Token::DataType(DataType::Text)),
        "duration" => Some(Token::DataType(DataType::Duration)),
        "time" => Some(Token::DataType(DataType::Time)),
        _ => None,
    }
}

pub fn data_type_to_string(data_type: &DataType) -> String {
    match data_type {
        DataType::Int => "int".to_string(),
        DataType::Boolean => "boolean".to_string(),
        DataType::Date => "date".to_string(),
        DataType::Decimal => "decimal".to_string(),
        DataType::Text => "text".to_string(),
        DataType::Duration => "duration".to_string(),
        DataType::Time => "time".to_string(),
    }
}
