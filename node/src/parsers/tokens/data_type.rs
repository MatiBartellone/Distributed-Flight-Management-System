use super::token::Token;

#[derive(Debug, PartialEq)]
pub enum DataType {
    Bigint,
    Boolean,
    Date,
    Decimal,
    Text,
    Duration,
    Time,
}

pub fn string_to_data_type(word: &str) -> Option<Token> {
    match word {
        "bigint" => Some(Token::DataType(DataType::Bigint)),
        "boolean" => Some(Token::DataType(DataType::Boolean)),
        "date" => Some(Token::DataType(DataType::Date)),
        "decimal" => Some(Token::DataType(DataType::Decimal)),
        "text" => Some(Token::DataType(DataType::Text)),
        "duration" => Some(Token::DataType(DataType::Duration)),
        "time" => Some(Token::DataType(DataType::Time)),
        _ => None, 
    }
}
