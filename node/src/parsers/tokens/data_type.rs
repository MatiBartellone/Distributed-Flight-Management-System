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
    match word.to_ascii_lowercase().as_str() {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string_to_data_type() {
        // Casos válidos
        assert_eq!(
            string_to_data_type("int"),
            Some(Token::DataType(DataType::Int))
        );
        assert_eq!(
            string_to_data_type("boolean"),
            Some(Token::DataType(DataType::Boolean))
        );
        assert_eq!(
            string_to_data_type("date"),
            Some(Token::DataType(DataType::Date))
        );
        assert_eq!(
            string_to_data_type("decimal"),
            Some(Token::DataType(DataType::Decimal))
        );
        assert_eq!(
            string_to_data_type("text"),
            Some(Token::DataType(DataType::Text))
        );
        assert_eq!(
            string_to_data_type("duration"),
            Some(Token::DataType(DataType::Duration))
        );
        assert_eq!(
            string_to_data_type("time"),
            Some(Token::DataType(DataType::Time))
        );

        // Casos no válidos
        assert_eq!(string_to_data_type("integer"), None); // Palabra no válida
        assert_eq!(string_to_data_type("BOOLEAN"), Some(Token::DataType(DataType::Boolean))); // Prueba con mayúsculas
        assert_eq!(string_to_data_type("Date"), Some(Token::DataType(DataType::Date))); // Prueba con capitalización mixta
        assert_eq!(string_to_data_type(""), None); // Cadena vacía
        assert_eq!(string_to_data_type("someOtherType"), None); // Tipo completamente diferente
    }
}

