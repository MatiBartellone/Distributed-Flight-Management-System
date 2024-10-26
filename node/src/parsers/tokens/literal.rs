use super::{data_type::DataType, terms::Term, token::Token};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct Literal {
    pub value: String,
    pub(crate) data_type: DataType,
}

impl Literal {
    pub fn new(value: String, data_type: DataType) -> Self {
        Literal { value, data_type }
    }
}

fn is_valid_bigint(input: &str) -> Option<Token> {
    if input.parse::<i64>().is_ok() {
        let literal = Literal::new(input.to_string(), Int);
        return Some(Token::Term(Term::Literal(literal)));
    }
    None
}

fn is_valid_decimal(input: &str) -> Option<Token> {
    let mut chars = input.chars();
    if let Some(first_char) = chars.next() {
        if first_char == '-' {
            if chars.next().is_none() {
                return None;
            }
        } 
        else if !first_char.is_digit(10) {
            return None;
        }
    }

    let mut decimal_point_seen = false;
    for c in input.chars().skip(1) {
        if c == '.' {
            if decimal_point_seen {
                return None;
            }
            decimal_point_seen = true;
        } 
        else if !c.is_digit(10) {
            return None;
        }
    }
    let literal = Literal::new(input.to_owned(), Decimal);
    Some(Token::Term(Term::Literal(literal)))
}

fn is_valid_boolean(input: &str) -> Option<Token> {
    match input {
        "true" => {
            let literal = Literal::new("true".to_string(), Boolean);
            Some(Token::Term(Term::Literal(literal)))
        }
        "false" => {
            let literal = Literal::new("false".to_string(), Boolean);
            Some(Token::Term(Term::Literal(literal)))
        }
        _ => None,
    }
}
fn is_valid_text(input: &str) -> Option<Token> {
    if input.starts_with('\'') && input.ends_with('\'') && input.len() > 2 {
        let inner = &input[1..input.len() - 1]; // Remueve la primera y última comilla
        if !inner.is_empty() {
            let literal = Literal {
                value: inner.to_string(),
                data_type: Text,
            };
            return Some(Token::Term(Term::Literal(literal)));
        }
    }
    None
}
fn is_valid_date(input: &str) -> Option<Token> {
    if input.starts_with('\'') && input.ends_with('\'') && input.contains("-"){
        
        let instances: Vec<&str> = input[1..input.len() - 1].split('-').collect();
        if instances.len() == 3 {
            let _ = instances[0].parse::<usize>().ok()?;
            let month = instances[1].parse::<usize>().ok()?;
            let day = instances[2].parse::<usize>().ok()?;
            if (1..=12).contains(&month) && (1..=31).contains(&day) {
                let inner = &input[1..input.len() - 1];
                let literal = Literal {
                    value: inner.to_string(),
                    data_type: Date,
                };
                return Some(Token::Term(Term::Literal(literal)));
            }
        }
    }
    None
}

fn is_valid_time(input: &str) -> Option<Token> {
    if input.starts_with('\'') && input.ends_with('\'') && input.contains(":"){
        let instances: Vec<&str> = input[1..input.len() - 1].split(':').collect();
        if instances.len() == 3 {
            let hour = instances[0].parse::<usize>().ok()?;
            let minutes = instances[1].parse::<usize>().ok()?;
            let seconds = instances[2].parse::<usize>().ok()?;
            if (0..=23).contains(&hour) && (0..=59).contains(&minutes) && (0..=59).contains(&seconds) {
                let inner = &input[1..input.len() - 1];
                let literal = Literal {
                    value: inner.to_string(),
                    data_type: Date,
                };
                return Some(Token::Term(Term::Literal(literal)));
            }
        }
    }
    None
}

pub fn to_literal(word: &str) -> Option<Token> {
    if let Some(token) = is_valid_date(word) {
        return Some(token);
    }
    if let Some(token) = is_valid_time(word) {
        return Some(token);
    }
    if let Some(token) = is_valid_bigint(word) {
        return Some(token);
    }
   if let Some(token) = is_valid_decimal(word) {
        return Some(token);
    }
    if let Some(token) = is_valid_boolean(word) {
        return Some(token);
    }
    if let Some(token) = is_valid_text(word) {
        return Some(token);
    }
    None
}

pub fn create_literal(value: &str, data_type: DataType) -> Literal {
    Literal {
        value: value.to_string(),
        data_type,
    }
}

use DataType::*;

impl PartialOrd for Literal {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        if self.data_type != other.data_type {
            return None;
        }
        match self.data_type {
            Int => {
                let val1 = self.value.parse::<i64>().ok()?;
                let val2 = other.value.parse::<i64>().ok()?;
                Some(val1.cmp(&val2))
            }
            Boolean => {
                let val1 = self.value.parse::<bool>().ok()?;
                let val2 = other.value.parse::<bool>().ok()?;
                Some(val1.cmp(&val2))
            }
            Decimal => {
                let val1 = self.value.parse::<f64>().ok()?;
                let val2 = other.value.parse::<f64>().ok()?;
                Some(val1.partial_cmp(&val2)?)
            }
            Text => Some(self.value.cmp(&other.value)),
            Date => todo!(),
            Duration => todo!(),
            Time => todo!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Tests para `is_valid_text`
    #[test]
    fn test_is_valid_text_correct_input() {
        let input = "'valor1'";
        let result = is_valid_text(input).unwrap();
        let literal = Literal::new("valor1".to_string(), Text);
        let token = Token::Term(Term::Literal(literal));
        assert_eq!(result, token);
    }

    #[test]
    fn test_is_valid_text_empty_input() {
        let empty_input = "''";
        let empty_result = is_valid_text(empty_input);
        assert_eq!(empty_result, None);
    }

    #[test]
    fn test_is_valid_text_without_quotes() {
        let sin_comillas = "valor";
        let result_sin_comillas = is_valid_text(sin_comillas);
        assert_eq!(result_sin_comillas, None);
    }

    #[test]
    fn test_is_valid_text_incomplete_quotes() {
        let input_con_comillas_incorrectas = "'valor";
        let result_con_comillas_incorrectas = is_valid_text(input_con_comillas_incorrectas);
        assert_eq!(result_con_comillas_incorrectas, None);
    }

    // Tests para `is_valid_bigint`
    #[test]
    fn test_is_valid_bigint_positive() {
        let input = "12345";
        let result = is_valid_bigint(input).unwrap();
        let literal = Literal::new("12345".to_string(), Int);
        let token = Token::Term(Term::Literal(literal));
        assert_eq!(result, token);
    }

    #[test]
    fn test_is_valid_bigint_negative() {
        let input_negativo = "-98765";
        let result_negativo = is_valid_bigint(input_negativo).unwrap();
        let literal_negativo = Literal::new("-98765".to_string(), Int);
        let token_negativo = Token::Term(Term::Literal(literal_negativo));
        assert_eq!(result_negativo, token_negativo);
    }

    #[test]
    fn test_is_valid_bigint_invalid() {
        let input_no_valido = "123abc";
        let result_no_valido = is_valid_bigint(input_no_valido);
        assert_eq!(result_no_valido, None);
    }

    // Tests para `is_valid_boolean`
    #[test]
    fn test_is_valid_boolean_true() {
        let input = "true";
        let result = is_valid_boolean(input).unwrap();
        let literal = Literal::new("true".to_string(), Boolean);
        let token = Token::Term(Term::Literal(literal));
        assert_eq!(result, token);
    }

    #[test]
    fn test_is_valid_boolean_false() {
        let input = "false";
        let result = is_valid_boolean(input).unwrap();
        let literal = Literal::new("false".to_string(), Boolean);
        let token = Token::Term(Term::Literal(literal));
        assert_eq!(result, token);
    }

    #[test]
    fn test_is_valid_boolean_invalid() {
        let input = "notabool";
        let result = is_valid_boolean(input);
        assert_eq!(result, None);
    }

    // Tests para `to_literal`
    #[test]
    fn test_to_literal_bigint() {
        let input = "12345";
        let result = to_literal(input).unwrap();
        let literal = Literal::new("12345".to_string(), Int);
        let token = Token::Term(Term::Literal(literal));
        assert_eq!(result, token);
    }

    #[test]
    fn test_to_literal_boolean_true() {
        let input = "true";
        let result = to_literal(input).unwrap();
        let literal = Literal::new("true".to_string(), Boolean);
        let token = Token::Term(Term::Literal(literal));
        assert_eq!(result, token);
    }

    #[test]
    fn test_to_literal_text() {
        let input = "'valor1'";
        let result = to_literal(input).unwrap();
        let literal = Literal::new("valor1".to_string(), Text);
        let token = Token::Term(Term::Literal(literal));
        assert_eq!(result, token);
    }

    #[test]
    fn test_to_literal_invalid() {
        let input = "notavalidtype";
        let result = to_literal(input);
        assert_eq!(result, None);
    }


    #[test]
    fn test_to_literal_date() {
        let input = "'2004-05-23'";
        let result = to_literal(input).unwrap();
        let literal = Literal::new("2004-05-23".to_string(), Date);
        let token = Token::Term(Term::Literal(literal));
        assert_eq!(result, token);
    }

    #[test]
    fn test_to_literal_time() {
        let input = "'17:30:00'";
        let result = to_literal(input).unwrap();
        let literal = Literal::new("17:30:00".to_string(), Date);
        let token = Token::Term(Term::Literal(literal));
        assert_eq!(result, token);
    }
}
