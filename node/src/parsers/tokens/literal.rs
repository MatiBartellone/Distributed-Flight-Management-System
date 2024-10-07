use super::{data_type::DataType, token::Token, terms::Term};

#[derive(Debug, PartialEq)]
pub struct Literal {
    valor: String,
    tipo: DataType,
}

impl Literal {
    pub fn new(valor: String, tipo: DataType) -> Self {
        Literal { valor, tipo }
    }
}

fn is_valid_bigint(input: &str) -> Option<Token> {
    if input.parse::<i64>().is_ok() {
        let literal = Literal::new(input.to_string(), DataType::Bigint);
        return Some(Token::Term(Term::Literal(literal)));
    }
    None
}

fn is_valid_boolean(input: &str) -> Option<Token> {
    match input {
        "true" => {
            let literal = Literal::new("true".to_string(), DataType::Boolean);
            Some(Token::Term(Term::Literal(literal)))
        }
        "false" => {
            let literal = Literal::new("false".to_string(), DataType::Boolean);
            Some(Token::Term(Term::Literal(literal)))
        }
        _ => None,
    }
}

fn is_valid_text(input: &str) -> Option<Token> {
    if input.starts_with('\'') && input.ends_with('\'') && !input.is_empty() {
        let inner = &input[1..input.len() - 1];
        let literal = Literal {
            valor: inner.to_string(),
            tipo: DataType::Text,
        };
        Some(Token::Term(Term::Literal(literal))) 
    } else {
        None
    }
}

pub fn to_literal(word: &str) -> Option<Token> {
    if let Some(token) = is_valid_bigint(word) {
        return  Some(token);
    }
    if let Some(token) = is_valid_boolean(word) {
        return  Some(token);
    }
    if let Some(token) = is_valid_text(word) {
        return  Some(token);
    }
    //si se puede usar regex, es una pavada
    //si no se puede, suerte :))
    None
}