use std::str::FromStr;

use crate::utils::errors::Errors;

#[allow(dead_code)]
pub enum Token {
    Identifier(String),
    Term(Term),
    Reserved(String),
    DataType(DataType),
    TokensList(Vec<Token>),
}

#[allow(dead_code)]
pub enum Term {
    Literal(Literal),
    AritmeticasMath(AritmeticasMath),
    AritmeticasBool(BooleanOperations)
}

#[allow(dead_code)]
#[derive(Debug, PartialOrd, PartialEq)]
pub struct Literal {
    pub valor: String,
    pub tipo: DataType,
}

#[allow(dead_code)]
enum AritmeticasMath {
    Suma,
    Resta,
    Division,
    Resto,
    Multiplication,
}

#[allow(dead_code)]
pub enum BooleanOperations {
    Logical(LogicalOperators),
    Comparison(ComparisonOperators),
}

#[allow(dead_code)]
pub enum LogicalOperators {
    Or,
    And,
    Not,
}

#[allow(dead_code)]
pub enum ComparisonOperators {
    Less,
    Equal,
    NotEqual,
    Greater,
    GreaterOrEqual,
    LessOrEqual,
}

#[allow(dead_code)]
#[derive(Debug, PartialOrd, PartialEq)]
pub enum DataType {
    Integer,
    Boolean,
    Date,
    Decimal,
    Text,
    Duration,
    Time,
}

use DataType::*;

pub fn compare_literals <T> (lit1: &Literal, lit2: &Literal, comparison: fn(&T, &T) -> bool) -> Result<bool, Errors>{
    if lit1.tipo != lit2.tipo {
        return Err(Errors::ProtocolError(format!("Cannot compare values of different types: {:?} and {:?}", lit1.tipo, lit2.tipo)));
    }
    match lit1.tipo {
        Integer => {
            let val1: i64 = lit1.valor.parse().map_err(|_| Errors::ProtocolError("Invalid bigint value".to_string()))?;
            let val2: i64 = lit2.valor.parse().map_err(|_| Errors::ProtocolError("Invalid bigint value".to_string()))?;
            Ok(comparison(&val1, &val2))
        },
        Decimal => {
            let val1: f64 = lit1.valor.parse().map_err(|_| Errors::ProtocolError("Invalid decimal value".to_string()))?;
            let val2: f64 = lit2.valor.parse().map_err(|_| Errors::ProtocolError("Invalid decimal value".to_string()))?;
            Ok(comparison(&val1, &val2))
        },
        Text => {
            let val1: &str = &lit1.valor;
            let val2: &str = &lit2.valor;
            Ok(comparison(&val1, &val2))
        },
        Boolean => {
            let val1: bool = lit1.valor.parse().map_err(|_| Errors::ProtocolError("Invalid boolean value".to_string()))?;
            let val2: bool = lit2.valor.parse().map_err(|_| Errors::ProtocolError("Invalid boolean value".to_string()))?;
            Ok(comparison(&val1, &val2))
        },
        Date => todo!(),
        Duration => todo!(),
        Time => todo!(),
    }
}

// #[allow(dead_code)]
// fn eliminar_between(input: &str, fin: &str, ini: &str) -> String {
//     let mut res = String::new();
//     let mut dentro_comentario = false;
//     for (i, c) in input.chars().enumerate() {
//         if dentro_comentario {
//             if i + 1 < input.len() && &input[i..i+2] == fin {
//                 dentro_comentario = false;
//             }
//         } else {
//             if i + 1 < input.len() && &input[i..i+2] == ini {
//                 dentro_comentario = true;
//             } else {
//                 res.push(c);
//             }
//         }
//     }
//     res
// }
//
// #[allow(dead_code)]
// fn eliminar_comentarios(input: &str) -> String {
//     let sin_diagonal = eliminar_between(input, "\n", "//");
//     let sin_barra = eliminar_between(&sin_diagonal, "\n", "--");
//     eliminar_between(&sin_barra, "*/", "/*")
// }
// #[allow(dead_code)]
// fn normalizar(entrada: &str) -> Vec<String> {
//     let entrada = eliminar_comentarios(entrada);
//     entrada.split_whitespace()
//         .map(|s| s.to_string())
//         .collect()
// }