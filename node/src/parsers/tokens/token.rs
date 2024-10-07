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
    AritmeticasBool(AritmeticasBool)
}
#[allow(dead_code)]
#[derive(PartialEq, Debug)]
pub struct Literal {
    pub valor: String,
    pub tipo: DataType,
}
#[allow(dead_code)]
pub enum AritmeticasMath {
    Suma,
    Resta,
    Division,
    Resto,
    Multiplication,
}
#[allow(dead_code)]
pub enum AritmeticasBool {
    Or,
    And,
    Not, 
    Menor,
    Igual,
    Disinto,
    Mayor,
    MayorIgual,
    MenorIgual
}
#[allow(dead_code)]
#[derive(PartialEq, Debug)]
pub enum DataType {
    Bigint,
    Boolean,
    Date,
    Decimal,
    Text,
    Duration,
    Time,
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