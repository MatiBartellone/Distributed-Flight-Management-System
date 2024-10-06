enum Token {
    Identifier(String),
    Term(Term),
    Reserved(String),
    DataType(DataType),
    TokensList(Vec<Token>),
}

enum Term {
    Literal(Literal),
    AritmeticasMath(AritmeticasMath),
    AritmeticasBool(AritmeticasBool)
}

struct Literal {
    valor: String,
    tipo: DataType,
}

enum AritmeticasMath {
    Suma,
    Resta,
    Division,
    Resto,
    Multiplication,
}

enum AritmeticasBool {
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

enum DataType {
    Bigint,
    Boolean,
    Date,
    Decimal,
    Text,
    Duration,
    Time,
}


fn eliminar_between(input: &str, fin: &str, ini: &str) -> String {
    let mut res = String::new();
    let mut dentro_comentario = false;
    for (i, c) in input.chars().enumerate() {
        if dentro_comentario {
            if i + 1 < input.len() && &input[i..i+2] == fin {
                dentro_comentario = false;
            }
        } else {
            if i + 1 < input.len() && &input[i..i+2] == ini {
                dentro_comentario = true;
            } else {
                res.push(c);
            }
        }
    }
    res
}


fn eliminar_comentarios(input: &str) -> String {
    let sin_diagonal = eliminar_between(input, "\n", "//");
    let sin_barra = eliminar_between(&sin_diagonal, "\n", "--");
    eliminar_between(&sin_barra, "*/", "/*")
}

fn normalizar(entrada: &str) -> Vec<String> {
    let entrada = eliminar_comentarios(entrada);
    entrada.split_whitespace()
        .map(|s| s.to_string())
        .collect() 
}