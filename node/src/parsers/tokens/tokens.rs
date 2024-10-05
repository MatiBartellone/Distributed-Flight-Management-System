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
    tipo: Type,
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


fn normalizar(entrada: &str) -> Vec<String> {
    
}