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


//me separa todas las partes que esten entre $, ' o ""
//hola $hola como estas$ "bien" 'vos' el resto de el string
//queda como ["hola ", "$hola como estas$", " "bien" ", "'vos' el resto de el string"]
fn separar_secciones(input: &str) -> Vec<String> {
    let mut res = Vec::new();
    let mut seccion = String::new();
    let mut delimitador: Option<char> = None;
    let mut dentro_seccion = false;
    for c in input.chars() {
        if dentro_seccion {
            if let Some(c) = delimitador {
                seccion.push(c);
                res.push(seccion);
                seccion = String::new();
                dentro_seccion = false;
            } else {
                seccion.push(c);
            }

        } else {
            match c {
                '$' | '"' | '\'' => {
                    if !seccion.is_empty() {
                        res.push(seccion); 
                        seccion = String::new();
                    }
                    delimitador = Some(c);
                    seccion.push(c);
                } 
                _ => seccion.push(c)
                
            }
        }
    }
    if !seccion.is_empty() {
        res.push(seccion);
    }
    res
}

//Elimina todos los comentarios de la query
fn eliminar_comentarios(input: &str) -> String {
    let sin_diagonal = eliminar_between(input, "\n", "//");
    let sin_barra = eliminar_between(&sin_diagonal, "\n", "--");
    eliminar_between(&sin_barra, "*/", "/*")
}

fn separar_palabras(query: &str) -> Vec<String> {
    let query = query
        .replace("\n", " ")
        .replace("\t", " ");
    let query = query
        .replace(">=", " _GE_ ") //Greater Equal (para que no se separen con los otros replace)
        .replace("<=", " _LE_ ") //Less Equal
        .replace("!=", " _DF_ ") 
        .replace("<=", " _LE_ ") 
        .replace("+", " + ")
        .replace("-", " - ")
        .replace("/", " / ")
        .replace("%", " % ")
        .replace("<", " < ")
        .replace(">", " > ")
        .replace("(", " ( ")
        .replace(")", " ) ")
        .replace(")", " , ")
        .replace(";", "");
    query
        .split_whitespace()
        .map(|s| s.to_string())
        .collect() 
}


fn normalizar(entrada: &str) -> Vec<String> {
    let entrada = eliminar_comentarios(entrada);
    entrada.split_whitespace()
        .map(|s| s.to_string())
        .collect() 
}