fn caracteres(palabra: &str, inicio: usize, fin: usize) -> String {
    palabra.chars().skip(inicio).take(fin - inicio).collect()
}

fn inside_comment(input: &str, fin: &str,i: &mut usize, mut dentro: &mut bool) -> () {
    if *dentro {
        if *i + fin.len() <= input.len() && caracteres(input, *i, *i + fin.len()) == fin {
            *dentro = false; 
            *i += fin.len(); 
        } else {
            *i += 1; 
        }
    }
}

fn out_comment(input: &str, ini: &str,i: &mut usize, dentro: &mut bool,res: &mut String) -> () {
    if !*dentro {
        if *i + ini.len() <= input.len() && caracteres(input, *i, *i + ini.len()) == ini {
            *dentro = true; 
            *i += ini.len(); 
        } else {
            if let Some(c) = input.chars().nth(*i) {
                res.push(c);
            }
            *i += 1;
        }
    }
}


fn eliminar_between(input: &str, fin: &str, ini: &str) -> String {
    let mut res = String::new();
    let mut dentro_comentario = false;
    let mut i = 0;
    while i < input.len() {
        inside_comment(input, fin, &mut i, &mut dentro_comentario);
        out_comment(input, ini, &mut i, &mut dentro_comentario, &mut res);
    }
    res
}


fn inside_seccion( 
    c: char,
    seccion: &mut String,
    res: &mut Vec<String>,
    dentro_seccion: &mut bool,
    delimitador: &mut Option<char>,
) {
    seccion.push(c);
    if let Some(del) = delimitador {
        if c == *del {
            res.push(seccion.to_string());
            *seccion = String::new();
            *dentro_seccion = false;
            *delimitador = None;
        }
    } 
    
}

fn out_seccion(
    c: char,
    seccion: &mut String,
    res: &mut Vec<String>,
    dentro_seccion: &mut bool,
    delimitador: &mut Option<char>,
) {
    match c {
        '$' | '"' | '\'' => {
            if !seccion.is_empty() {
                res.push(seccion.clone());
                seccion.clear();
            }
            *delimitador = Some(c);
            seccion.push(c);
            *dentro_seccion = true;
        }
        _ => seccion.push(c),
    }
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
        if dentro_seccion{
            inside_seccion(c, &mut seccion, &mut res, &mut dentro_seccion, &mut delimitador);
        } else {
            out_seccion(c, &mut seccion, &mut res, &mut dentro_seccion, &mut delimitador);
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

fn es_seccion(palabra: &str) -> bool {
    // if let Some(primer_caracter) = palabra.chars().next() {
    //     return primer_caracter == '$' || primer_caracter == '\'' || primer_caracter == '"';
    // }
    // false
    matches!(palabra.chars().next(), Some('$' | '\'' | '"'))
}


fn normalizar(entrada: &str) -> Vec<String> {
    let entrada = eliminar_comentarios(entrada);
    let secciones = separar_secciones(&entrada);
    let mut normalizada = Vec::new();
    for palabra in secciones.iter() {
        if !es_seccion(&palabra) {
            let vocablos = separar_palabras(&palabra);
            for vocablo in vocablos.iter(){
                normalizada.push(vocablo.to_string());
            }
        } else {
            normalizada.push(palabra.to_string())
        }
    }
    normalizada
}

#[cfg(test)]
mod tests {
    use super::*;

    fn imprimir_vector(v: &Vec<String>) {
        for item in v.iter() {
            println!("{}", item);
        }
    }

    #[test]
    fn test_normalizar_con_secciones() {
        let entrada = r#"hola $hola como estas$ "bien" 'vos' el resto de el string"#;
        let resultado = normalizar(entrada);
        let esperado = vec![
            "hola".to_string(),
            "$hola como estas$".to_string(),
            "\"bien\"".to_string(),
            "'vos'".to_string(),
            "el".to_string(),
            "resto".to_string(),
            "de".to_string(),
            "el".to_string(),
            "string".to_string(),
        ];
        //imprimir_vector(&resultado);
        assert_eq!(resultado, esperado);
    }

    #[test]
    fn test_normalizar_con_comentarios() {
        let entrada = r#"hola // comentario
                         $esto es$ "un" -- otro comentario
                         'string'"#;
        let resultado = normalizar(entrada);
        let esperado = vec![
            "hola".to_string(),
            "$esto es$".to_string(),
            "\"un\"".to_string(),
            "'string'".to_string(),
        ];
        assert_eq!(resultado, esperado);
    }

    #[test]
    fn test_normalizar_con_espacios() {
        let entrada = r#"$palabra1$    $palabra2$  "string con espacios" 'otro string'"#;
        let resultado = normalizar(entrada);
        let esperado = vec![
            "$palabra1$".to_string(),
            "$palabra2$".to_string(),
            "\"string con espacios\"".to_string(),
            "'otro string'".to_string(),
        ];
        assert_eq!(resultado, esperado);
    }

    #[test]
    fn test_normalizar_solo_comentarios() {
        let entrada = r#"//comentario
                         -- otro comentario
                         /* aaaa 
                         aaaa */
                         "#;
        let resultado = normalizar(entrada);
        let esperado: Vec<String> = vec![]; // Sin secciones, solo comentarios
        assert_eq!(resultado, esperado);
    }

    #[test]
    fn test_normalizar_vacio() {
        let entrada = "";
        let resultado = normalizar(entrada);
        let esperado: Vec<String> = vec![];
        assert_eq!(resultado, esperado);
    }
    #[test]
    fn test_normalizar_con_comentario_bloque() {
        let entrada = r#"
            SELECT name, age /* Esto es un 
            comentario en bloque */
            FROM users WHERE age > 25;
        "#;

        let resultado = normalizar(entrada);
        
        let esperado = vec![
            "SELECT".to_string(),
            "name,".to_string(),
            "age".to_string(),
            "FROM".to_string(),
            "users".to_string(),
            "WHERE".to_string(),
            "age".to_string(),
            ">".to_string(), // Operador >
            "25".to_string() // El punto y coma se mantiene al final
        ];

        assert_eq!(resultado, esperado);
    }

    #[test]
    fn test_normalizar_query_cassandra() {
        let entrada = r#"
            // Este es un comentario
            SELECT name, age 
            FROM users -- Comentario de una sola línea
            WHERE age >= 30 /* Comentario multilinea 
            que debe ser eliminado */
            AND age = age + 2 
            /* Comentario
            que sigue */
            LIMIT 10;
        "#;

        let resultado = normalizar(entrada);
        
        let esperado = vec![
            "SELECT".to_string(),
            "name,".to_string(),
            "age".to_string(),
            "FROM".to_string(),
            "users".to_string(),
            "WHERE".to_string(),
            "age".to_string(),
            "_GE_".to_string(), // Para el operador >=
            "30".to_string(),
            "AND".to_string(),
            "age".to_string(),
            "=".to_string(),
            "age".to_string(),
            "+".to_string(), // Para el operador +
            "2".to_string(),
            "LIMIT".to_string(),
            "10".to_string()
        ];

        assert_eq!(resultado, esperado);
    }
    
}
