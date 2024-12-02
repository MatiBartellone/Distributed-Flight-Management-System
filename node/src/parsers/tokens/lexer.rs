//! Módulo que proporciona funciones para procesar y estandarizar cadenas de texto.
//!
//! particularmente en el contexto de consultas SQL. Este módulo incluye funciones
//! para eliminar comentarios, dividir el texto en secciones delimitadas, y estandarizar
//! la entrada eliminando los comentarios y separando las palabras según las reglas definidas.
//!
//! Las funciones permiten:
//! - **Eliminar comentarios**: Soporta la eliminación de comentarios de una línea y de bloque.
//! - **Dividir el texto en secciones**: Identifica y divide partes del texto delimitadas por caracteres especiales como `$`, `'` o `"`, conservando estas secciones para su posterior procesamiento.
//! - **Estandarizar la entrada**: Elimina los comentarios y divide el texto en palabras o secciones de acuerdo con las reglas de formato definidas, facilitando el análisis posterior de la cadena de texto.
//!
//! Este módulo puede ser útil para preprocesar consultas CQL, eliminar comentarios y manejar las secciones del texto de manera estructurada para su análisis o transformación posterior.

use crate::utils::parser_constants::SPACE;
use crate::utils::parser_constants::*;

use super::character_mapping::CharacterMappings;

fn characters(word: &str, start: usize, end: usize) -> String {
    word.chars().skip(start).take(end - start).collect()
}

fn inside_comment_fn(input: &str, end: &str, i: &mut usize, inside: &mut bool) {
    if *inside {
        if *i + end.len() <= input.len() && characters(input, *i, *i + end.len()) == end {
            *inside = false;
            *i += end.len();
        } else {
            *i += 1;
        }
    }
}

fn out_comment(input: &str, ini: &str, i: &mut usize, inside: &mut bool, res: &mut String) {
    if !*inside {
        if *i + ini.len() <= input.len() && characters(input, *i, *i + ini.len()) == ini {
            *inside = true;
            *i += ini.len();
        } else {
            if let Some(c) = input.chars().nth(*i) {
                res.push(c);
            }
            *i += 1;
        }
    }
}

fn remove_between(input: &str, end: &str, ini: &str) -> String {
    let mut res = String::new();
    let mut inside_comment = false;
    let mut i = 0;
    while i < input.len() {
        inside_comment_fn(input, end, &mut i, &mut inside_comment);
        out_comment(input, ini, &mut i, &mut inside_comment, &mut res);
    }
    res
}

fn inside_section_fn(
    c: char,
    section: &mut String,
    res: &mut Vec<String>,
    inside_section: &mut bool,
    delimiter: &mut Option<char>,
) {
    section.push(c);
    if let Some(del) = delimiter {
        if c == *del {
            res.push(section.to_string());
            *section = String::new();
            *inside_section = false;
            *delimiter = None;
        }
    }
}

fn out_section(
    c: char,
    section: &mut String,
    res: &mut Vec<String>,
    inside_section: &mut bool,
    delimiter: &mut Option<char>,
) {
    match c {
        DOLLAR | DOUBLE_QUOTE | SINGLE_QUOTE => {
            if !section.is_empty() {
                res.push(section.to_string());
                section.clear();
            }
            *delimiter = Some(c);
            section.push(c);
            *inside_section = true;
        }
        _ => section.push(c),
    }
}

/// Divide una cadena de texto en secciones, separadas por los delimitadores `$`, `'` o `"`.
/// Cada sección será un string independiente en el vector de salida, incluyendo los delimitadores.
/// Las secciones son aquellas partes del texto que están entre comillas simples, dobles o símbolos de dólar.
///
/// # Argumentos
/// * `input`: La cadena de texto de entrada a dividir.
///
/// # Retorno
/// Un `Vec<String>` con las secciones del texto, donde cada sección está entre los delimitadores
/// definidos: `$`, `'`, o `"`. Las partes no delimitadas también se incluyen como strings individuales.
///
/// # Ejemplo
/// ```ignore
/// let input = "hola $hola como estas$ \"bien\" 'vos' el resto de el string";
/// let result = divide_sections(input);
/// assert_eq!(result, ["hola ", "$hola como estas$", " \"bien\" ", "'vos' el resto de el string"]);
/// ```
fn divide_sections(input: &str) -> Vec<String> {
    let mut res = Vec::new();
    let mut section = String::new();
    let mut delimiter: Option<char> = None;
    let mut inside_section = false;
    for c in input.chars() {
        if inside_section {
            inside_section_fn(
                c,
                &mut section,
                &mut res,
                &mut inside_section,
                &mut delimiter,
            );
        } else {
            out_section(
                c,
                &mut section,
                &mut res,
                &mut inside_section,
                &mut delimiter,
            );
        }
    }
    if !section.is_empty() {
        res.push(section);
    }
    res
}

/// Elimina todos los comentarios de una cadena de texto, incluyendo comentarios de una línea
/// (tanto con `//` como con `--`) y comentarios de bloque (`/* */`).
///
/// # Argumentos
/// * `input`: La cadena de texto de entrada que puede contener comentarios.
///
/// # Retorno
/// La cadena de texto sin comentarios.
///
/// # Ejemplo
/// ```ignore
/// let input = "SELECT * FROM table -- comentario\n WHERE x = 1 /* comentario bloque */";
/// let result = remove_comments(input);
/// assert_eq!(result, "SELECT * FROM table \n WHERE x = 1 ");
/// ```
fn remove_comments(input: &str) -> String {
    let without_diagonal = remove_between(input, "\n", "//");
    let without_bar = remove_between(&without_diagonal, "\n", "--");
    remove_between(&without_bar, "*/", "/*")
}

fn replace_double_chars(query: &str) -> String {
    let mut result = String::new();
    let mut chars = query.chars().peekable();
    let characters = CharacterMappings::new();

    while let Some(current) = chars.next() {
        if let Some(&next) = chars.peek() {
            let pair = format!("{}{}", current, next);

            if let Some(replace) = characters.get_mapping(&pair) {
                result.push_str(replace); // Agregamos el reemplazo al resultado
                chars.next(); // Consumimos el siguiente carácter ya que procesamos el par
                continue; // Pasamos al siguiente ciclo
            }
        }

        // Si no es un par mapeado, agregamos el carácter actual
        result.push(current);
    }

    result
}

fn replace_simple_chars(query: &str) -> String {
    let mut result = String::new();
    let mut chars = query.chars().peekable();
    let characters = CharacterMappings::new();

    while let Some(current) = chars.next() {
        if let Some(&next) = chars.peek() {
            if current == '-' && next.is_ascii_digit() {
                result.push(current);
                continue;
            }
        }
        if let Some(replace) = characters.get_mapping(&current.to_string()) {
            result.push_str(replace);
            
        }
        else {
            result.push(current);
        }
        
    }

    result
}

fn divide_words(query: &str) -> Vec<String> {
    let query = query.replace("\n", SPACE).replace("\t", SPACE);
    let query = replace_double_chars(&query);
    let query = replace_simple_chars(&query);
    query.split_whitespace().map(|s| s.to_string()).collect()
}

fn is_section(word: &str) -> bool {
    matches!(word.chars().next(), Some('$' | '\'' | '"'))
}


/// Establece un formato estándar para la entrada de texto, eliminando los comentarios y dividiendo
/// el texto en palabras o secciones, de acuerdo con el contexto de los delimitadores.
///
/// # Argumentos
/// * `input`: La cadena de texto de entrada que puede contener comentarios y secciones delimitadas.
///
/// # Retorno
/// Un `Vec<String>` que contiene las palabras y secciones del texto en su formato estandarizado,
/// donde los comentarios han sido eliminados y las secciones delimitadas se conservan tal como están.
///
/// # Ejemplo
/// ```ignore
/// let input = "SELECT * FROM table -- comentario\n WHERE x = 1 /* comentario bloque */";
/// let result = standardize(input);
/// assert_eq!(result, ["SELECT", "*", "FROM", "table", "WHERE", "x", "=", "1"]);
/// ```
pub fn standardize(input: &str) -> Vec<String> {
    let input = remove_comments(input);
    let sections = divide_sections(&input);
    let mut standard = Vec::new();
    for word in sections.iter() {
        if !is_section(word) {
            let standar_words = divide_words(word);
            for standar_word in standar_words.iter() {
                standard.push(standar_word.to_string());
            }
        } else {
            standard.push(word.to_string())
        }
    }
    standard
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_standardize_with_sections() {
        let input = r#"hola $hola como estas$ "bien" 'vos' el resto de el string"#;
        let resultado = standardize(input);
        let expected = vec![
            "hola",
            "$hola como estas$",
            "\"bien\"",
            "'vos'",
            "el",
            "resto",
            "de",
            "el",
            "string",
        ];
        //imprimir_vector(&resultado);
        assert_eq!(resultado, expected);
    }

    #[test]
    fn test_negative_number() {
        let input = r#"hola -234 hola1"#;
        let resultado = standardize(input);
        let expected = vec![
            "hola",
            "-234",
            "hola1",
        ];
        //imprimir_vector(&resultado);
        assert_eq!(resultado, expected);
    }

    #[test]
    fn test_standardize_with_comments() {
        let input = r#"hola // comentario
                         $esto es$ "un" -- otro comentario
                         'string'"#;
        let resultado = standardize(input);
        let esperado = vec!["hola", "$esto es$", "\"un\"", "'string'"];
        assert_eq!(resultado, esperado);
    }

    #[test]
    fn test_standardize_with_spaces() {
        let input = r#"$palabra1$    $palabra2$  "string con espacios" 'otro string'"#;
        let result = standardize(input);
        let expected = vec![
            "$palabra1$",
            "$palabra2$",
            "\"string con espacios\"",
            "'otro string'",
        ];
        assert_eq!(result, expected);
    }

    #[test]
    fn test_standardize_only_comments() {
        let input = r#"//comentario
                         -- otro comentario
                         /* aaaa 
                         aaaa */
                         "#;
        let resultado = standardize(input);
        let esperado: Vec<String> = vec![]; // Sin sections, solo comments
        assert_eq!(resultado, esperado);
    }

    #[test]
    fn test_standardize_empty() {
        let input = "";
        let resultado = standardize(input);
        let esperado: Vec<String> = vec![];
        assert_eq!(resultado, esperado);
    }
    #[test]
    fn test_standardize_with_block_comment() {
        let input = r#"
            SELECT name, age /* Esto es un 
            comentario en bloque */
            FROM users WHERE age > 25;
        "#;

        let resultado = standardize(input);

        let esperado = vec![
            "SELECT", "name", ",", "age", "FROM", "users", "WHERE", "age", ">",  // Operador >
            "25", // El punto y coma se mantiene al final
        ];

        assert_eq!(resultado, esperado);
    }

    #[test]
    fn test_standardize_cassandra_query() {
        let input = r#"
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

        let resultado = standardize(input);

        let esperado = vec![
            "SELECT", "name", ",", "age", "FROM", "users", "WHERE", "age",
            "_GE_", // Para el operador >=
            "30", "AND", "age", "=", "age", "+", // Para el operador +
            "2", "LIMIT", "10",
        ];

        assert_eq!(resultado, esperado);
    }
}
