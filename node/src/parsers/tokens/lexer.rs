use std::collections::HashSet;

use crate::utils::constants::*;

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

//me separa todas las partes que esten entre $, ' o ""
//hola $hola como estas$ "bien" 'vos' el resto de el string
//queda como ["hola ", "$hola como estas$", " "bien" ", "'vos' el resto de el string"]
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

//Elimina todos los comments de la query
fn remove_comments(input: &str) -> String {
    let without_diagonal = remove_between(input, "\n", "//");
    let without_bar = remove_between(&without_diagonal, "\n", "--");
    remove_between(&without_bar, "*/", "/*")
}


fn replace_double(final_query: &mut String, double_op: &str) {
    let mut replace = "";
    match double_op {
        ">=" => replace = GE,
        "<=" => replace = LE,
        "!=" => replace = DF,
        _ => {}
    }
    final_query.push_str(replace)
}

fn replace_simple(final_query: &mut String, simple_op: &char) {
    let replace = format!(" {} ", simple_op);
    final_query.push_str(&replace)
}

fn divide_double(query: &str) -> String { 
    let doble_chars_set: HashSet<String> = [">=", "<=", "!="].iter().map(|&s| s.to_string()).collect();
    let mut iter = query.chars().peekable();
    let mut final_query = String::new();
    while let Some(elem) = iter.next() {
        if let Some(next_char) = iter.peek(){
            let possible_double = format!("{}{}", elem, next_char);
            if doble_chars_set.contains(&possible_double) {
                replace_double(&mut final_query, &possible_double);
                iter.next();
            }
            else if elem == '-' {
                if !next_char.is_ascii_digit(){
                    replace_simple(&mut final_query, &elem)
                }
            }
            else {
                final_query.push(elem)
            }
        } else {
            final_query.push(elem)
        }
    }
    final_query
}

fn divide_simple(query: &str) -> String {
    let chars_set: HashSet<char> = ['+', '/', '%', '<', '>', '(', ')', '}', '{', ','].into_iter().collect();
    let filter_set: HashSet<char> = ['\n', '\t'].into_iter().collect();
    let iter = query.chars().peekable();
    let mut final_query = String::new();
    for elem in iter {
        if chars_set.contains(&elem) {
            replace_simple(&mut final_query, &elem)
        }
        else if filter_set.contains(&elem) {
            final_query.push(' ')
        }
        else if elem == ';' {
            continue;
        }
        else {
            final_query.push(elem)
        }
    }
    final_query
}


fn divide_words(query: &str) -> Vec<String> {
    let mid_query = divide_double(query);
    let final_query = divide_simple(&mid_query);
    final_query.split_whitespace().map(|s| s.to_string()).collect()
}

fn is_section(word: &str) -> bool {
    matches!(word.chars().next(), Some('$' | '\'' | '"'))
}

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
