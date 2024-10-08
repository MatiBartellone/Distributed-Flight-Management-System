use super::data_type::{string_to_data_type, DataType};
use super::terms::{string_to_term, Term};
use super::words_reserved::WordsReserved;
use crate::utils::errors::Errors;

#[derive(Debug, PartialEq)]
pub enum Token {
    Identifier(String),
    Term(Term),
    Reserved(String),
    DataType(DataType),
    TokensList(Vec<Token>),
}

fn string_to_identifier(word: &str) -> Option<Token> {
    if word.starts_with('"') && word.ends_with('"') {
        let inner = &word[1..word.len() - 1];
        return Some(Token::Identifier(inner.to_string()));
    }

    if let Some(first_char) = word.chars().next() {
        if !(first_char.is_alphabetic() || first_char == '_') {
            return None;
        }
        for c in word.chars().skip(1) {
            if !(c.is_alphanumeric()) {
                return None;
            }
        }
        return Some(Token::Identifier(word.to_string()));
    }
    None
}

fn match_tokenize(palabra: String) -> Option<Token> {
    let reservadas = WordsReserved::new();
    if let Some(token) = string_to_term(&palabra) {
        return Some(token);
    } else if reservadas.is_reserved(&palabra) {
        return Some(Token::Reserved(palabra.to_ascii_uppercase()));
    } else if let Some(token) = string_to_data_type(&palabra) {
        return Some(token);
    } else if let Some(token) = string_to_identifier(&palabra) {
        return Some(token);
    }
    None
}

fn init_sub_list_token(
    palabras: &[String],
    i: &mut usize,
    res: &mut Vec<Token>,
) -> Result<bool, Errors> {
    if let Some(Token::Reserved(reserv)) = res.last() {
        if reserv == "WHERE" {
            let temp = tokenize_recursive(palabras, close_sub_list_where, i)?;
            //*i += len_lista_anidada(&temp); //no sé si debe haber un +1?
            res.push(Token::TokensList(temp));
            return Ok(true);
        }
    } else if &palabras[*i] == "(" {
        *i += 1;
        let temp = tokenize_recursive(palabras, close_sub_list_parentesis, i)?;
        *i += 1;
         //no sé si debe haber un +1?
        res.push(Token::TokensList(temp));
        return Ok(true);
    }
    Ok(false)
}

fn close_sub_list_parentesis(word: &str) -> bool {
    word == ")"
}

fn close_sub_list_where(word: &str) -> bool {
    let reservadas = WordsReserved::new();
    let word_upper = word.to_ascii_uppercase();
    reservadas.is_reserved(&word_upper)
        && !(word_upper == "AND" || word_upper == "OR" || word_upper == "NOT")
}

fn tokenize_recursive<F>(palabras: &[String], cierre: F, i: &mut usize) -> Result<Vec<Token>, Errors>
where
    F: Fn(&str) -> bool,
{
    let mut res = Vec::new();
    while *i < palabras.len() {
        let palabra = &palabras[*i];
        if init_sub_list_token(palabras, i, &mut res)? {
            continue;
        } 
        if cierre(palabra) {
            return Ok(res);
        } 
        if let Some(token) = match_tokenize(palabra.to_string()) {
            res.push(token)
        } else {
            return Err(Errors::SyntaxError(format!(
                "Hay Palabras Invalidas; palabra '{}' '{}'",
                palabra, i
            )));
        }
        *i += 1;
    }
    Ok(res)
}

pub fn tokenize(palabras: Vec<String>) -> Result<Vec<Token>, Errors> {
    // Definimos una closure que siempre devuelve false
    let siempre_false = |_: &str| false;
    tokenize_recursive(&palabras, siempre_false, &mut 0)
}

#[cfg(test)]
mod tests {
    use crate::parsers::tokens::{
        literal::Literal,
        terms::{BooleanOperations, ComparisonOperators, LogicalOperators},
    };

    use super::*; // Asegúrate de que estás importando el módulo donde se define tokenize y otras funciones relevantes.

    #[test]
    fn test_tokenize_simple_select() {
        let query = vec!["SELECT", "name", "FROM", "users"]
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<String>>();
        let result = tokenize(query).unwrap();
        let expected = vec![
            Token::Reserved("SELECT".to_string()),
            Token::Identifier("name".to_string()),
            Token::Reserved("FROM".to_string()),
            Token::Identifier("users".to_string()),
        ];
        assert_eq!(result, expected);
    }

    #[test]
    fn test_tokenize_where_clause() {
        let query = vec![
            "SELECT", "name", "FROM", "users", "WHERE", "age", ">", "30", "ORDER", "BY", "name",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<String>>();
        let result = tokenize(query).unwrap();
        let literal = Literal::new("30".to_string(), DataType::Bigint);
        let expected = vec![
            Token::Reserved("SELECT".to_string()),
            Token::Identifier("name".to_string()),
            Token::Reserved("FROM".to_string()),
            Token::Identifier("users".to_string()),
            Token::Reserved("WHERE".to_string()),
            Token::TokensList(vec![
                Token::Identifier("age".to_string()),
                Token::Term(Term::BooleanOperations(BooleanOperations::Comparison(
                    ComparisonOperators::Greater,
                ))), // Asegúrate de tener un enumerador Term para el operador '>'
                Token::Term(Term::Literal(literal)),
            ]),
            Token::Reserved("ORDER".to_string()),
            Token::Reserved("BY".to_string()),
            Token::Identifier("name".to_string()),
        ];
        assert_eq!(result, expected);
    }

    #[test]
    fn test_tokenize_where_clause2() {
        let query = vec![
            "SELECT", "name", "FROM", "users", "WHERE", "(","age", ">", "30", ")", "ORDER", "BY", "name",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<String>>();
        let result = tokenize(query).unwrap();
        let literal = Literal::new("30".to_string(), DataType::Bigint);
        let expected = vec![
            Token::Reserved("SELECT".to_string()),
            Token::Identifier("name".to_string()),
            Token::Reserved("FROM".to_string()),
            Token::Identifier("users".to_string()),
            Token::Reserved("WHERE".to_string()),
            Token::TokensList(vec![
                Token::TokensList(vec![
                    Token::Identifier("age".to_string()),
                    Token::Term(Term::BooleanOperations(BooleanOperations::Comparison(
                        ComparisonOperators::Greater,
                    ))), // Asegúrate de tener un enumerador Term para el operador '>'
                    Token::Term(Term::Literal(literal)),
                ]),
            ]),
            Token::Reserved("ORDER".to_string()),
            Token::Reserved("BY".to_string()),
            Token::Identifier("name".to_string()),
        ];
        assert_eq!(result, expected);
    }

    #[test]
    fn test_tokenize_where_clause3() {
        let query = vec![
            "SELECT", "name", "FROM", "users", "WHERE", "(","(","age", ">", "30", ")",")", "ORDER", "BY", "name",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<String>>();
        let result = tokenize(query).unwrap();
        let literal = Literal::new("30".to_string(), DataType::Bigint);
        let expected = vec![
            Token::Reserved("SELECT".to_string()),
            Token::Identifier("name".to_string()),
            Token::Reserved("FROM".to_string()),
            Token::Identifier("users".to_string()),
            Token::Reserved("WHERE".to_string()),
            Token::TokensList(vec![
                Token::TokensList(vec![
                    Token::TokensList(vec![
                        Token::Identifier("age".to_string()),
                        Token::Term(Term::BooleanOperations(BooleanOperations::Comparison(
                            ComparisonOperators::Greater,
                        ))), // Asegúrate de tener un enumerador Term para el operador '>'
                        Token::Term(Term::Literal(literal)),
                    ]),
                ]),
            ]),
            Token::Reserved("ORDER".to_string()),
            Token::Reserved("BY".to_string()),
            Token::Identifier("name".to_string()),
        ];
        assert_eq!(result, expected);
    }

    #[test]
    fn test_tokenize_with_parentheses() {
        let query = vec![
            "SELECT", "name", "FROM", "users", "WHERE", "age", ">", "30", "AND", "(", "active",
            "=", "true", ")", "OR", "name", "=", "'ivan'", "ORDER",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<String>>();

        let result = tokenize(query).unwrap();

        let literal_bigint = Literal::new("30".to_string(), DataType::Bigint);
        let literal_boolean = Literal::new("true".to_string(), DataType::Boolean);
        let literal_text = Literal::new("ivan".to_string(), DataType::Text);
        let expected = vec![
            Token::Reserved("SELECT".to_string()),
            Token::Identifier("name".to_string()),
            Token::Reserved("FROM".to_string()),
            Token::Identifier("users".to_string()),
            Token::Reserved("WHERE".to_string()),
            Token::TokensList(vec![
                Token::Identifier("age".to_string()),
                Token::Term(Term::BooleanOperations(BooleanOperations::Comparison(
                    ComparisonOperators::Greater,
                ))), // '>' como comparación
                Token::Term(Term::Literal(literal_bigint)), // Literal para "30"
                Token::Term(Term::BooleanOperations(BooleanOperations::Logical(
                    LogicalOperators::And,
                ))), // 'AND' como operación lógica
                Token::TokensList(vec![
                    Token::Identifier("active".to_string()),
                    Token::Term(Term::BooleanOperations(BooleanOperations::Comparison(
                        ComparisonOperators::Equal,
                    ))), // '=' como comparación
                    Token::Term(Term::Literal(literal_boolean)), // Literal para "true"
                ]),
                Token::Term(Term::BooleanOperations(BooleanOperations::Logical(
                    LogicalOperators::Or,
                ))),
                Token::Identifier("name".to_string()),
                Token::Term(Term::BooleanOperations(BooleanOperations::Comparison(
                    ComparisonOperators::Equal,
                ))),
                Token::Term(Term::Literal(literal_text)),
            ]),
            Token::Reserved("ORDER".to_string()),
        ];

        assert_eq!(result, expected);
    }

    #[test]
    fn test_tokenize_with_parentheses2() {
        let query = vec![
            "SELECT", "name", "FROM", "users", "WHERE", "age", ">", "30", "AND", "(", "active",
            "=", "true", ")", "ORDER"
        ]
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<String>>();

        let result = tokenize(query).unwrap();

        let literal_bigint = Literal::new("30".to_string(), DataType::Bigint);
        let literal_boolean = Literal::new("true".to_string(), DataType::Boolean);

        let expected = vec![
            Token::Reserved("SELECT".to_string()),
            Token::Identifier("name".to_string()),
            Token::Reserved("FROM".to_string()),
            Token::Identifier("users".to_string()),
            Token::Reserved("WHERE".to_string()),
            Token::TokensList(vec![
                Token::Identifier("age".to_string()),
                Token::Term(Term::BooleanOperations(BooleanOperations::Comparison(
                    ComparisonOperators::Greater,
                ))), // '>' como comparación
                Token::Term(Term::Literal(literal_bigint)), // Literal para "30"
                Token::Term(Term::BooleanOperations(BooleanOperations::Logical(
                    LogicalOperators::And,
                ))), // 'AND' como operación lógica
                Token::TokensList(vec![
                    Token::Identifier("active".to_string()),
                    Token::Term(Term::BooleanOperations(BooleanOperations::Comparison(
                        ComparisonOperators::Equal,
                    ))), // '=' como comparación
                    Token::Term(Term::Literal(literal_boolean)), // Literal para "true"
                ]),
            ]),
            Token::Reserved("ORDER".to_string()),
        ];

        assert_eq!(result, expected);
    }

    

    #[test]
    fn test_tokenize_invalid_query() {
        let query = vec![
            "SELECT", "name", "FROM", "users", "WHERE", "age", "???", // Un token inválido
        ]
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<String>>();

        let result = tokenize(query);
        assert!(result.is_err()); // Esperamos que retorne un error
    }

    #[test]
    fn test_tokenize_insert_with_parentheses() {
        let query = vec![
            "INSERT",
            "INTO",
            "peliculas",
            "(",
            "id",
            ",",
            "titulo",
            ",",
            "año",
            ",",
            "genero",
            ")",
            "VALUES",
            "(",
            "'1'",
            ",",
            "'El Padrino'",
            ",",
            "1972",
            ",",
            "'Drama'",
            ")",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<String>>();

        let result = tokenize(query).unwrap();

        let literal_bigint = Literal::new("1972".to_string(), DataType::Bigint);
        let literal_string_id = Literal::new("1".to_string(), DataType::Text); // Literal para el ID
        let literal_string_title = Literal::new("El Padrino".to_string(), DataType::Text);
        let literal_string_genre = Literal::new("Drama".to_string(), DataType::Text);

        let expected = vec![
            Token::Reserved("INSERT".to_string()),
            Token::Reserved("INTO".to_string()),
            Token::Identifier("peliculas".to_string()),
            Token::TokensList(vec![
                // Columnas
                Token::Identifier("id".to_string()),
                Token::Identifier("titulo".to_string()),
                Token::Identifier("año".to_string()),
                Token::Identifier("genero".to_string()),
            ]),
            Token::Reserved("VALUES".to_string()),
            Token::TokensList(vec![
                // Valores
                Token::Term(Term::Literal(literal_string_id)), // Literal para "1"
                Token::Term(Term::Literal(literal_string_title)), // Literal para "El Padrino"
                Token::Term(Term::Literal(literal_bigint)),    // Literal para "1972"
                Token::Term(Term::Literal(literal_string_genre)), // Literal para "Drama"
            ]),
        ];

        assert_eq!(result, expected);
    }

    #[test]
    fn test_tokenize_with_nested_parentheses() {
        let query = vec![
            "SELECT", "WHERE", "(", "age", "AND", "(", "active", "OR", 
            "(", "premium", "AND", "(", "location", ")", ")", ")", "AND", "status", ")"
        ]
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<String>>();

        let result = tokenize(query).unwrap();
        let expected = vec![
            Token::Reserved("SELECT".to_string()),
            Token::Reserved("WHERE".to_string()),
            Token::TokensList(vec![
                Token::TokensList(vec![
                    Token::Identifier("age".to_string()),
                    Token::Term(Term::BooleanOperations(BooleanOperations::Logical(
                        LogicalOperators::And,
                    ))), // 'AND' como operación lógica
                    Token::TokensList(vec![
                        Token::Identifier("active".to_string()),
                        Token::Term(Term::BooleanOperations(BooleanOperations::Logical(
                            LogicalOperators::Or,
                        ))), // 'OR' como operación lógica
                        Token::TokensList(vec![
                            Token::Identifier("premium".to_string()),
                            Token::Term(Term::BooleanOperations(BooleanOperations::Logical(
                                LogicalOperators::And,
                            ))), // 'AND' como operación lógica
                            Token::TokensList(vec![
                                Token::Identifier("location".to_string()),
                            ]),
                        ]),
                    ]),
                    Token::Term(Term::BooleanOperations(BooleanOperations::Logical(
                        LogicalOperators::And,
                    ))), 
                    Token::Identifier("status".to_string()),
                ]),
                
            ]),
        ];

        assert_eq!(result, expected);
    }

}
