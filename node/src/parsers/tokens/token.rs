use super::data_type::{string_to_data_type, DataType};
use super::reserved_words::WordsReserved;
use super::symbols::Symbols;
use super::terms::{string_to_term, Term};
use crate::utils::errors::Errors;
use crate::utils::parser_constants::{
    AND, ASC, BY, CLOSE_BRACE, CLOSE_PAREN, DESC, EXISTS, IF, NOT, OPEN_BRACE, OPEN_PAREN, OR,
    SELECT, SET, WHERE,
};


/// Enum que representa los distintos tipos de tokens que se pueden generar durante
/// la tokenización de una consulta CQL.
/// 
/// Los tokens están organizados en una estructura jerárquica, permitiendo representar
/// de manera eficiente la consulta para su análisis posterior.
/// 
/// # Tipos de tokens
/// - **Identifier(String):** Representa identificadores de objetos o entidades, como nombres de columnas,
///   tablas, keyspaces, etc. Los identificadores pueden estar entre comillas dobles para preservar su formato exacto.
/// - **Term(Term):** Representa literales, operadores aritméticos o booleanos utilizados en la consulta.
/// - **Reserved(String):** Palabras reservadas de CQL que no pueden ser usadas como identificadores, como `SELECT`,
///   `ORDER`, `PRIMARY`, etc.
/// - **DataType(DataType):** Representa los tipos de datos soportados por CQL, como `Int`, `Boolean`, `Text`, etc.
/// - **ParenList(Vec<Token>):** Agrupa elementos dentro de paréntesis, por ejemplo, las condiciones de una cláusula `WHERE`.
/// - **IterateToken(Vec<Token>):** Agrupa secciones de longitud variable, como columnas en un `SELECT` o valores en un `INSERT`.
/// - **BraceList(Vec<Token>):** Agrupa elementos dentro de llaves, como definiciones de tablas o keyspaces.
/// - **Symbol(String):** Representa símbolos específicos utilizados en la consulta, como `,`, `;`, `:`.
///
#[derive(Debug, PartialEq)]
pub enum Token {
    Identifier(String),
    Term(Term),
    Reserved(String),
    DataType(DataType),
    ParenList(Vec<Token>),
    IterateToken(Vec<Token>),
    BraceList(Vec<Token>),
    Symbol(String),
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
        let mut dot_count = 0;
        for c in word.chars().skip(1) {
            if !c.is_alphanumeric() && c == '.' {
                dot_count += 1;
                if dot_count > 1 {
                    return None;
                }
            }
            if !c.is_alphanumeric() && c != '.' && c != '_' {
                return None;
            }
        }
        return Some(Token::Identifier(word.to_ascii_lowercase()));
    }
    None
}

fn match_tokenize(word: String) -> Option<Token> {
    let reserved = WordsReserved::new();
    let symbols = Symbols::new();
    if let Some(token) = string_to_term(&word) {
        return Some(token);
    } else if reserved.is_reserved(&word) {
        return Some(Token::Reserved(word.to_ascii_uppercase()));
    } else if let Some(token) = string_to_data_type(&word) {
        return Some(token);
    } else if symbols.is_symbol(&word) {
        return Some(Token::Symbol(word));
    } else if let Some(token) = string_to_identifier(&word) {
        return Some(token);
    }
    None
}

fn sub_list_token(
    words: &[String],
    i: &mut usize,
    res: &mut Vec<Token>,
    reserved: String,
) -> Result<bool, Errors> {
    let temp;
    if reserved == WHERE {
        temp = tokenize_recursive(words, close_sub_list_where, i)?;
    } else if reserved == SELECT {
        temp = tokenize_recursive(words, close_sub_list_select, i)?;
    } else if reserved == BY {
        temp = tokenize_recursive(words, close_sub_list_order_by, i)?;
    } else if reserved == SET {
        temp = tokenize_recursive(words, close_sub_list_select, i)?;
    } else if reserved == IF {
        temp = tokenize_recursive(words, close_sub_list_if, i)?;
    } else {
        return Ok(false);
    }
    res.push(Token::IterateToken(temp));
    Ok(true)
}

fn init_sub_list_token(
    words: &[String],
    i: &mut usize,
    res: &mut Vec<Token>,
) -> Result<bool, Errors> {
    //Nos fijamos que fue lo ultimo YA LEIDO
    if let Some(Token::Reserved(reserved)) = res.last() {
        if sub_list_token(words, i, res, reserved.to_string())? {
            return Ok(true);
        }
    } //Nos fijamos la word ACTUAL
    if words[*i] == OPEN_PAREN {
        *i += 1;
        let temp = tokenize_recursive(words, close_sub_list_parentheses, i)?;
        *i += 1;
        res.push(Token::ParenList(temp));
        return Ok(true);
    } else if words[*i] == OPEN_BRACE {
        *i += 1;
        let temp = tokenize_recursive(words, close_sub_list_key_icon, i)?;
        *i += 1;
        res.push(Token::BraceList(temp));
        return Ok(true);
    }
    Ok(false)
}

fn close_sub_list_order_by(word: &str) -> bool {
    let reserved = WordsReserved::new();
    let word_upper = word.to_ascii_uppercase();
    reserved.is_reserved(&word_upper) && !(word_upper == ASC || word_upper == DESC)
}

fn close_sub_list_key_icon(word: &str) -> bool {
    word == CLOSE_BRACE
}

fn close_sub_list_select(word: &str) -> bool {
    let reserved = WordsReserved::new();
    let word_upper = word.to_ascii_uppercase();
    reserved.is_reserved(&word_upper)
}

fn close_sub_list_parentheses(word: &str) -> bool {
    word == CLOSE_PAREN
}

fn close_sub_list_where(word: &str) -> bool {
    let reserved = WordsReserved::new();
    let word_upper = word.to_ascii_uppercase();
    reserved.is_reserved(&word_upper)
        && !(word_upper == AND || word_upper == OR || word_upper == NOT)
}

fn close_sub_list_if(word: &str) -> bool {
    let reserved = WordsReserved::new();
    let word_upper = word.to_ascii_uppercase();
    reserved.is_reserved(&word_upper)
        && !(word_upper == AND || word_upper == OR || word_upper == NOT || word_upper == EXISTS)
}


/// Tokenización recursiva.
///
/// Esta función es la base del proceso de tokenización y permite manejar
/// estructuras anidadas como sublistas o bloques delimitados.
/// 
/// # Parámetros
/// - `words`: Vector de palabras de entrada.
/// - `closure`: Función que determina la condición de cierre de una sublista.
/// - `i`: Índice mutable para rastrear la posición actual en el vector de palabras.
///
/// # Retorna
/// Un `Result` con un vector de tokens o un error de tipo `Errors`.
fn tokenize_recursive<F>(words: &[String], closure: F, i: &mut usize) -> Result<Vec<Token>, Errors>
where
    F: Fn(&str) -> bool,
{
    let mut res = Vec::new();
    while *i < words.len() {
        let word = &words[*i];
        if init_sub_list_token(words, i, &mut res)? {
            continue;
        }
        if closure(word) {
            return Ok(res);
        }
        if let Some(token) = match_tokenize(word.to_string()) {
            res.push(token)
        } else {
            return Err(Errors::SyntaxError(format!(
                "Invalid words; word '{}' '{}'",
                word, i
            )));
        }
        *i += 1;
    }
    Ok(res)
}


/// Tokenizador de consultas CQL.
///
/// Este módulo convierte una consulta en texto en una secuencia de tokens, siguiendo
/// reglas predefinidas para clasificar cada palabra o símbolo.
///
/// # Funcionamiento
/// La tokenización sigue los siguientes pasos:
/// 1. Clasificar palabras y símbolos en tokens utilizando reglas predefinidas.
/// 2. Identificar y agrupar sublistas basadas en paréntesis, llaves o palabras reservadas.
/// 3. Validar que todos los elementos cumplen con las reglas sintácticas de CQL.
///
/// Si se encuentra un error durante el proceso, el sistema devuelve un `Errors::SyntaxError`
/// con detalles sobre la naturaleza y ubicación del problema.
///
/// # Ejemplo de uso
/// ```rust
/// let consulta = vec![
///     "SELECT".to_string(),
///     "*".to_string(),
///     "FROM".to_string(),
///     "tabla".to_string(),
///     "WHERE".to_string(),
///     "id".to_string(),
///     "=".to_string(),
///     "42".to_string(),
/// ];
/// 
/// let resultado = tokenize(consulta);
/// assert!(resultado.is_ok());
/// ```
///
/// # Manejo de errores
/// Si un literal o palabra es inválido, el sistema devuelve un error detallado:
/// ```rust
/// Err(Errors::SyntaxError("Invalid word: 'token'".to_string()));
/// ```
pub fn tokenize(words: Vec<String>) -> Result<Vec<Token>, Errors> {
    // Definimos una closure que siempre devuelve false
    let fn_false = |_: &str| false;
    tokenize_recursive(&words, fn_false, &mut 0)
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
        let query = ["SELECT", "name", "FROM", "users"]
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<String>>();
        let result = tokenize(query).unwrap();
        let expected = vec![
            Token::Reserved("SELECT".to_string()),
            Token::IterateToken(vec![Token::Identifier("name".to_string())]),
            Token::Reserved("FROM".to_string()),
            Token::Identifier("users".to_string()),
        ];
        assert_eq!(result, expected);
    }

    #[test]
    fn test_tokenize_where_clause() {
        let query = vec![
            "SELECT", "name", "dni", "FROM", "users", "WHERE", "age", ">", "30", "ORDER", "BY",
            "name", "dni",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<String>>();
        let result = tokenize(query).unwrap();

        let expected = vec![
            Token::Reserved("SELECT".to_string()),
            Token::IterateToken(vec![
                Token::Identifier("name".to_string()),
                Token::Identifier("dni".to_string()),
            ]),
            Token::Reserved("FROM".to_string()),
            Token::Identifier("users".to_string()),
            Token::Reserved("WHERE".to_string()),
            get_age_greater_than_30(false),
            Token::Reserved("ORDER".to_string()),
            Token::Reserved("BY".to_string()),
            Token::IterateToken(vec![
                Token::Identifier("name".to_string()),
                Token::Identifier("dni".to_string()),
            ]),
        ];
        assert_eq!(result, expected);
    }

    fn get_age_greater_than_30(paren_list: bool) -> Token {
        let literal = Literal::new("30".to_string(), DataType::Int);
        if paren_list {
            return Token::ParenList(vec![
                Token::Identifier("age".to_string()),
                Token::Term(Term::BooleanOperations(BooleanOperations::Comparison(
                    ComparisonOperators::Greater,
                ))),
                Token::Term(Term::Literal(literal)),
            ]);
        }
        Token::IterateToken(vec![
            Token::Identifier("age".to_string()),
            Token::Term(Term::BooleanOperations(BooleanOperations::Comparison(
                ComparisonOperators::Greater,
            ))),
            Token::Term(Term::Literal(literal)),
        ])
    }

    #[test]
    fn test_tokenize_where_clause_with_parentheses() {
        let query = vec![
            "SELECT", "name", "FROM", "users", "WHERE", "(", "age", ">", "30", ")", "ORDER", "BY",
            "name", "dni",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<String>>();
        let result = tokenize(query).unwrap();
        let expected = vec![
            Token::Reserved("SELECT".to_string()),
            Token::IterateToken(vec![Token::Identifier("name".to_string())]),
            Token::Reserved("FROM".to_string()),
            Token::Identifier("users".to_string()),
            Token::Reserved("WHERE".to_string()),
            Token::IterateToken(vec![get_age_greater_than_30(true)]),
            Token::Reserved("ORDER".to_string()),
            Token::Reserved("BY".to_string()),
            Token::IterateToken(vec![
                Token::Identifier("name".to_string()),
                Token::Identifier("dni".to_string()),
            ]),
        ];
        assert_eq!(result, expected);
    }

    #[test]
    fn test_tokenize_where_clause_hard() {
        let query = vec![
            "SELECT", "name", "FROM", "users", "WHERE", "(", "(", "age", ">", "30", ")", ")",
            "ORDER", "BY", "name",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<String>>();
        let result = tokenize(query).unwrap();
        let expected = vec![
            Token::Reserved("SELECT".to_string()),
            Token::IterateToken(vec![Token::Identifier("name".to_string())]),
            Token::Reserved("FROM".to_string()),
            Token::Identifier("users".to_string()),
            Token::Reserved("WHERE".to_string()),
            Token::IterateToken(vec![Token::ParenList(vec![get_age_greater_than_30(true)])]),
            Token::Reserved("ORDER".to_string()),
            Token::Reserved("BY".to_string()),
            Token::IterateToken(vec![Token::Identifier("name".to_string())]),
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

        let literal_bigint = Literal::new("30".to_string(), DataType::Int);
        let literal_text = Literal::new("ivan".to_string(), DataType::Text);
        let expected = vec![
            Token::Reserved("SELECT".to_string()),
            Token::IterateToken(vec![Token::Identifier("name".to_string())]),
            Token::Reserved("FROM".to_string()),
            Token::Identifier("users".to_string()),
            Token::Reserved("WHERE".to_string()),
            Token::IterateToken(vec![
                Token::Identifier("age".to_string()),
                Token::Term(Term::BooleanOperations(BooleanOperations::Comparison(
                    ComparisonOperators::Greater,
                ))), // '>' como comparación
                Token::Term(Term::Literal(literal_bigint)), // Literal para "30"
                Token::Term(Term::BooleanOperations(BooleanOperations::Logical(
                    LogicalOperators::And,
                ))), // 'AND' como operación lógica
                get_active_equals_true(),
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

    fn get_active_equals_true() -> Token {
        let literal_boolean = Literal::new("true".to_string(), DataType::Boolean);
        Token::ParenList(vec![
            Token::Identifier("active".to_string()),
            Token::Term(Term::BooleanOperations(BooleanOperations::Comparison(
                ComparisonOperators::Equal,
            ))), // '=' como comparación
            Token::Term(Term::Literal(literal_boolean)), // Literal para "true"
        ])
    }

    #[test]
    fn test_tokenize_with_twice_parentheses() {
        let query = vec![
            "SELECT", "name", "FROM", "users", "WHERE", "age", ">", "30", "AND", "(", "active",
            "=", "true", ")", "ORDER",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<String>>();

        let result = tokenize(query).unwrap();
        let literal_bigint = Literal::new("30".to_string(), DataType::Int);
        let expected = vec![
            Token::Reserved("SELECT".to_string()),
            Token::IterateToken(vec![Token::Identifier("name".to_string())]),
            Token::Reserved("FROM".to_string()),
            Token::Identifier("users".to_string()),
            Token::Reserved("WHERE".to_string()),
            Token::IterateToken(vec![
                Token::Identifier("age".to_string()),
                Token::Term(Term::BooleanOperations(BooleanOperations::Comparison(
                    ComparisonOperators::Greater,
                ))), // '>' como comparación
                Token::Term(Term::Literal(literal_bigint)), // Literal para "30"
                Token::Term(Term::BooleanOperations(BooleanOperations::Logical(
                    LogicalOperators::And,
                ))), // 'AND' como operación lógica
                get_active_equals_true(),
            ]),
            Token::Reserved("ORDER".to_string()),
        ];

        assert_eq!(result, expected);
    }

    #[test]
    fn test_tokenize_invalid_query() {
        let query = [
            "SELECT", "name", "FROM", "users", "WHERE", "age", "???", // Un token inválido
        ]
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<String>>();

        let result = tokenize(query);
        assert!(result.is_err()); // Esperamos que retorne un error
    }

    #[test]
    fn test_tokenize_insert() {
        let query = vec![
            "INSERT",
            "INTO",
            "\"tAbla\"",
            "(",
            "columna1",
            ",",
            "columna2",
            ")",
            "VALUES",
            "(",
            "'valor1'",
            ",",
            "'valor2'",
            ")",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<String>>();

        let literal_1 = Literal::new("valor1".to_string(), DataType::Text);
        let literal_2 = Literal::new("valor2".to_string(), DataType::Text);

        let expected_tokens = vec![
            Token::Reserved("INSERT".to_string()),
            Token::Reserved("INTO".to_string()),
            Token::Identifier("tAbla".to_string()),
            Token::ParenList(vec![
                Token::Identifier("columna1".to_string()),
                Token::Symbol(",".to_string()),
                Token::Identifier("columna2".to_string()),
            ]),
            Token::Reserved("VALUES".to_string()),
            Token::ParenList(vec![
                Token::Term(Term::Literal(literal_1)),
                Token::Symbol(",".to_string()),
                Token::Term(Term::Literal(literal_2)),
            ]),
        ];

        let result = tokenize(query).unwrap();
        assert_eq!(result, expected_tokens);
    }

    #[test]
    fn test_tokenize_with_nested_parentheses() {
        let query = vec![
            "SELECT", "WHERE", "(", "age", "AND", "(", "active", "OR", "(", "premium", "AND", "(",
            "location", ")", ")", ")", "AND", "status", ")",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<String>>();

        let result = tokenize(query).unwrap();
        let expected = vec![
            Token::Reserved("SELECT".to_string()),
            Token::IterateToken(vec![]),
            Token::Reserved("WHERE".to_string()),
            Token::IterateToken(vec![Token::ParenList(vec![
                Token::Identifier("age".to_string()),
                Token::Term(Term::BooleanOperations(BooleanOperations::Logical(
                    LogicalOperators::And,
                ))), // 'AND' como operación lógica
                Token::ParenList(vec![
                    Token::Identifier("active".to_string()),
                    Token::Term(Term::BooleanOperations(BooleanOperations::Logical(
                        LogicalOperators::Or,
                    ))), // 'OR' como operación lógica
                    Token::ParenList(vec![
                        Token::Identifier("premium".to_string()),
                        Token::Term(Term::BooleanOperations(BooleanOperations::Logical(
                            LogicalOperators::And,
                        ))), // 'AND' como operación lógica
                        Token::ParenList(vec![Token::Identifier("location".to_string())]),
                    ]),
                ]),
                Token::Term(Term::BooleanOperations(BooleanOperations::Logical(
                    LogicalOperators::And,
                ))),
                Token::Identifier("status".to_string()),
            ])]),
        ];

        assert_eq!(result, expected);
    }

    #[test]
    fn test_tokenize_update() {
        let query = [
            "UPDATE",
            "\"tAbla\"",
            "SET",
            "columna1",
            "=",
            "'nuevo_valor'",
            "WHERE",
            "id",
            ">",
            "10",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<String>>();

        let literal = Literal::new("nuevo_valor".to_string(), DataType::Text);
        let expected_tokens = vec![
            Token::Reserved("UPDATE".to_string()),
            Token::Identifier("tAbla".to_string()),
            Token::Reserved("SET".to_string()),
            Token::IterateToken(vec![
                Token::Identifier("columna1".to_string()),
                Token::Term(Term::BooleanOperations(BooleanOperations::Comparison(
                    ComparisonOperators::Equal,
                ))),
                Token::Term(Term::Literal(literal)),
            ]),
            Token::Reserved("WHERE".to_string()),
            Token::IterateToken(vec![
                Token::Identifier("id".to_string()),
                Token::Term(Term::BooleanOperations(BooleanOperations::Comparison(
                    ComparisonOperators::Greater,
                ))),
                Token::Term(Term::Literal(Literal::new("10".to_string(), DataType::Int))),
            ]),
        ];

        let result = tokenize(query).unwrap();
        assert_eq!(result, expected_tokens);
    }

    #[test]
    fn test_tokenize_delete() {
        let query = ["DELETE", "FROM", "\"tAbla\"", "WHERE", "id", "=", "5"]
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<String>>();

        let expected_tokens = vec![
            Token::Reserved("DELETE".to_string()),
            Token::Reserved("FROM".to_string()),
            Token::Identifier("tAbla".to_string()),
            Token::Reserved("WHERE".to_string()),
            Token::IterateToken(vec![
                Token::Identifier("id".to_string()),
                Token::Term(Term::BooleanOperations(BooleanOperations::Comparison(
                    ComparisonOperators::Equal,
                ))),
                Token::Term(Term::Literal(Literal::new("5".to_string(), DataType::Int))),
            ]),
        ];

        let result = tokenize(query).unwrap();
        assert_eq!(result, expected_tokens);
    }

    #[test]
    fn test_tokenize_create_table() {
        let query = vec![
            "CREATE",
            "TABLE",
            "\"tAbla\"",
            "(",
            "\"columna1\"",
            "TEXT",
            ",",
            "\"columna2\"",
            "INT",
            ",",
            "\"columna3\"",
            "PRIMARY",
            "KEY",
            "(",
            "\"columna1\"",
            ")",
            ")",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<String>>();

        let expected_tokens = vec![
            Token::Reserved("CREATE".to_string()),
            Token::Reserved("TABLE".to_string()),
            Token::Identifier("tAbla".to_string()),
            Token::ParenList(vec![
                Token::Identifier("columna1".to_string()),
                Token::DataType(DataType::Text),
                Token::Symbol(",".to_string()),
                Token::Identifier("columna2".to_string()),
                Token::DataType(DataType::Int),
                Token::Symbol(",".to_string()),
                Token::Identifier("columna3".to_string()),
                Token::Reserved("PRIMARY".to_string()),
                Token::Reserved("KEY".to_string()),
                Token::ParenList(vec![Token::Identifier("columna1".to_string())]),
            ]),
        ];

        let result = tokenize(query).unwrap();
        assert_eq!(result, expected_tokens);
    }

    #[test]
    fn test_tokenize_create_keyspace() {
        let query = vec![
            "CREATE",
            "KEYSPACE",
            "\"miKeyspace\"",
            "WITH",
            "REPLICATION",
            "=",
            "{",
            "'class'",
            ":",
            "'SimpleStrategy'",
            ",",
            "'replication_factor'",
            ":",
            "1",
            "}",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<String>>();

        let expected_tokens = vec![
            Token::Reserved("CREATE".to_string()),
            Token::Reserved("KEYSPACE".to_string()),
            Token::Identifier("miKeyspace".to_string()),
            Token::Reserved("WITH".to_string()),
            Token::Reserved("REPLICATION".to_string()),
            Token::Term(Term::BooleanOperations(BooleanOperations::Comparison(
                ComparisonOperators::Equal,
            ))),
            get_replication_options(),
        ];

        let result = tokenize(query).unwrap();
        assert_eq!(result, expected_tokens);
    }

    #[test]
    fn test_tokenize_drop_table() {
        let query = ["DROP", "TABLE", "\"users\""]
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<String>>();

        let expected_tokens = vec![
            Token::Reserved("DROP".to_string()),
            Token::Reserved("TABLE".to_string()),
            Token::Identifier("users".to_string()), // Asegúrate de manejar las comillas adecuadamente en tu implementación
        ];

        let result = tokenize(query).unwrap();
        assert_eq!(result, expected_tokens);
    }

    #[test]
    fn test_tokenize_drop_keyspace() {
        let query = ["DROP", "KEYSPACE", "\"my_Keyspace\""]
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<String>>();

        let expected_tokens = vec![
            Token::Reserved("DROP".to_string()),
            Token::Reserved("KEYSPACE".to_string()),
            Token::Identifier("my_Keyspace".to_string()), // Igualmente, asegúrate de manejar las comillas adecuadamente
        ];

        let result = tokenize(query).unwrap();
        assert_eq!(result, expected_tokens);
    }

    #[test]
    fn test_tokenize_alter_table() {
        let query = ["ALTER", "TABLE", "\"users\"", "ADD", "\"age\"", "INT"]
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<String>>();

        let expected_tokens = vec![
            Token::Reserved("ALTER".to_string()),
            Token::Reserved("TABLE".to_string()),
            Token::Identifier("users".to_string()), // Manejo de comillas
            Token::Reserved("ADD".to_string()),
            Token::Identifier("age".to_string()), // Manejo de comillas
            Token::DataType(DataType::Int) // Tipo de dato
        ];

        let result = tokenize(query).unwrap();
        assert_eq!(result, expected_tokens);
    }

    #[test]
    fn test_tokenize_alter_keyspace() {
        let query = vec![
            "ALTER",
            "KEYSPACE",
            "\"my_keyspace\"",
            "WITH",
            "REPLICATION",
            "=",
            "{",
            "'class'",
            ":",
            "'SimpleStrategy'",
            ",",
            "'replication_factor'",
            ":",
            "1",
            "}",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<String>>();

        let expected_tokens = vec![
            Token::Reserved("ALTER".to_string()),
            Token::Reserved("KEYSPACE".to_string()),
            Token::Identifier("my_keyspace".to_string()), // Manejo de comillas
            Token::Reserved("WITH".to_string()),
            Token::Reserved("REPLICATION".to_string()),
            Token::Term(Term::BooleanOperations(BooleanOperations::Comparison(
                ComparisonOperators::Equal,
            ))),
            get_replication_options(),
        ];

        let result = tokenize(query).unwrap();
        assert_eq!(result, expected_tokens);
    }

    fn get_replication_options() -> Token {
        Token::BraceList(vec![
            Token::Term(Term::Literal(Literal::new(
                "class".to_string(),
                DataType::Text,
            ))),
            Token::Symbol(":".to_string()),
            Token::Term(Term::Literal(Literal::new(
                "SimpleStrategy".to_string(),
                DataType::Text,
            ))),
            Token::Symbol(",".to_string()),
            Token::Term(Term::Literal(Literal::new(
                "replication_factor".to_string(),
                DataType::Text,
            ))),
            Token::Symbol(":".to_string()),
            Token::Term(Term::Literal(Literal::new("1".to_string(), DataType::Int))),
        ])
    }
}
