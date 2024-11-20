use crate::utils::constants::*;
use crate::{
    parsers::tokens::{
        terms::{ComparisonOperators, Term},
        token::Token,
    },
    queries::set_logic::assigmente_value::AssignmentValue,
    utils::{
        errors::Errors,
        token_conversor::{get_arithmetic_math, get_comparison_operator, get_next_value},
    },
};
use std::collections::HashMap;
use std::{iter::Peekable, vec::IntoIter};
use ComparisonOperators::*;
use Term::*;
use Token::*;

pub struct SetClauseParser;

impl SetClauseParser {
    pub fn parse(tokens: Vec<Token>) -> Result<HashMap<String, AssignmentValue>, Errors> {
        let mut changes = HashMap::new();
        values(&mut tokens.into_iter().peekable(), &mut changes)?;
        Ok(changes)
    }
}

fn values(
    tokens: &mut Peekable<IntoIter<Token>>,
    changes: &mut HashMap<String, AssignmentValue>,
) -> Result<(), Errors> {
    match tokens.next() {
        Some(Identifier(column_name)) => assignment(tokens, changes, column_name),
        _ if changes.is_empty() => {
            Err(Errors::SyntaxError("Invalid Sintax follow SET".to_string()))
        }
        _ => Ok(()),
    }
}

fn assignment(
    tokens: &mut Peekable<IntoIter<Token>>,
    changes: &mut HashMap<String, AssignmentValue>,
    column_name: String,
) -> Result<(), Errors> {
    let Ok(Equal) = get_comparison_operator(tokens) else {
        return Err(Errors::SyntaxError(
            "= should follow a SET assignment".to_string(),
        ));
    };
    match get_next_value(tokens)? {
        // [column_name, = , literal]
        Term(Literal(value)) => {
            changes.insert(column_name, AssignmentValue::Simple(value));
            check_comma(tokens, changes)
        }
        // [column_name, = , other_column, ...]
        Identifier(other_column) => column_asssigment(tokens, changes, column_name, other_column),
        _ => Err(Errors::SyntaxError("Invalid assigment".to_string())),
    }
}

fn column_asssigment(
    tokens: &mut Peekable<IntoIter<Token>>,
    changes: &mut HashMap<String, AssignmentValue>,
    column_name: String,
    other_column: String,
) -> Result<(), Errors> {
    match tokens.peek() {
        // [column_name, = , other_column, +|-, literal]
        Some(Term(ArithMath(_))) => {
            arithmetic_assigment(tokens, changes, column_name, other_column)
        }
        // [column_name, = , other_column]
        _ => {
            changes.insert(column_name, AssignmentValue::Column(other_column));
            check_comma(tokens, changes)
        }
    }
}

fn arithmetic_assigment(
    tokens: &mut Peekable<IntoIter<Token>>,
    changes: &mut HashMap<String, AssignmentValue>,
    column_name: String,
    other_column: String,
) -> Result<(), Errors> {
    let op = get_arithmetic_math(tokens)?;
    match get_next_value(tokens)? {
        Term(Literal(literal)) => {
            changes.insert(
                column_name,
                AssignmentValue::Arithmetic(other_column, op, literal),
            );
            check_comma(tokens, changes)
        }
        _ => Err(Errors::SyntaxError(
            "Expected a numeric literal after the arithmetic operator".to_string(),
        )),
    }
}

fn check_comma(
    tokens: &mut Peekable<IntoIter<Token>>,
    changes: &mut HashMap<String, AssignmentValue>,
) -> Result<(), Errors> {
    match get_next_value(tokens) {
        Ok(Symbol(s)) if s == *COMMA && tokens.peek().is_some() => values(tokens, changes),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use ArithMath::*;
    use AssignmentValue::*;
    use ComparisonOperators::*;
    use DataType::*;

    use crate::{
        parsers::{
            query_parsers::set_clause_parser::SetClauseParser,
            tokens::{
                data_type::DataType,
                literal::create_literal,
                terms::{ArithMath, ComparisonOperators},
                token::Token,
            },
        },
        queries::set_logic::assigmente_value::AssignmentValue,
        utils::token_conversor::{
            create_arith_math_token, create_comparison_operation_token, create_identifier_token,
            create_symbol_token, create_token_literal,
        },
    };

    use super::COMMA;

    fn test_successful_parser_case(caso: Vec<Token>, expected: HashMap<String, AssignmentValue>) {
        let resultado = SetClauseParser::parse(caso);
        match resultado {
            Ok(if_clause) => assert_eq!(if_clause, expected, "Resultado inesperado"),
            Err(e) => panic!("Parser devolvió un error inesperado: {}", e),
        }
    }

    fn test_parser_error_case(caso: Vec<Token>, mensaje_error_esperado: &str) {
        let resultado = SetClauseParser::parse(caso);
        match resultado {
            Ok(_) => panic!("Se esperaba un error"),
            Err(e) => assert!(
                e.to_string().contains(mensaje_error_esperado),
                "Se esperaba un error que contenga '{}', pero se obtuvo: '{}'",
                mensaje_error_esperado,
                e
            ),
        }
    }

    #[test]
    fn test_set_clause_parser_simple_assignment() {
        // age = 30;
        let tokens = vec![
            create_identifier_token("age"),
            create_comparison_operation_token(Equal),
            create_token_literal("30", Int),
        ];

        let mut expected_changes = HashMap::new();
        expected_changes.insert("age".to_string(), Simple(create_literal("30", Int)));

        test_successful_parser_case(tokens, expected_changes);
    }

    #[test]
    fn test_set_clause_parser_column_assignment() {
        // age = height
        let tokens = vec![
            create_identifier_token("age"),
            create_comparison_operation_token(Equal),
            create_identifier_token("height"),
        ];

        let mut expected_changes = HashMap::new();
        expected_changes.insert("age".to_string(), Column("height".to_string()));

        test_successful_parser_case(tokens, expected_changes);
    }

    #[test]
    fn test_set_clause_parser_arithmetic_assignment() {
        // age = height + 10
        let tokens = vec![
            create_identifier_token("age"),
            create_comparison_operation_token(Equal),
            create_identifier_token("height"),
            create_arith_math_token(Suma),
            create_token_literal("10", Int),
        ];

        let mut expected_changes = HashMap::new();
        expected_changes.insert(
            "age".to_string(),
            Arithmetic("height".to_string(), Suma, create_literal("10", Int)),
        );

        test_successful_parser_case(tokens, expected_changes);
    }

    #[test]
    fn test_set_clause_parser_complex_assignment() {
        // SET age = 30, height = weight + 10, name = 'John', score = level
        let tokens = vec![
            create_identifier_token("age"),
            create_comparison_operation_token(Equal),
            create_token_literal("30", Int),
            create_symbol_token(COMMA),
            create_identifier_token("height"),
            create_comparison_operation_token(Equal),
            create_identifier_token("weight"),
            create_arith_math_token(Suma),
            create_token_literal("10", Int),
            create_symbol_token(COMMA),
            create_identifier_token("name"),
            create_comparison_operation_token(Equal),
            create_token_literal("'John'", Text),
            create_symbol_token(COMMA),
            create_identifier_token("score"),
            create_comparison_operation_token(Equal),
            create_identifier_token("level"),
        ];

        let mut expected_changes = HashMap::new();

        expected_changes.insert("age".to_string(), Simple(create_literal("30", Int)));
        expected_changes.insert(
            "height".to_string(),
            Arithmetic("weight".to_string(), Suma, create_literal("10", Int)),
        );
        expected_changes.insert("name".to_string(), Simple(create_literal("'John'", Text)));
        expected_changes.insert("score".to_string(), Column("level".to_string()));

        test_successful_parser_case(tokens, expected_changes);
    }

    #[test]
    fn test_set_clause_parser_invalid_syntax() {
        // age 30
        let tokens = vec![
            create_identifier_token("age"),
            create_token_literal("30", Int),
        ];

        let expected_error = "= should follow a SET assignment";
        test_parser_error_case(tokens, expected_error);
    }

    #[test]
    fn test_set_clause_parser_invalid_assignment() {
        // age = height +
        let tokens = vec![
            create_identifier_token("age"),
            create_comparison_operation_token(Equal),
            create_identifier_token("height"),
            create_arith_math_token(Suma),
        ];

        let expected_error = "Query lacks parameters";
        test_parser_error_case(tokens, expected_error);
    }
}
