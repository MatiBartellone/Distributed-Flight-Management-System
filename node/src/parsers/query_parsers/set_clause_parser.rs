use std::collections::HashMap;
use Token::*;
use ComparisonOperators::*;
use Term::*;
use std::{iter::Peekable, vec::IntoIter};
use crate::{parsers::tokens::{terms::{ComparisonOperators, Term}, token::Token}, queries::set_logic::assigmente_value::AssignmentValue, utils::{errors::Errors, token_conversor::{get_arithmetic_math, get_comparision_operator, get_next_value}}};

pub struct SetClauseParser;

impl SetClauseParser {
    pub fn parse(tokens: Vec<Token>) -> Result<HashMap<String, AssignmentValue>, Errors> {
        let mut changes = HashMap::new();
        values(&mut tokens.into_iter().peekable(), &mut changes)?;
        Ok(changes)
    }
}

fn values(tokens: &mut Peekable<IntoIter<Token>>, changes: &mut HashMap<String, AssignmentValue>) -> Result<(), Errors> {
    match tokens.next(){
        Some(Identifier(column_name)) => assignment(tokens, changes, column_name),
        _ if changes.is_empty() => Err(Errors::SyntaxError("Invalid Sintax follow SET".to_string())),
        _ => Ok(())
    }
}

fn assignment(tokens: &mut Peekable<IntoIter<Token>>, changes: &mut HashMap<String, AssignmentValue>, column_name: String) -> Result<(), Errors> {
    let Ok(Equal) = get_comparision_operator(tokens) else {
        return Err(Errors::SyntaxError("= should follow a SET assignment".to_string()))
    };
    match get_next_value(tokens)? {
        // [column_name, = , literal]
        Term(Literal(value)) => {
            changes.insert(column_name, AssignmentValue::Simple(value));
            values(tokens, changes)
        }
        // [column_name, = , other_column, ...]
        Identifier(other_column) => column_asssigment(tokens, changes, column_name, other_column),
        _ => Err(Errors::SyntaxError("Invalid assigment".to_string())),
    }
}

fn column_asssigment(tokens: &mut Peekable<IntoIter<Token>>, changes: &mut HashMap<String, AssignmentValue>, column_name: String, other_column: String) -> Result<(), Errors> {
    match tokens.peek() {
        // [column_name, = , other_column, +|-, literal]
        Some(Term(ArithMath(_))) =>
            arithmetic_assigment(tokens, changes, column_name, other_column),
        // [column_name, = , other_column]
        _ => {
            changes.insert(column_name, AssignmentValue::Column(other_column));
            values(tokens, changes)
        }
    }
}

fn arithmetic_assigment(tokens: &mut Peekable<IntoIter<Token>>, changes: &mut HashMap<String, AssignmentValue>, column_name: String, other_column: String) -> Result<(), Errors> {
    let op = get_arithmetic_math(tokens)?;
    match get_next_value(tokens)? {
        Term(Literal(literal)) => {
            changes.insert(column_name, AssignmentValue::Arithmetic(other_column, op, literal));
            values(tokens, changes)
        }
        _ => Err(Errors::SyntaxError("Expected a numeric literal after the arithmetic operator".to_string())),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use ComparisonOperators::*;
    use DataType::*;
    use ArithMath::*;

    use crate::{parsers::{query_parsers::set_clause_parser::SetClauseParser, tokens::{data_type::DataType, literal::create_literal, terms::{ArithMath, ComparisonOperators}, token::Token}}, queries::set_logic::assigmente_value::AssignmentValue, utils::{errors::Errors, token_conversor::{create_aritmeticas_math_token, create_comparison_operation_token, create_identifier_token, create_token_literal}}};

    fn test_successful_set_clause_parser_case(tokens: Vec<Token>, expected_changes: HashMap<String, AssignmentValue>) {
        let result = SetClauseParser::parse(tokens);
        assert!(result.is_ok(), "El parser falló en un caso exitoso.");
        let changes = result.unwrap();
        assert_eq!(changes, expected_changes, "Los cambios generados no coinciden con los esperados.");
    }

    fn test_failed_set_clause_parser_case(tokens: Vec<Token>, expected_error: Errors) {
        let result = SetClauseParser::parse(tokens);
        assert!(result.is_err(), "El parser no falló cuando debía.");
        let error = result.unwrap_err();
        assert_eq!(error, expected_error, "El error recibido no coincide con el esperado.");
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
        expected_changes.insert("age".to_string(), AssignmentValue::Simple(create_literal("30", Int)));

        test_successful_set_clause_parser_case(tokens, expected_changes);
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
        expected_changes.insert("age".to_string(), AssignmentValue::Column("height".to_string()));

        test_successful_set_clause_parser_case(tokens, expected_changes);
    }

    #[test]
    fn test_set_clause_parser_arithmetic_assignment() {
        // age = height + 10
        let tokens = vec![
            create_identifier_token("age"),
            create_comparison_operation_token(Equal),
            create_identifier_token("height"),
            create_aritmeticas_math_token(Suma),
            create_token_literal("10", Int),
        ];

        let mut expected_changes = HashMap::new();
        expected_changes.insert(
            "age".to_string(),
            AssignmentValue::Arithmetic("height".to_string(), Suma, create_literal("10", Int)),
        );

        test_successful_set_clause_parser_case(tokens, expected_changes);
    }

    #[test]
    fn test_set_clause_parser_invalid_syntax() {
        // age 30
        let tokens = vec![
            create_identifier_token("age"),
            create_token_literal("30", Int),
        ];

        let expected_error = Errors::SyntaxError("= should follow a SET assignment".to_string());
        test_failed_set_clause_parser_case(tokens, expected_error);
    }

    #[test]
    fn test_set_clause_parser_invalid_assignment() {
        // age = height +
        let tokens = vec![
            create_identifier_token("age"),
            create_comparison_operation_token(Equal),
            create_identifier_token("height"),
            create_aritmeticas_math_token(Suma),
        ];

        let expected_error = Errors::SyntaxError("Query lacks parameters".to_string());
        test_failed_set_clause_parser_case(tokens, expected_error);
    }
}