use std::{iter::Peekable, vec::IntoIter};
use BooleanOperations::*;
use LogicalOperators::*;
use Term::*;
use Token::*;
use IfClause::Exist;

use crate::parsers::tokens::token::{BooleanOperations, LogicalOperators, Term, Token};
use crate::queries::if_clause::{and_if, comparison_if, not_if, or_if, IfClause};
use crate::utils::errors::Errors;
use crate::utils::token_conversor::{get_comparision_operator, get_literal, get_next_value};

const EXISTS: &str = "EXISTS";

pub struct IfClauseParser;

impl IfClauseParser {
    pub fn parse(tokens: Vec<Token>) -> Result<IfClause, Errors> {
        if_clause(&mut tokens.into_iter().peekable())
    }
}

fn if_and_or(
    tokens: &mut Peekable<IntoIter<Token>>,
    left_expr: IfClause,
) -> Result<IfClause, Errors> {
    match get_next_value(tokens) {
        // [left_expre, AND, ...]
        Ok(Term(AritmeticasBool(Logical(And)))) => Ok(and_if(left_expr, if_clause(tokens)?)),
        // [left_expre, OR, ...]
        Ok(Term(AritmeticasBool(Logical(Or)))) => Ok(or_if(left_expr, if_clause(tokens)?)),
        // [left_expre]
        Err(_) => Ok(left_expr),
        _ => Err(Errors::SyntaxError(
            "Invalid Syntaxis in IF_CLAUSE".to_string(),
        )),
    }
}

fn if_comparision(
    tokens: &mut Peekable<IntoIter<Token>>,
    column_name: String,
) -> Result<IfClause, Errors> {
    let operator = get_comparision_operator(tokens)?;
    let literal = get_literal(tokens)?;
    let expression = comparison_if(&column_name, operator, literal);
    if_and_or(tokens, expression)
}

fn if_clause(tokens: &mut Peekable<IntoIter<Token>>) -> Result<IfClause, Errors> {
    match get_next_value(tokens)? {
        // [column_name, comparasion, literal, ...]
        Identifier(column_name) => if_comparision(tokens, column_name),
        // [exists, ...]
        Reserved(exists) if exists == *EXISTS => if_and_or(tokens, Exist),
        // [NOT, ...]
        Term(AritmeticasBool(Logical(Not))) => Ok(not_if(if_clause(tokens)?)),
        _ => Err(Errors::SyntaxError(
            "Invalid Syntaxis in IF_CLAUSE".to_string(),
        )),
    }
}


#[cfg(test)]
mod tests {
    use crate::{parsers::tokens::token::{create_literal, ComparisonOperators, DataType, LogicalOperators, Token}, queries::if_clause::{and_if, comparison_if, not_if, IfClause}, utils::token_conversor::{create_comparison_operation_token, create_identifier_token, create_logical_operation_token, create_reserved_token, create_token_literal}};
    use LogicalOperators::*;
    use ComparisonOperators::*;
    use DataType::*;
    use super::IfClauseParser;

    fn test_successful_parser_case(caso: Vec<Token>, expected: Option<IfClause>) {
        let resultado = IfClauseParser::parse(caso);
        match resultado {
            Ok(if_clause) => assert_eq!(if_clause, expected.unwrap(), "Resultado inesperado"),
            Err(e) => panic!("Parser devolvió un error inesperado: {}", e),
        }
    }

    fn test_parser_error_case(caso: Vec<Token>, mensaje_error_esperado: &str) {
        let resultado = IfClauseParser::parse(caso);
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
    fn test_if_clause_exists() {
        // EXISTS
        let tokens = vec![
            create_reserved_token("EXISTS"),
        ];
        let expected = Some(IfClause::Exist);
        test_successful_parser_case(tokens, expected);
    }

    #[test]
    fn test_if_clause_not_exists() {
        // IF NOT EXISTS
        let tokens = vec![
            create_logical_operation_token(Not),
            create_reserved_token("EXISTS"),
        ];
        let expected = Some(not_if(IfClause::Exist));
        test_successful_parser_case(tokens, expected);
    }

    #[test]
    fn test_parser_simple_comparation() {
        // id = 8
        let tokens = vec![
            create_identifier_token("id"),
            create_comparison_operation_token(Equal),
            create_token_literal("8", Integer),
        ];
        let expected = Some(comparison_if("id", Equal, create_literal("8", Integer)));
        test_successful_parser_case(tokens, expected);
    }

    #[test]
    fn test_parser_and_comparison() {
        // name = 'Alice' AND id != 20
        let tokens = vec![
            create_identifier_token("name"),
            create_comparison_operation_token(Equal),
            create_token_literal("Alice", Text),
            create_logical_operation_token(And),
            create_identifier_token("id"),
            create_comparison_operation_token(NotEqual),
            create_token_literal("20", DataType::Integer),
        ];

        let expected = Some(and_if(
            comparison_if("name", Equal, create_literal("Alice", Text)),
            comparison_if("id", NotEqual, create_literal("20", Integer)),
        ));

        test_successful_parser_case(tokens, expected);
    }

    #[test]
    fn test_parser_not_clause() {
        // NOT is_active = true
        let tokens = vec![
            create_logical_operation_token(Not),
            create_identifier_token("is_active"),
            create_comparison_operation_token(Equal),
            create_token_literal("true", Boolean),
        ];

        let expected = Some(not_if(comparison_if(
            "is_active",
            Equal,
            create_literal("true", Boolean),
        )));

        test_successful_parser_case(tokens, expected);
    }

    #[test]
    fn test_if_clause_exists_in_the_middle() {
        // id = 8 AND EXISTS AND age > 30
        let tokens = vec![
            create_identifier_token("id"),
            create_comparison_operation_token(Equal),
            create_token_literal("8", Integer),
            create_logical_operation_token(And),
            create_reserved_token("EXISTS"),
            create_logical_operation_token(And),
            create_identifier_token("age"),
            create_comparison_operation_token(Greater),
            create_token_literal("30", Integer),
        ];
        
        let expected = Some(and_if(
                comparison_if("id", Equal, create_literal("8", Integer)),
                and_if(
                    IfClause::Exist,
                    comparison_if("age", Greater, create_literal("30", Integer)),
                ),
            ),
        );

        test_successful_parser_case(tokens, expected);
    }

    #[test]
    fn test_if_clause_invalid_mixed_exists_and_comparison() {
        // IF EXISTS id = 8
        let tokens = vec![
            create_reserved_token("EXISTS"),
            create_identifier_token("id"),
            create_comparison_operation_token(Equal),
            create_token_literal("8", Integer),
        ];
        test_parser_error_case(tokens, "Invalid Syntaxis in IF_CLAUSE");
    }

    #[test]
    fn test_parser_invalid_case_missing_comparator() {
        // age 18
        let tokens = vec![
            create_identifier_token("age"),
            create_token_literal("18", Integer),
        ];

        test_parser_error_case(tokens, "Expected comparision operator");
    }

    #[test]
    fn test_parser_invalid_case_unexpected_token() {
        // Alice AND
        let tokens = vec![
            create_token_literal("Alice", Text),
            create_logical_operation_token(And),
        ];

        test_parser_error_case(tokens, "Invalid Syntaxis in IF_CLAUSE");
    }
}
