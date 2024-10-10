use crate::{
    parsers::tokens::token::{BooleanOperations, LogicalOperators, Term, Token}, queries::where_logic::where_clause::{and_where, build_tuple, comparison_where, not_where, or_where, WhereClause}, utils::{
        errors::Errors,
        token_conversor::{get_comparision_operator, get_list, get_literal, get_next_value},
    }
};

use BooleanOperations::*;
use LogicalOperators::*;
use Term::*;
use Token::*;

use std::{iter::Peekable, vec::IntoIter};

pub struct WhereClauseParser;

impl WhereClauseParser {
    pub fn parse(tokens: Vec<Token>) -> Result<WhereClause, Errors> {
        where_clause(&mut tokens.into_iter().peekable())
    }
}

fn where_and_or(
    tokens: &mut Peekable<IntoIter<Token>>,
    left_expr: WhereClause,
) -> Result<WhereClause, Errors> {
    match get_next_value(tokens) {
        // [left_expre, AND, ...]
        Ok(Term(AritmeticasBool(Logical(And)))) => Ok(and_where(left_expr, where_clause(tokens)?)),
        // [left_expre, OR, ...]
        Ok(Term(AritmeticasBool(Logical(Or)))) => Ok(or_where(left_expr, where_clause(tokens)?)),
        // [left_expre]
        Err(_) => Ok(left_expr),
        _ => Err(Errors::SyntaxError(
            "Invalid Syntaxis in WHERE_CLAUSE".to_string(),
        )),
    }
}

fn where_comparision(
    tokens: &mut Peekable<IntoIter<Token>>,
    column_name: String,
) -> Result<WhereClause, Errors> {
    let operator = get_comparision_operator(tokens)?;
    let literal = get_literal(tokens)?;
    let expression = comparison_where(&column_name, operator, literal);
    where_and_or(tokens, expression)
}

fn where_tuple(
    tokens: &mut Peekable<IntoIter<Token>>,
    column_names: Vec<Token>,
) -> Result<WhereClause, Errors> {
    let operator = get_comparision_operator(tokens)?;
    let literals = get_list(tokens)?;
    if column_names.len() != literals.len() {
        return Err(Errors::SyntaxError("Invalid tuples len".to_string()));
    }
    let left_expr = build_tuple(column_names, literals, operator)?;
    where_and_or(tokens, left_expr)
}

fn where_list(
    tokens: &mut Peekable<IntoIter<Token>>,
    list: Vec<Token>,
) -> Result<WhereClause, Errors> {
    match tokens.peek() {
        // [tupla, comparison, tupla, ...]
        Some(Term(AritmeticasBool(Comparison(_)))) => where_tuple(tokens, list),
        // [lista, ...]
        _ => {
            let left_expr = where_clause(&mut list.into_iter().peekable())?;
            where_and_or(tokens, left_expr)
        }
    }
}

fn where_clause(tokens: &mut Peekable<IntoIter<Token>>) -> Result<WhereClause, Errors> {
    match get_next_value(tokens)? {
        // [column_name, comparasion, literal, ...]
        Identifier(column_name) => where_comparision(tokens, column_name),
        // [tupla, comparasion, tupla, ...] or [lista, ...]
        TokensList(token_list) => where_list(tokens, token_list),
        // [NOT, ...]
        Term(AritmeticasBool(Logical(Not))) => Ok(not_where(where_clause(tokens)?)),
        _ => Err(Errors::SyntaxError(
            "Invalid Syntaxis in WHERE_CLAUSE".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use crate::{parsers::tokens::token::{create_literal, ComparisonOperators, DataType, Literal, LogicalOperators, Token}, queries::where_logic::{comparison::ComparisonExpr, where_clause::{and_where, comparison_where, not_where, or_where, tuple_expr, WhereClause}}, utils::token_conversor::{
        create_comparison_operation_token, create_identifier_token, create_logical_operation_token,
        create_token_literal,
    }};
    use crate::parsers::tokens::token::Term;
    use ComparisonOperators::*;
    use DataType::*;
    use LogicalOperators::*;
    use Token::*;

    use super::WhereClauseParser;

    fn test_successful_parser_case(caso: Vec<Token>, expected: Option<WhereClause>) {
        let resultado = WhereClauseParser::parse(caso);
        match resultado {
            Ok(where_clause) => assert_eq!(where_clause, expected.unwrap(), "Resultado inesperado"),
            Err(e) => panic!("Parser devolvió un error inesperado: {}", e),
        }
    }

    fn test_parser_error_case(caso: Vec<Token>, mensaje_error_esperado: &str) {
        let resultado = WhereClauseParser::parse(caso);
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
    fn test_parser_simple_comparation() {
        // id = 8
        let tokens = vec![
            create_identifier_token("id"),
            create_comparison_operation_token(Equal),
            create_token_literal("8", Integer),
        ];
        let expected = Some(comparison_where("id", Equal, create_literal("8", Integer)));
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

        let expected = Some(and_where(
            comparison_where("name", Equal, create_literal("Alice", DataType::Text)),
            comparison_where("id", NotEqual, create_literal("20", DataType::Integer)),
        ));

        test_successful_parser_case(tokens, expected);
    }

    #[test]
    fn test_parser_not_clause() {
        // NOT is_active = true
        let tokens = vec![
            create_logical_operation_token(LogicalOperators::Not),
            create_identifier_token("is_active"),
            create_comparison_operation_token(ComparisonOperators::Equal),
            create_token_literal("true", DataType::Boolean),
        ];

        let expected = Some(not_where(comparison_where(
            "is_active",
            ComparisonOperators::Equal,
            create_literal("true", DataType::Boolean),
        )));

        test_successful_parser_case(tokens, expected);
    }

    #[test]
    fn test_parser_tuple_comparation() {
        // (id, name) = (5, 'ivan')
        let tokens = vec![
            TokensList(vec![
                create_identifier_token("id"),
                create_identifier_token("name"),
            ]),
            create_comparison_operation_token(Equal),
            TokensList(vec![
                create_token_literal("5", Integer),
                create_token_literal("ivan", Text),
            ]),
        ];

        let expected = Some(tuple_expr(vec![
            ComparisonExpr::new("id".to_string(), &Equal, create_literal("5", Integer)),
            ComparisonExpr::new("name".to_string(), &Equal, create_literal("ivan", Text)),
        ]));

        test_successful_parser_case(tokens, expected);
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

        test_parser_error_case(tokens, "Invalid Syntaxis in WHERE_CLAUSE");
    }

    #[test]
    fn test_parser_multiple_operations() {
        // (id, name) = (5, 'ivan') AND NOT is_active = true OR age < 30
        let tokens = vec![
            TokensList(vec![
                create_identifier_token("id"),
                create_identifier_token("name"),
            ]),
            create_comparison_operation_token(Equal),
            TokensList(vec![
                create_token_literal("5", Integer),
                create_token_literal("ivan", Text),
            ]),
            create_logical_operation_token(And),
            create_logical_operation_token(Not),
            create_identifier_token("is_active"),
            create_comparison_operation_token(Equal),
            create_token_literal("true", Boolean),
            create_logical_operation_token(Or),
            create_identifier_token("age"),
            create_comparison_operation_token(Less),
            create_token_literal("30", Integer),
        ];

        let expected = Some(and_where(
            tuple_expr(vec![
                ComparisonExpr::new("id".to_string(), &Equal, create_literal("5", Integer)),
                ComparisonExpr::new("name".to_string(), &Equal, create_literal("ivan", Text)),
            ]),
            not_where(or_where(
                comparison_where(
                    "is_active",
                    ComparisonOperators::Equal,
                    create_literal("true", DataType::Boolean),
                ),
                comparison_where(
                    "age",
                    ComparisonOperators::Less,
                    create_literal("30", DataType::Integer),
                ),
            )),
        ));

        test_successful_parser_case(tokens, expected);
    }

    /// Este test lo cree porque no me estaba funcionando algo del peek que capaz despues intento cambiar asi que lo dejo aca
    #[test]
    fn test_peek() {
        let mut tokens = vec![
            Identifier("id".to_string()),
            Term(Term::Literal(Literal {
                valor: "5".to_string(),
                tipo: DataType::Integer,
            })),
            Term(Term::Literal(Literal {
                valor: "ivan".to_string(),
                tipo: DataType::Text,
            })),
        ]
        .into_iter()
        .peekable();

        let peeked = tokens.peek(); // Esto no consume el token

        // Comprobación
        if let Some(token) = peeked {
            println!("El siguiente token es: {:?}", token);
        }

        // Ahora, consume el siguiente token
        let next = tokens.next();
        println!("Token consumido: {:?}", next);
    }
}
