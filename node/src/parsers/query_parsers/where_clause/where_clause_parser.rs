use super::boolean_expression::WhereClause;
use crate::{
    parsers::{
        query_parsers::where_clause::comparison::ComparisonExpr,
        tokens::token::{BooleanOperations, LogicalOperators, Term, Token},
    },
    utils::{
        errors::Errors,
        token_conversor::{
            get_comparision_operator, get_identifier_string, get_list, get_literal, get_next_value,
        },
    },
};

use BooleanOperations::*;
use LogicalOperators::*;
use Term::*;
use Token::*;

use std::{iter::Peekable, vec::IntoIter};

pub struct WhereClauseParser;

impl WhereClauseParser {
    pub fn parse(tokens: Vec<Token>) -> Result<Option<WhereClause>, Errors> {
        Ok(Some(where_clause(&mut tokens.into_iter().peekable())?))
    }
}

fn where_and_or(
    tokens: &mut Peekable<IntoIter<Token>>,
    left_expr: WhereClause,
) -> Result<WhereClause, Errors> {
    match get_next_value(tokens) {
        Ok(Term(AritmeticasBool(Logical(And)))) => {
            let right_expr = where_clause(tokens)?;
            Ok(WhereClause::And(
                Box::new(left_expr),
                Box::new(right_expr),
            ))
        }
        Ok(Term(AritmeticasBool(Logical(Or)))) => {
            let right_expr = where_clause(tokens)?;
            Ok(WhereClause::Or(
                Box::new(left_expr),
                Box::new(right_expr),
            ))
        }
        Err(_) => Ok(left_expr),
        _ => Err(Errors::Invalid(
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
    let expression =
        WhereClause::Comparation(ComparisonExpr::new(column_name, &operator, literal));
    where_and_or(tokens, expression)
}

fn where_tuple(
    tokens: &mut Peekable<IntoIter<Token>>,
    column_names: Vec<Token>,
) -> Result<WhereClause, Errors> {
    let operator = get_comparision_operator(tokens)?;
    let literals = get_list(tokens)?;
    if column_names.len() != literals.len() {
        return Err(Errors::Invalid("Invalid tuples len".to_string()));
    }
    let iterations = column_names.len();
    let mut column_iter = column_names.into_iter().peekable();
    let mut literal_iter = literals.into_iter().peekable();

    let mut tuple = Vec::new();
    for _ in 0..iterations {
        let column_name = get_identifier_string(&mut column_iter)?;
        let literal = get_literal(&mut literal_iter)?;

        let expression = ComparisonExpr::new(column_name, &operator, literal);

        tuple.push(expression);
    }
    let left_expr = WhereClause::Tuple(tuple);
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
        Term(AritmeticasBool(Logical(Not))) => {
            let expression = where_clause(tokens)?;
            Ok(WhereClause::Not(Box::new(expression)))
        }
        _ => Err(Errors::Invalid(
            "Invalid Syntaxis in WHERE_CLAUSE".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::WhereClauseParser;
    use crate::parsers::query_parsers::where_clause::where_clause_parser::Term::AritmeticasBool;
    use crate::parsers::tokens::token::LogicalOperators;
    use crate::parsers::{
        query_parsers::where_clause::{
            boolean_expression::WhereClause, comparison::ComparisonExpr,
        },
        tokens::token::{BooleanOperations, ComparisonOperators, DataType, Literal, Term, Token},
    };

    fn test_successful_parser_case(caso: Vec<Token>, expected: Option<WhereClause>) {
        let resultado = WhereClauseParser::parse(caso);
        match resultado {
            Ok(where_clause) => assert_eq!(where_clause, expected, "Resultado inesperado"),
            Err(e) => panic!("Parser devolvió un error inesperado: {}", e.to_string()),
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
                e.to_string()
            ),
        }
    }

    #[test]
    fn test_parser_simple_comparation() {
        // id = 8
        let tokens = vec![
            Token::Identifier("id".to_string()),
            Token::Term(AritmeticasBool(BooleanOperations::Comparison(
                ComparisonOperators::Equal,
            ))),
            Token::Term(Term::Literal(Literal {
                valor: "8".to_string(),
                tipo: DataType::Integer,
            })),
        ];

        let expected = Some(WhereClause::Comparation(ComparisonExpr::new(
            "id".to_string(),
            &ComparisonOperators::Equal,
            Literal {
                valor: "8".to_string(),
                tipo: DataType::Integer,
            },
        )));

        test_successful_parser_case(tokens, expected);
    }

    #[test]
    fn test_parser_and_comparison() {
        // name = Alice AND id != 20
        let tokens = vec![
            Token::Identifier("name".to_string()),
            Token::Term(AritmeticasBool(BooleanOperations::Comparison(
                ComparisonOperators::Equal,
            ))),
            Token::Term(Term::Literal(Literal {
                valor: "Alice".to_string(),
                tipo: DataType::Text,
            })),
            Token::Term(AritmeticasBool(BooleanOperations::Logical(
                LogicalOperators::And,
            ))),
            Token::Identifier("id".to_string()),
            Token::Term(AritmeticasBool(BooleanOperations::Comparison(
                ComparisonOperators::NotEqual,
            ))),
            Token::Term(Term::Literal(Literal {
                valor: "20".to_string(),
                tipo: DataType::Integer,
            })),
        ];

        let expected = Some(WhereClause::And(
            Box::new(WhereClause::Comparation(ComparisonExpr::new(
                "name".to_string(),
                &ComparisonOperators::Equal,
                Literal {
                    valor: "Alice".to_string(),
                    tipo: DataType::Text,
                },
            ))),
            Box::new(WhereClause::Comparation(ComparisonExpr::new(
                "id".to_string(),
                &ComparisonOperators::NotEqual,
                Literal {
                    valor: "20".to_string(),
                    tipo: DataType::Integer,
                },
            ))),
        ));

        test_successful_parser_case(tokens, expected);
    }

    #[test]
    fn test_parser_not_clause() {
        // NOT is_active = true
        let tokens = vec![
            Token::Term(AritmeticasBool(BooleanOperations::Logical(
                LogicalOperators::Not,
            ))),
            Token::Identifier("is_active".to_string()),
            Token::Term(AritmeticasBool(BooleanOperations::Comparison(
                ComparisonOperators::Equal,
            ))),
            Token::Term(Term::Literal(Literal {
                valor: "true".to_string(),
                tipo: DataType::Boolean,
            })),
        ];

        let expected = Some(WhereClause::Not(Box::new(
            WhereClause::Comparation(ComparisonExpr::new(
                "is_active".to_string(),
                &ComparisonOperators::Equal,
                Literal {
                    valor: "true".to_string(),
                    tipo: DataType::Boolean,
                },
            )),
        )));

        test_successful_parser_case(tokens, expected);
    }

    #[test]
    fn test_parser_tuple_comparation() {
        // (id, name) = (5, 'ivan')
        let tokens = vec![
            Token::TokensList(vec![
                Token::Identifier("id".to_string()),
                Token::Identifier("name".to_string()),
            ]),
            Token::Term(AritmeticasBool(BooleanOperations::Comparison(
                ComparisonOperators::Equal,
            ))),
            Token::TokensList(vec![
                Token::Term(Term::Literal(Literal {
                    valor: "5".to_string(),
                    tipo: DataType::Integer,
                })),
                Token::Term(Term::Literal(Literal {
                    valor: "ivan".to_string(),
                    tipo: DataType::Text,
                })),
            ]),
        ];

        let expected = Some(WhereClause::Tuple(vec![
            ComparisonExpr::new(
                "id".to_string(),
                &ComparisonOperators::Equal,
                Literal {
                    valor: "5".to_string(),
                    tipo: DataType::Integer,
                },
            ),
            ComparisonExpr::new(
                "name".to_string(),
                &ComparisonOperators::Equal,
                Literal {
                    valor: "ivan".to_string(),
                    tipo: DataType::Text,
                },
            ),
        ]));

        test_successful_parser_case(tokens, expected);
    }

    #[test]
    fn test_parser_invalid_case_missing_comparator() {
        // age 18
        let tokens = vec![
            Token::Identifier("age".to_string()),
            Token::Term(Term::Literal(Literal {
                valor: "18".to_string(),
                tipo: DataType::Integer,
            })),
        ];

        test_parser_error_case(tokens, "Expected comparision operator");
    }

    #[test]
    fn test_parser_invalid_case_unexpected_token() {
        // Alice AND
        let tokens = vec![
            Token::Term(Term::Literal(Literal {
                valor: "Alice".to_string(),
                tipo: DataType::Text,
            })),
            Token::Term(AritmeticasBool(BooleanOperations::Logical(
                LogicalOperators::And,
            ))),
        ];

        test_parser_error_case(tokens, "Invalid Syntaxis in WHERE_CLAUSE");
    }

    #[test]
    fn test_parser_multiple_operations() {
        // (id, name) = (5, 'ivan') AND NOT is_active = true OR age < 30
        let tokens = vec![
            Token::TokensList(vec![
                Token::Identifier("id".to_string()),
                Token::Identifier("name".to_string()),
            ]),
            Token::Term(AritmeticasBool(BooleanOperations::Comparison(
                ComparisonOperators::Equal,
            ))),
            Token::TokensList(vec![
                Token::Term(Term::Literal(Literal {
                    valor: "5".to_string(),
                    tipo: DataType::Integer,
                })),
                Token::Term(Term::Literal(Literal {
                    valor: "ivan".to_string(),
                    tipo: DataType::Text,
                })),
            ]),
            Token::Term(AritmeticasBool(BooleanOperations::Logical(
                LogicalOperators::And,
            ))),
            Token::Term(AritmeticasBool(BooleanOperations::Logical(
                LogicalOperators::Not,
            ))),
            Token::Identifier("is_active".to_string()),
            Token::Term(AritmeticasBool(BooleanOperations::Comparison(
                ComparisonOperators::Equal,
            ))),
            Token::Term(Term::Literal(Literal {
                valor: "true".to_string(),
                tipo: DataType::Boolean,
            })),
            Token::Term(AritmeticasBool(BooleanOperations::Logical(
                LogicalOperators::Or,
            ))),
            Token::Identifier("age".to_string()),
            Token::Term(AritmeticasBool(BooleanOperations::Comparison(
                ComparisonOperators::Less,
            ))),
            Token::Term(Term::Literal(Literal {
                valor: "30".to_string(),
                tipo: DataType::Integer,
            })),
        ];

        // (id, name) = (5, 'ivan') AND NOT is_active = true OR age < 30
        let expected = Some(WhereClause::And(
            Box::new(WhereClause::Tuple(vec![
                ComparisonExpr::new(
                    "id".to_string(),
                    &ComparisonOperators::Equal,
                    Literal {
                        valor: "5".to_string(),
                        tipo: DataType::Integer,
                    },
                ),
                ComparisonExpr::new(
                    "name".to_string(),
                    &ComparisonOperators::Equal,
                    Literal {
                        valor: "ivan".to_string(),
                        tipo: DataType::Text,
                    },
                ),
            ])),
            Box::new(WhereClause::Not(Box::new(WhereClause::Or(
                Box::new(WhereClause::Comparation(ComparisonExpr::new(
                    "is_active".to_string(),
                    &ComparisonOperators::Equal,
                    Literal {
                        valor: "true".to_string(),
                        tipo: DataType::Boolean,
                    },
                ))),
                Box::new(WhereClause::Comparation(ComparisonExpr::new(
                    "age".to_string(),
                    &ComparisonOperators::Less,
                    Literal {
                        valor: "30".to_string(),
                        tipo: DataType::Integer,
                    },
                ))),
            )))),
        ));
        test_successful_parser_case(tokens, expected);
    }

    /// Este test lo cree porque no me estaba funcionando algo del peek que capaz despues intento cambiar asi que lo dejo aca
    #[test]
    fn test_peek() {
        let mut tokens = vec![
            Token::Identifier("id".to_string()),
            Token::Term(Term::Literal(Literal {
                valor: "5".to_string(),
                tipo: DataType::Integer,
            })),
            Token::Term(Term::Literal(Literal {
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
