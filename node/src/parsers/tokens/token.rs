#[allow(dead_code)]
#[derive(Debug)]
pub enum Token {
    Identifier(String),
    Term(Term),
    Reserved(String),
    DataType(DataType),
    TokensList(Vec<Token>),
}

#[allow(dead_code)]
#[derive(Debug)]
pub enum Term {
    Literal(Literal),
    AritmeticasMath(AritmeticasMath),
    AritmeticasBool(BooleanOperations),
}

#[allow(dead_code)]
#[derive(Debug, PartialEq)]
pub struct Literal {
    pub valor: String,
    pub tipo: DataType,
}

#[allow(dead_code)]
#[derive(Debug)]
pub enum AritmeticasMath {
    Suma,
    Resta,
    Division,
    Resto,
    Multiplication,
}

#[allow(dead_code)]
#[derive(Debug)]
pub enum BooleanOperations {
    Logical(LogicalOperators),
    Comparison(ComparisonOperators),
}

#[allow(dead_code)]
#[derive(Debug)]
pub enum LogicalOperators {
    Or,
    And,
    Not,
}

#[allow(dead_code)]
#[derive(Debug, PartialEq)]
pub enum ComparisonOperators {
    Less,
    Equal,
    NotEqual,
    Greater,
    GreaterOrEqual,
    LessOrEqual,
}

#[allow(dead_code)]
#[derive(Debug, PartialOrd, PartialEq)]
pub enum DataType {
    Integer,
    Boolean,
    Date,
    Decimal,
    Text,
    Duration,
    Time,
}

use DataType::*;

impl PartialOrd for Literal {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        if self.tipo != other.tipo {
            return None;
        }
        match self.tipo {
            Integer => {
                let val1 = self.valor.parse::<i64>().ok()?;
                let val2 = other.valor.parse::<i64>().ok()?;
                Some(val1.cmp(&val2))
            }
            Boolean => {
                let val1 = self.valor.parse::<bool>().ok()?;
                let val2 = other.valor.parse::<bool>().ok()?;
                Some(val1.cmp(&val2))
            }
            Decimal => {
                let val1 = self.valor.parse::<f64>().ok()?;
                let val2 = other.valor.parse::<f64>().ok()?;
                Some(val1.partial_cmp(&val2)?)
            }
            Text => Some(self.valor.cmp(&other.valor)),
            Date => todo!(),
            Duration => todo!(),
            Time => todo!(),
        }
    }
}