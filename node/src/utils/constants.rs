use std::process;

pub const NODES_METADATA: &str = "src/meta_data/nodes/metadata.json";

pub fn nodes_meta_data_path() -> String {
    format!("src/meta_data/nodes/{}.json", process::id())
}
pub const QUERY_DELEGATION_PORT: i32 = 9090;
pub const CLIENTS_PORT: i32 = 8080;
pub const DOLLAR: char = '$';
pub const DOUBLE_QUOTE: char = '"';
pub const SINGLE_QUOTE: char = '\'';
pub const WHERE: &str = "WHERE";
pub const FROM: &str = "FROM";
pub const SELECT: &str = "SELECT";
pub const BY: &str = "BY";
pub const SET: &str = "SET";
pub const OPEN_PAREN: &str = "(";
pub const CLOSE_PAREN: &str = ")";
pub const OPEN_BRACE: &str = "{";
pub const CLOSE_BRACE: &str = "}";
pub const ASC: &str = "ASC";
pub const DESC: &str = "DESC";
pub const AND: &str = "AND";
pub const OR: &str = "OR";
pub const NOT: &str = "NOT";
pub const GE: &str = " _GE_ ";
pub const LE: &str = " _LE_ ";
pub const DF: &str = " _DF_ ";
pub const PLUS: &str = " + ";
pub const MINUS: &str = " - ";
pub const DIV: &str = " / ";
pub const MOD: &str = " % ";
pub const LT: &str = " < ";
pub const GT: &str = " > ";
pub const EMPTY: &str = "";
pub const SPACE: &str = " ";
pub const EXISTS: &str = "EXISTS";
pub const IF: &str = "IF";
pub const INTO: &str = "INTO";
pub const VALUES: &str = "VALUES";
pub const ORDER: &str = "ORDER";
pub const COMMA: &str = ",";
pub const KEYSPACE: &str = "KEYSPACE";
pub const TABLE: &str = "TABLE";
