
use std::collections::HashSet;

const PALABRAS_RESERVADAS: &[&str] = &[
    "SELECT", "INSERT", "ALTER", "AND", "ASC", "AS", "BATCH", "BY", "CREATE", "DELETE", 
    "DESC", "DISTINCT", "DROP", "FROM", "IF", "INTO", "KEY", "KEYS", "KEYSPACE", "KEYSPACES", 
    "NOT", "NULL", "OR", "PRIMARY", "RENAME", "REPLACE", "SET", "TABLE", "TO", "TRUNCATE", 
    "UPDATE", "USE", "USING", "VALUES", "WHERE", "WITH"
];

struct WordsReserved {
    words: HashSet<String>,
}

impl WordsReserved {
    #[allow(dead_code)]
    fn new() -> Self {
        let mut set = HashSet::new();
        for &word in PALABRAS_RESERVADAS {
            set.insert(word.to_string());
        }
        WordsReserved { words: set }
    }
    #[allow(dead_code)]
    fn is_reserved(&self, word: &str) -> bool {
        self.words.contains(&word.to_uppercase())
    }
}
