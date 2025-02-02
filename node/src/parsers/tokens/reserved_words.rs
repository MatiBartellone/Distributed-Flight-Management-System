//! # WordsReserved Module
//!
//! Este módulo proporciona la estructura `WordsReserved` para manejar un conjunto de palabras reservadas predefinidas.

use std::collections::HashSet;

const RESERVED_WORDS: &[&str] = &[
    "SELECT",
    "INSERT",
    "ALTER",
    "ADD",
    "AND",
    "ASC",
    "AS",
    "BATCH",
    "BY",
    "CREATE",
    "DELETE",
    "DESC",
    "DISTINCT",
    "DROP",
    "FROM",
    "IF",
    "INTO",
    "KEY",
    "KEYS",
    "KEYSPACE",
    "KEYSPACES",
    "NOT",
    "NULL",
    "OR",
    "PRIMARY",
    "RENAME",
    "REPLACE",
    "SET",
    "TABLE",
    "TO",
    "TRUNCATE",
    "UPDATE",
    "USE",
    "USING",
    "VALUES",
    "WHERE",
    "WITH",
    "ORDER",
    "REPLICATION",
    "EXISTS",
];

/// Representa un conjunto de palabras reservadas predefinidas.
///
/// La estructura `WordsReserved` permite verificar si una palabra pertenece al conjunto de palabras
/// reservadas definidas en `RESERVED_WORDS`.
pub struct WordsReserved {
    words: HashSet<&'static str>,
}

impl WordsReserved {
    /// Crea una nueva instancia de `WordsReserved` con las palabras reservadas predefinidas.
    ///
    /// # Retorno
    /// - Una nueva instancia de `WordsReserved`.
    ///
    /// # Ejemplo
    /// ```ignore
    /// use crate::WordsReserved;
    /// let reserved = WordsReserved::new();
    /// ```
    pub fn new() -> Self {
        let mut set = HashSet::new();
        for &word in RESERVED_WORDS {
            set.insert(word);
        }
        WordsReserved { words: set }
    }

    /// Verifica si una palabra es una palabra reservada.
    ///
    /// Este método es insensible a mayúsculas y minúsculas.
    ///
    /// # Parámetros
    /// - `word`: La palabra a verificar.
    ///
    /// # Retorno
    /// - `true` si la palabra está en el conjunto de palabras reservadas.
    /// - `false` en caso contrario.
    ///
    /// # Ejemplo
    /// ```ignore
    /// use crate::WordsReserved;
    /// let reserved = WordsReserved::new();
    /// assert!(reserved.is_reserved("SELECT"));
    /// assert!(reserved.is_reserved("select")); // Insensible a mayúsculas.
    /// assert!(!reserved.is_reserved("non_reserved_word"));
    /// ```
    pub fn is_reserved(&self, word: &str) -> bool {
        self.words.contains(&word.to_uppercase().as_str())
    }
}

impl Default for WordsReserved {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_words_reserved_new() {
        let reserved = WordsReserved::new();
        assert!(reserved.is_reserved("SELECT"));
        assert!(reserved.is_reserved("INSERT"));
        assert!(!reserved.is_reserved("RANDOM")); // Palabra no reservada
        assert!(!reserved.is_reserved("")); // Cadena vacía no debe ser reservada
    }

    #[test]
    fn test_words_reserved_case_insensitivity() {
        let reserved = WordsReserved::new();
        assert!(reserved.is_reserved("select")); // Prueba en minúsculas
        assert!(reserved.is_reserved("Select")); // Prueba con capitalización mixta
        assert!(reserved.is_reserved("SeLeCt")); // Variación adicional
    }

    #[test]
    fn test_words_reserved_default() {
        let reserved = WordsReserved::default(); // Usa la implementación de Default
        assert!(reserved.is_reserved("CREATE"));
        assert!(reserved.is_reserved("DROP"));
        assert!(!reserved.is_reserved("CUSTOM")); // No es reservada
    }

    #[test]
    fn test_words_reserved_non_reserved_word() {
        let reserved = WordsReserved::new();
        assert!(!reserved.is_reserved("hello")); // Palabra aleatoria no reservada
        assert!(!reserved.is_reserved("cassandra")); // Otra palabra no reservada
    }

    #[test]
    fn test_words_reserved_partial_match() {
        let reserved = WordsReserved::new();
        assert!(!reserved.is_reserved("SELECTED")); // No debe coincidir con "SELECT"
        assert!(!reserved.is_reserved("WHEREVER")); // No debe coincidir con "WHERE"
    }
}
