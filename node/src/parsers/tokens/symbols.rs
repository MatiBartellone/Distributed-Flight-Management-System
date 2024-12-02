//! # Symbols Module
//!
//! Este módulo define la estructura `Symbols` para manejar un conjunto de símbolos predefinidos.

use std::collections::HashSet;

const WORDS_SYMBOLS: &[&str] = &[",", ":"];

pub struct Symbols {
    words: HashSet<&'static str>,
}

/// Representa un conjunto de símbolos predefinidos.
///
/// La estructura `Symbols` permite verificar si una palabra pertenece al conjunto de símbolos
/// definidos en `WORDS_SYMBOLS`.
impl Symbols {
    /// Crea una nueva instancia de `Symbols` con los símbolos predefinidos.
    ///
    /// # Retorno
    /// - Una nueva instancia de `Symbols`.
    ///
    /// # Ejemplo
    /// ```
    /// use crate::Symbols;
    /// let symbols = Symbols::new();
    /// ```
    pub fn new() -> Self {
        let mut set = HashSet::new();
        for &word in WORDS_SYMBOLS {
            set.insert(word);
        }
        Symbols { words: set }
    }

    pub fn is_symbol(&self, word: &str) -> bool {
        self.words.contains(word)
    }
}

impl Default for Symbols {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_symbols_new() {
        let symbols = Symbols::new();
        assert!(symbols.is_symbol(","));
        assert!(symbols.is_symbol(":"));
        assert!(!symbols.is_symbol(".")); // No incluido en WORDS_SYMBOLS
    }

    #[test]
    fn test_symbols_is_symbol() {
        let symbols = Symbols::new();
        assert!(symbols.is_symbol(",")); // Verifica que `,` está en el conjunto
        assert!(symbols.is_symbol(":")); // Verifica que `:` está en el conjunto
        assert!(!symbols.is_symbol(";")); // No está en WORDS_SYMBOLS
        assert!(!symbols.is_symbol("word")); // No está en WORDS_SYMBOLS
    }

    #[test]
    fn test_symbols_default() {
        let symbols = Symbols::default(); // Usa la implementación de Default
        assert!(symbols.is_symbol(",")); // Verifica que `,` está en el conjunto
        assert!(symbols.is_symbol(":")); // Verifica que `:` está en el conjunto
        assert!(!symbols.is_symbol("!")); // No incluido en WORDS_SYMBOLS
    }

    #[test]
    fn test_symbols_empty_string() {
        let symbols = Symbols::new();
        assert!(!symbols.is_symbol("")); // Verifica que una cadena vacía no sea símbolo
    }
}

