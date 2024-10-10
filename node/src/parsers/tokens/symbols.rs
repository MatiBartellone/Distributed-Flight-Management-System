use std::collections::HashSet;

const WORDS_SYMBOLS: &[&str] = &[
    "{",
    "}",
    ",",
    ":"
];

pub struct Symbols {
    words: HashSet<&'static str>,
}

impl Symbols {
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