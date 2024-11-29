use std::collections::HashMap;

const CHARACTER_MAPPINGS: &[(&str, &str)] = &[
    (">=", " _GE_ "),
    ("<=", " _LE_ "),
    ("!=", " _DF_ "),
    ("+", " + "),
    ("-", " - "),
    ("/", " / "),
    ("%", " % "),
    ("<", " < "),
    (">", " > "),
    ("(", " ( "),
    (")", " ) "),
    ("}", " } "),
    ("{", " { "),
    (";", ""),
    (",", " , "),
];

pub struct CharacterMappings {
    mappings: HashMap<String, String>,
}

impl CharacterMappings {
    pub fn new() -> Self {
        let mut map = HashMap::new();
        for &(key, value) in CHARACTER_MAPPINGS {
            map.insert(key.to_string(), value.to_string());
        }
        CharacterMappings { mappings: map }
    }

    pub fn get_mapping(&self, input: &str) -> Option<&String> {
        self.mappings.get(input)
    }

    pub fn is_mapped(&self, input: &str) -> bool {
        self.mappings.contains_key(input)
    }
}

impl Default for CharacterMappings {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mapping_initialization() {
        let mappings = CharacterMappings::new();

        // Verificar que se inicializan todas las claves del arreglo
        assert!(mappings.is_mapped(">="));
        assert!(mappings.is_mapped("<="));
        assert!(mappings.is_mapped("+"));
        assert!(mappings.is_mapped(";"));
        assert!(!mappings.is_mapped("unknown"));
    }

    #[test]
    fn test_get_mapping() {
        let mappings = CharacterMappings::new();

        // Verificar que se recuperan correctamente los valores mapeados
        assert_eq!(mappings.get_mapping(">=").unwrap(), " _GE_ ");
        assert_eq!(mappings.get_mapping("<=").unwrap(), " _LE_ ");
        assert_eq!(mappings.get_mapping("+").unwrap(), " + ");
        assert_eq!(mappings.get_mapping(";").unwrap(), "");

        // Verificar que una clave inexistente devuelve None
        assert!(mappings.get_mapping("unknown").is_none());
    }

    #[test]
    fn test_is_mapped() {
        let mappings = CharacterMappings::new();

        // Verificar que los caracteres mapeados devuelven true
        assert!(mappings.is_mapped(">="));
        assert!(mappings.is_mapped("{"));
        assert!(mappings.is_mapped("}"));

        // Verificar que un carácter no mapeado devuelva false
        assert!(!mappings.is_mapped("unknown"));
    }

    #[test]
    fn test_case_sensitivity() {
        let mappings = CharacterMappings::new();

        // Verificar que las claves son case-sensitive
        assert!(mappings.is_mapped(">="));
        assert!(!mappings.is_mapped("GE")); // Case sensitivity check
    }

    #[test]
    fn test_empty_mapping() {
        let mappings = CharacterMappings::new();

        // Verificar que los caracteres que deben producir valores vacíos se comporten correctamente
        assert_eq!(mappings.get_mapping(";").unwrap(), "");
    }
}