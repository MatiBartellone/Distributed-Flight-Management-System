use std::collections::HashMap;

use super::errors::Errors;

pub fn bytes_to_string_map(_bytes: &[u8]) -> Result<HashMap<String, String>, Errors> {
    let map = HashMap::new();
    Ok(map)
}
