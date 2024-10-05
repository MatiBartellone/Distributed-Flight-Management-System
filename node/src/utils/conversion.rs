use std::collections::HashMap;

use super::errors::Errors;


// A [short] n, followed by n pair <k><v> where <k> and <v> are [string].
pub fn bytes_to_string_map(bytes: &[u8]) -> Result<HashMap<String, String>, Errors> {
    let map = HashMap::new();
    Ok(map)
}
