use rmp_serde::decode::Error;

use crate::utils::types_to_bytes::TypesToBytes;

use super::errors::Errors;
pub struct Response;

impl Response {
    pub fn void() -> Result<Vec<u8>, Errors> {
        let mut encoder = TypesToBytes::default();
        encoder.write_int(0x0001).map_err(|e| Errors::TruncateError(e))?;
        Ok(encoder.into_bytes())
    }

    pub fn set_keyspace(keyspace: &str) -> Result<Vec<u8>, Errors> {
        let mut encoder = TypesToBytes::default();
        encoder.write_int(0x0003).map_err(|e| Errors::TruncateError(e))?;
        encoder.write_string(keyspace).map_err(|e| Errors::TruncateError(e))?;
        Ok(encoder.into_bytes())
    }

    pub fn schema_change(change_type: &str, target: &str, options: &str) -> Result<Vec<u8>, Errors> {
        let mut encoder = TypesToBytes::default();
        encoder.write_int(0x0005).map_err(|e| Errors::TruncateError(e))?;
        encoder.write_string(change_type).map_err(|e| Errors::TruncateError(e))?;
        encoder.write_string(target).map_err(|e| Errors::TruncateError(e))?;
        encoder.write_string(options).map_err(|e| Errors::TruncateError(e))?;
        Ok(encoder.into_bytes())
    }

    pub fn rows() -> Result<Vec<u8>, String> {
        unimplemented!() // Implementación pendiente
    }
}
