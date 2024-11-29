use super::{consistency_level::ConsistencyLevel, constants::ERROR_WRITE, errors::Errors};
use std::{collections::HashMap, io::Write};

#[derive(Default)]
pub struct TypesToBytes {
    bytes: Vec<u8>,
}

impl TypesToBytes {
    pub fn write_u8(&mut self, value: u8) -> Result<(), Errors> {
        self.bytes.write_all(&[value]).map_err(|_| Errors::ServerError(String::from(ERROR_WRITE)))?;
        Ok(())
    }

    pub fn write_i16(&mut self, value: i16) -> Result<(), Errors> {
        self.bytes
            .write_all(&value.to_be_bytes())
            .map_err(|_| Errors::ServerError(String::from(ERROR_WRITE)))?;
        Ok(())
    }

    pub fn write_u32(&mut self, value: u32) -> Result<(), Errors> {
        self.bytes
            .write_all(&value.to_be_bytes())
            .map_err(|_| Errors::ServerError(String::from(ERROR_WRITE)))?;
        Ok(())
    }

    pub fn write_u64(&mut self, value: u64) -> Result<(), Errors> {
        self.bytes
            .write_all(&value.to_be_bytes())
            .map_err(|_| Errors::ServerError(String::from(ERROR_WRITE)))?;
        Ok(())
    }

    pub fn write_short(&mut self, value: u16) -> Result<(), Errors> {
        self.write_i16(value as i16)
    }

    pub fn write_int(&mut self, value: i32) -> Result<(), Errors> {
        self.bytes
            .write_all(&value.to_be_bytes())
            .map_err(|_| Errors::ServerError(String::from(ERROR_WRITE)))?;
        Ok(())
    }

    pub fn write_long(&mut self, value: i64) -> Result<(), Errors> {
        self.bytes
            .write_all(&value.to_be_bytes())
            .map_err(|_| Errors::ServerError(String::from(ERROR_WRITE)))?;
        Ok(())
    }

    pub fn write_string(&mut self, value: &str) -> Result<(), Errors> {
        let bytes = value.as_bytes();
        let length = bytes.len() as i16;
        self.write_short(length as u16)?; // Usa write_short
        self.bytes.write_all(bytes).map_err(|_| Errors::ServerError(String::from(ERROR_WRITE)))?;
        Ok(())
    }

    pub fn write_long_string(&mut self, value: &str) -> Result<(), Errors> {
        let bytes = value.as_bytes();
        let length = bytes.len() as i32;
        self.write_int(length)?; // Usa write_int
        self.bytes.write_all(bytes).map_err(|_| Errors::ServerError(String::from(ERROR_WRITE)))?;
        Ok(())
    }

    pub fn write_string_map(&mut self, map: &HashMap<String, String>) -> Result<(), Errors> {
        let n = map.len() as u16;
        self.write_short(n)?;

        for (key, value) in map {
            self.write_string(key)?;
            self.write_string(value)?
        }

        Ok(())
    }

    pub fn write_consistency(&mut self, consistency: ConsistencyLevel) -> Result<(), Errors> {
        self.write_i16(consistency.to_i16())
    }

    pub fn write_bytes(&mut self, bytes: &[u8]) {
        self.bytes.extend(bytes);
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }

    pub fn length(&self) -> usize {
        self.bytes.len()
    }
}
