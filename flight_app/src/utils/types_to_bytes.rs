use super::{consistency_level::ConsistencyLevel, constants::ERROR_WRITE};
use std::{collections::HashMap, io::Write};

#[derive(Default)]
pub struct TypesToBytes {
    bytes: Vec<u8>,
}

impl TypesToBytes {
    pub fn write_u8(&mut self, value: u8) -> Result<(), String> {
        self.bytes.write_all(&[value]).map_err(|_| ERROR_WRITE)?;
        Ok(())
    }

    pub fn write_i16(&mut self, value: i16) -> Result<(), String> {
        self.bytes
            .write_all(&value.to_be_bytes())
            .map_err(|_| ERROR_WRITE)?;
        Ok(())
    }

    pub fn write_u32(&mut self, value: u32) -> Result<(), String> {
        self.bytes
            .write_all(&value.to_be_bytes())
            .map_err(|_| ERROR_WRITE)?;
        Ok(())
    }

    pub fn write_short(&mut self, value: u16) -> Result<(), String> {
        self.write_i16(value as i16)
    }

    pub fn write_int(&mut self, value: i32) -> Result<(), String> {
        self.bytes
            .write_all(&value.to_be_bytes())
            .map_err(|_| ERROR_WRITE)?;
        Ok(())
    }

    pub fn write_long(&mut self, value: i64) -> Result<(), String> {
        self.bytes
            .write_all(&value.to_be_bytes())
            .map_err(|_| ERROR_WRITE)?;
        Ok(())
    }

    pub fn write_string(&mut self, value: &str) -> Result<(), String> {
        let bytes = value.as_bytes();
        let length = bytes.len() as i16;
        self.write_short(length as u16)?; // Usa write_short
        self.bytes.write_all(bytes).map_err(|_| ERROR_WRITE)?;
        Ok(())
    }

    pub fn write_long_string(&mut self, value: &str) -> Result<(), String> {
        let bytes = value.as_bytes();
        let length = bytes.len() as i32;
        self.write_int(length)?; // Usa write_int
        self.bytes.write_all(bytes).map_err(|_| ERROR_WRITE)?;
        Ok(())
    }

    pub fn write_string_map(&mut self, map: &HashMap<String, String>) -> Result<(), String> {
        let n = map.len() as u16;
        self.write_short(n)?;

        for (key, value) in map {
            self.write_string(key)?;
            self.write_string(value)?
        }

        Ok(())
    }

    pub fn write_consistency(&mut self, consistency: ConsistencyLevel) -> Result<(), String> {
        self.write_i16(consistency.to_i16())
    }

    pub fn write_bytes(&mut self, bytes: &[u8]) {
        self.bytes.extend(bytes);
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}


#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::utils::{bytes_cursor::BytesCursor, types_to_bytes::TypesToBytes};

    #[test]
    fn test_write_u8() {
        let mut cursor = TypesToBytes::default();
        cursor.write_u8(1).unwrap();
        assert_eq!(cursor.into_bytes(), vec![0x01]);
    }

    #[test]
    fn test_write_i16() {
        let mut cursor = TypesToBytes::default();
        cursor.write_i16(2).unwrap();
        assert_eq!(cursor.into_bytes(), vec![0x00, 0x02]);
    }

    #[test]
    fn test_write_u32() {
        let mut cursor = TypesToBytes::default();
        cursor.write_u32(3).unwrap();
        assert_eq!(cursor.into_bytes(), vec![0x00, 0x00, 0x00, 0x03]);
    }

    #[test]
    fn test_write_int() {
        let mut cursor = TypesToBytes::default();
        cursor.write_int(18).unwrap();
        assert_eq!(cursor.into_bytes(), vec![0x00, 0x00, 0x00, 0x12]);
    }

    #[test]
    fn test_write_long() {
        let mut cursor = TypesToBytes::default();
        cursor.write_long(35).unwrap();
        assert_eq!(cursor.into_bytes(), vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x23]);
    }

    #[test]
    fn test_write_short() {
        let mut cursor = TypesToBytes::default();
        cursor.write_short(26).unwrap();
        assert_eq!(cursor.into_bytes(), vec![0x00, 0x1A]);
    }

    #[test]
    fn test_write_string() {
        let bytes = vec![
            0x00, 0x0A, b'I', b'n', b'm', b'u', b't', b'a', b'b', b'l', b'e', b's'
        ]; // length, string
        let mut cursor = TypesToBytes::default();
        cursor.write_string("Inmutables").unwrap();
        assert_eq!(cursor.into_bytes(), bytes);
    }

    #[test]
    fn test_write_long_string() {
        let bytes = vec![
            0x00, 0x00, 0x00, 0x0A, b'I', b'n', b'm', b'u', b't', b'a', b'b', b'l', b'e', b's'
        ]; // length, string
        let mut cursor = TypesToBytes::default();
        cursor.write_long_string("Inmutables").unwrap();
        assert_eq!(cursor.into_bytes(), bytes);
    }

    #[test]
    fn test_write_string_map() {
        let mut string_map = HashMap::new();
        string_map.insert("key1".to_string(), "value1".to_string());
        string_map.insert("key2".to_string(), "value2".to_string());

        let mut cursor = TypesToBytes::default();
        cursor.write_string_map(&string_map).unwrap();
        let result_bytes = cursor.into_bytes();

        let mut cursor = BytesCursor::new(&result_bytes);
        assert_eq!(cursor.read_string_map().unwrap(), string_map);
    }
}