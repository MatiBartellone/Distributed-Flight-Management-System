use crate::utils::consistency_level::ConsistencyLevel;
use crate::utils::errors::Errors;
use std::collections::HashMap;
use std::io::{Cursor, Read};

/// This structure represents a cursor to read bytes, which given an initial slice
/// of bytes consumes them dynamically with the use of its functions
///
/// Its functions are defined to read specific sizes and different types of data
/// represented by bytes
pub struct BytesCursor {
    cursor: Cursor<Vec<u8>>,
}

impl BytesCursor {
    pub fn new(bytes: &[u8]) -> BytesCursor {
        BytesCursor {
            cursor: Cursor::new(bytes.to_vec()),
        }
    }

    pub fn read_u8(&mut self) -> Result<u8, Errors> {
        let mut buf = [0u8; 1];
        self.cursor
            .read_exact(&mut buf)
            .map_err(|_| Errors::ProtocolError(String::from("Could not read byte")))?;
        Ok(buf[0])
    }

    pub fn read_i16(&mut self) -> Result<i16, Errors> {
        let mut buf = [0u8; 2];
        self.cursor
            .read_exact(&mut buf)
            .map_err(|_| Errors::ProtocolError(String::from("Could not read bytes")))?;
        Ok(i16::from_be_bytes(buf))
    }

    pub fn read_u32(&mut self) -> Result<u32, Errors> {
        let mut buf = [0u8; 4];
        self.cursor
            .read_exact(&mut buf)
            .map_err(|_| Errors::ProtocolError(String::from("Could not read bytes")))?;
        Ok(u32::from_be_bytes(buf))
    }

    pub fn read_u64(&mut self) -> Result<u64, Errors> {
        let mut buf = [0u8; 8];
        self.cursor
            .read_exact(&mut buf)
            .map_err(|_| Errors::ProtocolError(String::from("Could not read bytes")))?;
        Ok(u64::from_be_bytes(buf))
    }

    pub fn read_remaining_bytes(&mut self) -> Result<Vec<u8>, Errors> {
        let mut body = Vec::new();
        self.cursor
            .read_to_end(&mut body)
            .map_err(|_| Errors::ProtocolError(String::from("Could not read bytes")))?;
        Ok(body)
    }

    pub fn read_exact(&mut self, bytes: usize) -> Result<Vec<u8>, Errors> {
        let mut buf = vec![0u8; bytes];
        self.cursor
            .read_exact(&mut buf)
            .map_err(|_| Errors::ProtocolError(String::from("Could not read bytes")))?;
        Ok(buf)
    }

    pub fn read_int(&mut self) -> Result<i32, Errors> {
        let buf = self.read_exact(4)?;
        Ok(i32::from_be_bytes(buf.try_into().map_err(|_| {
            Errors::ProtocolError(String::from("Could not read bytes"))
        })?))
    }
    pub fn read_long(&mut self) -> Result<i64, Errors> {
        let buf = self.read_exact(8)?;
        Ok(i64::from_be_bytes(buf.try_into().map_err(|_| {
            Errors::ProtocolError(String::from("Could not read bytes"))
        })?))
    }
    pub fn read_short(&mut self) -> Result<i16, Errors> {
        let buf = self.read_exact(2)?;
        Ok(i16::from_be_bytes(buf.try_into().map_err(|_| {
            Errors::ProtocolError(String::from("Could not read bytes"))
        })?))
    }
    pub fn read_string(&mut self) -> Result<String, Errors> {
        let length = self.read_short()? as usize;
        let bytes = self.read_exact(length)?;
        String::from_utf8(bytes)
            .map_err(|_| Errors::ProtocolError(String::from("Invalid UTF-8 string")))
    }

    pub fn read_long_string(&mut self) -> Result<String, Errors> {
        let length = self.read_int()? as usize;
        let bytes = self.read_exact(length)?;
        String::from_utf8(bytes)
            .map_err(|_| Errors::ProtocolError(String::from("Invalid UTF-8 string")))
    }

    pub fn read_consistency(&mut self) -> Result<ConsistencyLevel, Errors> {
        let value = self.read_short()?;
        ConsistencyLevel::from_i16(value)
    }

    pub fn read_string_map(&mut self) -> Result<HashMap<String, String>, Errors> {
        let n = self.read_short()? as usize;
        let mut map = HashMap::new();

        for _ in 0..n {
            let key = self.read_string()?;
            let value = self.read_string()?;
            map.insert(key, value);
        }
        Ok(map)
    }

    pub fn read_bytes(&mut self) -> Result<Option<Vec<u8>>, Errors> {
        let n = self.read_int()?;
        match n {
            _ if n < 0 => Ok(None),
            _ => Ok(Some(self.read_exact(n as usize)?)),
        }
    }

    pub fn read_short_bytes(&mut self) -> Result<Option<Vec<u8>>, Errors> {
        let n = self.read_short()?;
        match n {
            _ if n < 0 => Ok(None),
            _ => Ok(Some(self.read_exact(n as usize)?)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_read_u8() {
        let bytes = vec![0x01];
        let mut cursor = BytesCursor::new(&bytes);
        assert_eq!(cursor.read_u8().unwrap(), 1);
    }

    #[test]
    fn test_read_i16() {
        let bytes = vec![0x00, 0x02];
        let mut cursor = BytesCursor::new(&bytes);
        assert_eq!(cursor.read_i16().unwrap(), 2);
    }

    #[test]
    fn test_read_u32() {
        let bytes = vec![0x00, 0x00, 0x00, 0x03];
        let mut cursor = BytesCursor::new(&bytes);
        assert_eq!(cursor.read_u32().unwrap(), 3);
    }

    #[test]
    fn test_read_remaining_bytes() {
        let bytes = vec![0x01, 0x02, 0x03, 0x04];
        let mut cursor = BytesCursor::new(&bytes);
        cursor.read_u8().unwrap();
        assert_eq!(
            cursor.read_remaining_bytes().unwrap(),
            vec![0x02, 0x03, 0x04]
        );
    }

    #[test]
    fn test_read_exact() {
        let bytes = vec![0x01, 0x02, 0x03, 0x04];
        let mut cursor = BytesCursor::new(&bytes);
        assert_eq!(cursor.read_exact(2).unwrap(), vec![0x01, 0x02]);
    }

    #[test]
    fn test_read_int() {
        let bytes = vec![0x00, 0x00, 0x00, 0x12];
        let mut cursor = BytesCursor::new(&bytes);
        assert_eq!(cursor.read_int().unwrap(), 18);
    }

    #[test]
    fn test_read_long() {
        let bytes = vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x23];
        let mut cursor = BytesCursor::new(&bytes);
        assert_eq!(cursor.read_long().unwrap(), 35);
    }

    #[test]
    fn test_read_short() {
        let bytes = vec![0x00, 0x1A];
        let mut cursor = BytesCursor::new(&bytes);
        assert_eq!(cursor.read_short().unwrap(), 26);
    }

    #[test]
    fn test_read_string() {
        let bytes = vec![
            0x00, 0x0A, b'I', b'n', b'm', b'u', b't', b'a', b'b', b'l', b'e', b's', 0x02,
        ]; // length, string, extra
        let mut cursor = BytesCursor::new(&bytes);
        assert_eq!(cursor.read_string().unwrap(), "Inmutables".to_string());
    }

    #[test]
    fn test_read_long_string() {
        let bytes = vec![
            0x00, 0x00, 0x00, 0x0A, b'I', b'n', b'm', b'u', b't', b'a', b'b', b'l', b'e', b's',
            0x02,
        ]; // length, string, extra
        let mut cursor = BytesCursor::new(&bytes);
        assert_eq!(cursor.read_long_string().unwrap(), "Inmutables".to_string());
    }

    #[test]
    fn test_read_string_map() {
        let bytes = vec![
            0x00, 0x02, // n )
            0x00, 0x04, b'k', b'e', b'y', b'1', // Key 1
            0x00, 0x06, b'v', b'a', b'l', b'u', b'e', b'1', // Value 1
            0x00, 0x04, b'k', b'e', b'y', b'2', // Key 2
            0x00, 0x06, b'v', b'a', b'l', b'u', b'e', b'2', // Value 2
        ];
        let mut cursor = BytesCursor::new(&bytes);
        let mut expected = HashMap::new();
        expected.insert("key1".to_string(), "value1".to_string());
        expected.insert("key2".to_string(), "value2".to_string());
        assert_eq!(cursor.read_string_map().unwrap(), expected);
    }

    #[test]
    fn test_read_bytes() {
        let data = vec![
            0x00, 0x00, 0x00, 0x04, // n
            0x01, 0x02, 0x03, 0x04, // bytes
            0x0A, // EXTRA
        ];
        let mut cursor = BytesCursor::new(&data);
        assert_eq!(
            cursor.read_bytes().unwrap(),
            Some(vec![0x01, 0x02, 0x03, 0x04])
        );
    }

    #[test]
    fn test_read_short_bytes() {
        let data = vec![
            0x00, 0x02, // n
            0x01, 0x02, // bytes
            0x0A, // EXTRA
        ];
        let mut cursor = BytesCursor::new(&data);
        assert_eq!(cursor.read_short_bytes().unwrap(), Some(vec![0x01, 0x02]));
    }
}
