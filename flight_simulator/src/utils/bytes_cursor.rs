use std::collections::HashMap;
use std::io::{Cursor, Read};

use super::consistency_level::ConsistencyLevel;
use super::constants::READ_ERROR;

pub struct BytesCursor {
    cursor: Cursor<Vec<u8>>,
}

impl BytesCursor {
    pub fn new(bytes: &[u8]) -> BytesCursor {
        BytesCursor {
            cursor: Cursor::new(bytes.to_vec()),
        }
    }

    pub fn read_u8(&mut self) -> Result<u8, String> {
        let mut buf = [0u8; 1];
        self.cursor.read_exact(&mut buf).map_err(|_| READ_ERROR)?;
        Ok(buf[0])
    }

    pub fn read_i16(&mut self) -> Result<i16, String> {
        let mut buf = [0u8; 2];
        self.cursor.read_exact(&mut buf).map_err(|_| READ_ERROR)?;
        Ok(i16::from_be_bytes(buf))
    }

    pub fn read_u32(&mut self) -> Result<u32, String> {
        let mut buf = [0u8; 4];
        self.cursor.read_exact(&mut buf).map_err(|_| READ_ERROR)?;
        Ok(u32::from_be_bytes(buf))
    }

    pub fn read_remaining_bytes(&mut self) -> Result<Vec<u8>, String> {
        let mut body = Vec::new();
        self.cursor.read_to_end(&mut body).map_err(|_| READ_ERROR)?;
        Ok(body)
    }

    pub fn read_exact(&mut self, bytes: usize) -> Result<Vec<u8>, String> {
        let mut buf = vec![0u8; bytes];
        self.cursor.read_exact(&mut buf).map_err(|_| READ_ERROR)?;
        Ok(buf)
    }

    pub fn read_int(&mut self) -> Result<i32, String> {
        let buf = self.read_exact(4)?;
        Ok(i32::from_be_bytes(buf.try_into().map_err(|_| READ_ERROR)?))
    }
    pub fn read_long(&mut self) -> Result<i64, String> {
        let buf = self.read_exact(8)?;
        Ok(i64::from_be_bytes(buf.try_into().map_err(|_| READ_ERROR)?))
    }
    pub fn read_short(&mut self) -> Result<i16, String> {
        let buf = self.read_exact(2)?;
        Ok(i16::from_be_bytes(buf.try_into().map_err(|_| READ_ERROR)?))
    }
    pub fn read_string(&mut self) -> Result<String, String> {
        let length = self.read_short()? as usize;
        let bytes = self.read_exact(length)?;
        Ok(String::from_utf8(bytes).map_err(|_| READ_ERROR)?)
    }

    pub fn read_long_string(&mut self) -> Result<String, String> {
        let length = self.read_int()? as usize;
        let bytes = self.read_exact(length)?;
        Ok(String::from_utf8(bytes).map_err(|_| READ_ERROR)?)
    }

    pub fn read_consistency(&mut self) -> Result<ConsistencyLevel, String> {
        let value = self.read_short()?;
        ConsistencyLevel::from_i16(value)
    }

    pub fn read_string_map(&mut self) -> Result<HashMap<String, String>, String> {
        let n = self.read_short()? as usize;
        let mut map = HashMap::new();

        for _ in 0..n {
            let key = self.read_string()?;
            let value = self.read_string()?;
            map.insert(key, value);
        }
        Ok(map)
    }

    pub fn read_bytes(&mut self) -> Result<Option<Vec<u8>>, String> {
        let n = self.read_int()?;
        match n {
            _ if n < 0 => Ok(None),
            _ => Ok(Some(self.read_exact(n as usize)?)),
        }
    }

    pub fn read_short_bytes(&mut self) -> Result<Option<Vec<u8>>, String> {
        let n = self.read_short()?;
        match n {
            _ if n < 0 => Ok(None),
            _ => Ok(Some(self.read_exact(n as usize)?)),
        }
    }
}
