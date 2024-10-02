use crate::utils::errors::Errors;
use std::io::{Cursor, Read};

pub struct Frame {
    version: u8,
    flags: u8,
    stream: i16,
    opcode: u8,
    length: u32,
    body: Vec<u8>,
}

impl Frame {
    fn parse_frame(bytes: &[u8]) -> Result<Frame, Errors> {
        let mut cursor = FrameCursor::new(bytes);
        let version = cursor.read_u8()?;
        let flags = cursor.read_u8()?;
        let stream = cursor.read_i16()?;
        let opcode = cursor.read_u8()?;
        let length = cursor.read_u32()?;
        let body = cursor.read_remaining_bytes()?;

        Ok(Frame {
            version,
            flags,
            stream,
            opcode,
            length,
            body,
        })
    }
}

struct FrameCursor {
    cursor: Cursor<Vec<u8>>,
}

impl FrameCursor {
    fn new(bytes: &[u8]) -> FrameCursor {
        FrameCursor {
            cursor: Cursor::new(bytes.to_vec()),
        }
    }

    fn read_u8(&mut self) -> Result<u8, Errors> {
        let mut buf = [0u8; 1];
        self.cursor.read_exact(&mut buf).map_err(|_| Errors::ProtocolError(format!("")))?;
        Ok(buf[0])
    }

    fn read_i16(&mut self) -> Result<i16, Errors> {
        let mut buf = [0u8; 2];
        self.cursor.read_exact(&mut buf).map_err(|_| Errors::ProtocolError(format!("")))?;
        Ok(i16::from_be_bytes(buf))
    }

    fn read_u32(&mut self) -> Result<u32, Errors> {
        let mut buf = [0u8; 4];
        self.cursor.read_exact(&mut buf).map_err(|_| Errors::ProtocolError(format!("")))?;
        Ok(u32::from_be_bytes(buf))
    }

    fn read_remaining_bytes(&mut self) -> Result<Vec<u8>, Errors> {
        let mut body = Vec::new();
        self.cursor.read_to_end(&mut body).map_err(|_| Errors::ProtocolError(format!("")))?;
        Ok(body)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_u8() -> Result<(), Errors> {
        let bytes = vec![0x01];
        let mut cursor = FrameCursor::new(&bytes);
        let result = cursor.read_u8()?;
        assert_eq!(result, 0x01);
        Ok(())
    }

    #[test]
    fn test_read_i16() -> Result<(), Errors> {
        let bytes = vec![0x12, 0x26];
        let mut cursor = FrameCursor::new(&bytes);
        let result = cursor.read_i16()?;
        assert_eq!(result, i16::from_be_bytes([0x12, 0x26]));
        Ok(())
    }

    #[test]
    fn test_read_u32() -> Result<(), Errors> {
        let bytes = vec![0x13, 0x32, 0x51, 0x75];
        let mut cursor = FrameCursor::new(&bytes);
        let result = cursor.read_u32()?;
        assert_eq!(result, u32::from_be_bytes([0x13, 0x32, 0x51, 0x75]));
        Ok(())
    }

    #[test]
    fn test_read_remaining_bytes() -> Result<(), Errors> {
        let bytes = vec![0x01, 0x02, 0x03, 0x04];
        let mut cursor = FrameCursor::new(&bytes);
        cursor.read_u8()?;
        let result = cursor.read_remaining_bytes()?;
        assert_eq!(result, vec![0x02, 0x03, 0x04]);
        Ok(())
    }

    #[test]
    fn test_parse_frame() -> Result<(), Errors> {
        let bytes = vec![
            0x04,
            0x00,
            0x00, 0x01,
            0x03,
            0x00, 0x00, 0x00, 0x05,
            0x10, 0x03, 0x35, 0x12, 0x22
        ];
        let result = Frame::parse_frame(&bytes)?;
        assert_eq!(result.version, 4);
        assert_eq!(result.flags, 0);
        assert_eq!(result.stream, 1);
        assert_eq!(result.opcode, 3);
        assert_eq!(result.length, 5);
        assert_eq!(result.body, vec![16, 3, 53, 18, 34]);
        Ok(())
    }
}
