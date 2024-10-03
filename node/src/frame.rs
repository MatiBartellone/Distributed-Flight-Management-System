use crate::utils::errors::Errors;
use std::io::{Cursor, Read};

pub struct Frame {
    pub version: u8,
    pub flags: u8,
    pub stream: i16,
    pub opcode: u8,
    pub length: u32,
    pub body: Vec<u8>,
}

impl Frame {
    pub fn parse_frame(bytes: &[u8]) -> Result<Frame, Errors> {
        let mut cursor = FrameCursor::new(bytes);
        let version = cursor.read_u8()?;
        let flags = cursor.read_u8()?;
        let stream = cursor.read_i16()?;
        let opcode = cursor.read_u8()?;
        let length = cursor.read_u32()?;
        let body = cursor.read_remaining_bytes()?;

        let frame = Frame {
            version,
            flags,
            stream,
            opcode,
            length,
            body,
        };
        frame.validate_request_frame()?;
        Ok(frame)
    }

    fn validate_request_frame(&self) -> Result<(), Errors> {
        if self.version != 0x03 { return Err(Errors::ProtocolError(format!("Version {} is incorrect", self.version))); }
        if self.flags != 0x00 { return Err(Errors::ProtocolError(format!("Flag {} is incorrect", self.flags))); }
        if self.stream < 0x00 { return Err(Errors::ProtocolError(String::from("Stream cannot be negative"))); }
        Ok(())
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
        self.cursor.read_exact(&mut buf).map_err(|_| Errors::ProtocolError(String::from("Protocol lenght is shorter than expected")))?;
        Ok(buf[0])
    }

    fn read_i16(&mut self) -> Result<i16, Errors> {
        let mut buf = [0u8; 2];
        self.cursor.read_exact(&mut buf).map_err(|_| Errors::ProtocolError(String::from("Protocol lenght is shorter than expected")))?;
        Ok(i16::from_be_bytes(buf))
    }

    fn read_u32(&mut self) -> Result<u32, Errors> {
        let mut buf = [0u8; 4];
        self.cursor.read_exact(&mut buf).map_err(|_| Errors::ProtocolError(String::from("Protocol lenght is shorter than expected")))?;
        Ok(u32::from_be_bytes(buf))
    }

    fn read_remaining_bytes(&mut self) -> Result<Vec<u8>, Errors> {
        let mut body = Vec::new();
        self.cursor.read_to_end(&mut body).map_err(|_| Errors::ProtocolError(String::from("Protocol lenght is shorter than expected")))?;
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
            0x03,
            0x00,
            0x00, 0x01,
            0x03,
            0x00, 0x00, 0x00, 0x05,
            0x10, 0x03, 0x35, 0x12, 0x22
        ];
        let result = Frame::parse_frame(&bytes)?;
        assert_eq!(result.version, 3);
        assert_eq!(result.flags, 0);
        assert_eq!(result.stream, 1);
        assert_eq!(result.opcode, 3);
        assert_eq!(result.length, 5);
        assert_eq!(result.body, vec![16, 3, 53, 18, 34]);
        Ok(())
    }
}
