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

pub struct FrameCursor {
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
