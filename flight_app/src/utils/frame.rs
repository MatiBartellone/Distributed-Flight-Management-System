use super::{bytes_cursor::BytesCursor, types_to_bytes::TypesToBytes};

#[derive(Debug, PartialEq)]
pub struct Frame {
    pub version: u8,
    pub flags: u8,
    pub stream: i16,
    pub opcode: u8,
    pub length: u32,
    pub body: Vec<u8>,
}

impl Frame {
    // El unico que ya lo recibe en bytes es el body porque varia el formato
    pub fn new(
        version: u8,
        flags: u8,
        stream: i16,
        opcode: u8,
        length: u32,
        body: Vec<u8>,
    ) -> Self {
        Frame {
            version,
            flags,
            stream,
            opcode,
            length,
            body,
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, String> {
        let mut types_to_bytes = TypesToBytes::default();
        types_to_bytes.write_u8(self.version)?;
        types_to_bytes.write_u8(self.flags)?;
        types_to_bytes.write_i16(self.stream)?;
        types_to_bytes.write_u8(self.opcode)?;
        types_to_bytes.write_u32(self.length)?;
        types_to_bytes.write_bytes(&self.body);
        Ok(types_to_bytes.into_bytes())
    }

    pub fn parse_frame(bytes: &[u8]) -> Result<Frame, String> {
        let mut cursor = BytesCursor::new(bytes);
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
        Ok(frame)
    }

    pub fn validate_request_frame(&self) -> Result<(), String> {
        if self.version != 0x03 {
            return Err(format!("Version {} is incorrect", self.version));
        }
        if self.flags != 0x00 {
            return Err(format!("Flag {} is incorrect", self.flags));
        }
        if self.stream < 0x00 {
            return Err("Stream cannot be negative".to_string());
        }
        Ok(())
    }
}
