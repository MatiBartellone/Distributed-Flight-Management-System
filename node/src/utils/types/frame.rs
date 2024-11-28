use crate::utils::bytes_cursor::BytesCursor;
use crate::utils::errors::Errors;

#[derive(Debug, PartialEq, Clone)]
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
        let mut cursor = BytesCursor::new(bytes);
        let version = cursor.read_u8()?;
        let flags = cursor.read_u8()?;
        let stream = cursor.read_i16()?;
        let opcode = cursor.read_u8()?;
        let length = cursor.read_u32()?;
        let body = cursor.read_exact(length as usize)?;

        Ok(Frame {
            version,
            flags,
            stream,
            opcode,
            length,
            body,
        })
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        bytes.push(self.version);
        bytes.push(self.flags);
        bytes.extend(&self.stream.to_be_bytes());
        bytes.push(self.opcode);
        bytes.extend(&self.length.to_be_bytes());
        bytes.extend(&self.body);

        bytes
    }

    pub fn validate_request_frame(&self) -> Result<(), Errors> {
        if self.version != 0x03 {
            return Err(Errors::ProtocolError(format!(
                "Version {} is incorrect",
                self.version
            )));
        }
        if self.flags != 0x00 {
            return Err(Errors::ProtocolError(format!(
                "Flag {} is incorrect",
                self.flags
            )));
        }
        if self.stream < 0x00 {
            return Err(Errors::ProtocolError(String::from(
                "Stream cannot be negative",
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_frame() -> Result<(), Errors> {
        let bytes = vec![
            0x03, 0x00, 0x00, 0x01, 0x03, 0x00, 0x00, 0x00, 0x05, 0x10, 0x03, 0x35, 0x12, 0x22,
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
