use crate::frame::Frame;
use crate::utils::errors::Errors;

pub struct FrameBuilder;
impl FrameBuilder {
    pub fn build_response_frame(request_frame: Frame, opcode: u8 , new_body: Vec<u8>) -> Result<Frame, Errors> {
        let response_version = request_frame.version | 0x80;
        let length = new_body.len() as u32;
        let mut new_bytes: Vec<u8> = Vec::new();
        new_bytes.push(response_version);
        new_bytes.push(request_frame.flags);
        new_bytes.extend_from_slice(&request_frame.stream.to_be_bytes());
        new_bytes.push(opcode);
        new_bytes.extend_from_slice(&length.to_be_bytes());
        new_bytes.extend(new_body);
        Frame::parse_frame(&new_bytes)
    }
}

#[cfg(test)]
mod tests {
    use crate::utils::parser_constants::{QUERY, RESULT};
    use super::*;
    #[test]
    fn test_build_response_frame() {
        let request_bytes = vec![
            0x03,
            0x00,
            0x00, 0x01,
            QUERY,
            0x00, 0x00, 0x00, 0x05,
            0x10, 0x03, 0x35, 0x12, 0x22
        ];
        let request_frame = Frame::parse_frame(&request_bytes).unwrap();
        let new_body = vec![0x0A, 0x2B, 0x05, 0x45, 0x61, 0x6E];
        let expected = Frame {
            version: 0x83,
            flags: 0x00,
            stream: 0x01,
            opcode: RESULT,
            length: 0x00000006,
            body: vec![0x0A, 0x2B, 0x05, 0x45, 0x61, 0x6E]
        };
        assert_eq!(expected, FrameBuilder::build_response_frame(request_frame, RESULT, new_body).unwrap())
    }
}