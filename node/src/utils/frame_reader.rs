use crate::utils::types::frame::Frame;
use crate::utils::errors::Errors;
use crate::utils::types::bytes_cursor::BytesCursor;
use crate::utils::parser_constants::{ERROR, AUTHENTICATE, AUTH_SUCCESS, AUTH_CHALLENGE, SUPPORTED, RESULT};

pub struct FrameReader;

impl FrameReader {
    pub fn read_frame(frame: Frame) -> Result<String, Errors> {
        let mut response = String::new();
        match frame.opcode {
            ERROR => {
                let mut cursor = BytesCursor::new(frame.body.as_slice());
                let error_type = vec![cursor.read_u8().unwrap(), cursor.read_u8().unwrap()];
                let msg = cursor.read_string().unwrap();
                let error = Errors::new(error_type.as_slice(), msg);
                response = format!("{}", error);
            }
            AUTHENTICATE => {
                let mut cursor = BytesCursor::new(frame.body.as_slice());
                response += format!("AUTHENTICATE\n{}", cursor.read_string()?).as_str();
            }
            AUTH_SUCCESS => {
                response += "AUTH_SUCCESS";
            }
            AUTH_CHALLENGE => {
                response += "AUTH_CHALLENGE";
            }
            SUPPORTED => {
                let mut cursor = BytesCursor::new(frame.body.as_slice());
                response += "SUPPORTED";
                for (key, value) in cursor.read_string_map().unwrap() {
                    response += format!("\n{}: {}", key, value).as_str();
                }
            }
            RESULT => {
                response = Self::show_response(frame.body).unwrap();
            }
            _ => {}
        }
        Ok(response)
    }

    fn show_response(body: Vec<u8>) -> Result<String, Errors> {
        let mut cursor = BytesCursor::new(body.as_slice());
        let mut response = String::new();
        match cursor.read_int()?{
            1 => {
                Ok("Operation was succesful".to_string())
            },
            3 => {
                Ok("Use keyspace was succesful".to_string())
            },
            5 => {
                let change = cursor.read_string()?;
                let target = cursor.read_string()?;
                let option = cursor.read_string()?;
                Ok(format!("Operation was succesful, change: {}, target: {}, option: {}", change, target, option))
            },
            2 => {
                let _ = cursor.read_int()?;
                let col_count = cursor.read_int()?;
                let keyspace = cursor.read_string()?;
                let table = cursor.read_string()?;
                response += format!("Rows from keyspace: {} and table {}:", keyspace, table).as_str();
                let mut header = String::new();
                for i in 0..col_count {
                    if i != 0 {
                        header += ", ";
                    }
                    let col_name = cursor.read_string()?;
                    let _ = cursor.read_i16()?;
                    header += &col_name;
                }
                response += format!("\n{}", header).as_str();
                let row_count = cursor.read_int()?;
                for _ in 0..row_count {
                    let mut row = String::new();
                    for i in 0..col_count {
                        if i != 0 {
                            row += ", ";
                        }
                        let value = cursor.read_string()?;
                        row += &value;
                    }
                    response += format!("\n{}", row).as_str();
                }
                Ok(response)
            }
            _ => {Ok("".to_string())}
        }
    }
}