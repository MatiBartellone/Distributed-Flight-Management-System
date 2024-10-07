use core::str;

use crate::executables::auth_response_executable::AuthResponseExecutable;
use crate::executables::executable::Executable;
use crate::parsers::parser::Parser;
use crate::utils::bytes_cursor::BytesCursor;
use crate::utils::errors::Errors;

pub struct AuthResponseParser {}

impl AuthResponseParser {
    fn valid_credentials(&self, body: &[u8]) -> Result<Option<Vec<u8>>, Errors> {
        let mut cursor = BytesCursor::new(body);
        let credentials_as_bytes = cursor.read_bytes()?;
        cursor.read_remaining_bytes()?;
        Ok(credentials_as_bytes)
    }

    fn get_user_and_password(
        &self,
        credentials: Option<Vec<u8>>,
    ) -> Result<(String, String), Errors> {
        let credentials =
            credentials.ok_or(Errors::ProtocolError("Invalid credentials".to_string()))?;
        let credentials = str::from_utf8(&credentials)
            .map_err(|_| Errors::ProtocolError("Invalid credentials".to_string()))?;
        let mut credentials_split = credentials.split(':');

        let user = credentials_split.next().ok_or(Errors::ProtocolError(
            "Missing user in credentials".to_string(),
        ))?;
        let password = credentials_split.next().ok_or(Errors::ProtocolError(
            "Missing password in credentials".to_string(),
        ))?;

        Ok((user.to_string(), password.to_string()))
    }
}
impl Parser for AuthResponseParser {
    fn parse(&self, body: &[u8]) -> Result<Box<dyn Executable>, Errors> {
        let credentials = self.valid_credentials(body)?;
        let (user, password) = self.get_user_and_password(credentials)?;

        Ok(Box::new(AuthResponseExecutable::new(user, password)))
    }
}
