use core::str;

use crate::auth::password_authenticator::PasswordAuthenticator;
use crate::executables::auth_response_executable::AuthResponseExecutable;
use crate::executables::executable::Executable;
use crate::parsers::parser::Parser;
use crate::utils::bytes_cursor::BytesCursor;
use crate::utils::errors::Errors;

pub struct AuthResponseParser;

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

        if user.is_empty() || password.is_empty() {
            return Err(Errors::ProtocolError("Invalid credentials".to_string()));
        }

        Ok((user.to_string(), password.to_string()))
    }
}
impl Parser for AuthResponseParser {
    fn parse(&self, body: &[u8]) -> Result<Box<dyn Executable>, Errors> {
        let credentials = self.valid_credentials(body)?;
        let (user, password) = self.get_user_and_password(credentials)?;

        Ok(Box::new(AuthResponseExecutable::new(
            user,
            password,
            Box::new(PasswordAuthenticator::new()),
        )))
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    const USR: &str = "my_usr";
    const PASS: &str = "my_pass";
    const ERR_MSG: &str = "Invalid credentials";
    const EMPTY: &str = "";

    fn get_body_as_vec(usr: &str, pass: &str) -> Vec<u8> {
        let usr_pass = format!("{}:{}", usr, pass);
        let usr_pass_as_bytes = usr_pass.as_bytes();
        let size = (usr_pass.len() as i32).to_be_bytes();
        let mut body = Vec::new();
        body.extend_from_slice(&size);
        body.extend_from_slice(usr_pass_as_bytes);
        body
    }

    fn get_user_and_pass(body: Vec<u8>) -> Result<(String, String), Errors> {
        let auth_parser = AuthResponseParser {};
        let credentials = auth_parser.valid_credentials(body.as_slice())?;
        auth_parser.get_user_and_password(credentials)
    }

    fn assert_with_credentials(usr: &str, pass: &str) {
        let body = get_body_as_vec(usr, pass);
        let (user, password) = get_user_and_pass(body).unwrap();

        assert_eq!(user, usr);
        assert_eq!(password, pass);
    }

    fn assert_with_credentials_and_error(usr: &str, pass: &str, err_msg: &str) {
        let body = get_body_as_vec(usr, pass);
        let res = get_user_and_pass(body);
        assert!(matches!(res, Err(Errors::ProtocolError(ref msg)) if msg == err_msg));
    }

    #[test]
    fn test_01_valid_credentials() {
        assert_with_credentials(USR, PASS)
    }
    #[test]
    fn test_02_credentials_missing_password() {
        assert_with_credentials_and_error(USR, EMPTY, ERR_MSG);
    }

    #[test]
    fn test_03_credentials_missing_user() {
        assert_with_credentials_and_error(EMPTY, PASS, ERR_MSG);
    }

    #[test]
    fn test_04_credentials_empty() {
        assert_with_credentials_and_error(EMPTY, EMPTY, ERR_MSG);
    }
}
