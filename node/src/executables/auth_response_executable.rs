use crate::auth::authenticator::Authenticator;
use crate::executables::executable::Executable;
use crate::response_builders::frame_builder::FrameBuilder;
use crate::utils::errors::Errors;
use crate::utils::errors::Errors::BadCredentials;
use crate::utils::parser_constants::AUTH_SUCCESS;
use crate::utils::types::frame::Frame;

pub struct AuthResponseExecutable {
    user: String,
    password: String,
    authenticator: Box<dyn Authenticator>,
}

impl AuthResponseExecutable {
    pub fn new(
        user: String,
        password: String,
        authenticator: Box<dyn Authenticator>,
    ) -> AuthResponseExecutable {
        AuthResponseExecutable {
            user,
            password,
            authenticator,
        }
    }
}

impl Executable for AuthResponseExecutable {
    fn execute(&mut self, request: Frame) -> Result<Frame, Errors> {
        let user = self.user.to_string();
        let password = self.password.to_string();
        let ok = self.authenticator.validate_credentials(user, password)?;
        if !ok {
            return Err(BadCredentials(String::from("Invalid credentials provided")));
        }
        FrameBuilder::build_response_frame(request, AUTH_SUCCESS, Vec::new())
    }
}

#[cfg(test)]
mod tests {
    const VALID_USER: &str = "my_usr";
    const VALID_PASS: &str = "my_pass";
    const INVALID_PASS: &str = "invalid_pass";
    const INVALID_USER: &str = "invalid_user";

    struct AuthenticatorMock;
    impl AuthenticatorMock {
        fn new() -> AuthenticatorMock {
            AuthenticatorMock
        }
    }

    impl Authenticator for AuthenticatorMock {
        fn validate_credentials(&self, user: String, password: String) -> Result<bool, Errors> {
            if user == "my_usr" && password == "my_pass" {
                return Ok(true);
            }
            Ok(false)
        }
    }

    fn get_body_as_vec(usr: &str, pass: &str) -> Vec<u8> {
        let usr_pass = format!("{}:{}", usr, pass);
        let usr_pass_as_bytes = usr_pass.as_bytes();
        let size = (usr_pass.len() as i32).to_be_bytes();
        let mut body = Vec::new();
        body.extend_from_slice(&size);
        body.extend_from_slice(usr_pass_as_bytes);
        body
    }

    fn build_request_with(user: &str, pass: &str) -> Frame {
        let body = get_body_as_vec(user, pass);
        let size = body.len() as u32;
        Frame {
            version: 0x03,
            flags: 0x00,
            stream: 0x01,
            opcode: AUTH_CHALLENGE,
            length: size,
            body,
        }
    }

    fn assert_ok(user: &str, pass: &str, expected_opcode: u8, expected_body: Vec<u8>) {
        let request = build_request_with(user, pass);
        let authenticator = AuthenticatorMock::new();
        let mut executable = AuthResponseExecutable::new(
            user.to_string(),
            pass.to_string(),
            Box::new(authenticator),
        );
        let response = executable.execute(request);
        assert!(response.is_ok());
        let frame = response.unwrap();
        assert_eq!(frame.opcode, expected_opcode);
        assert_eq!(frame.body, expected_body);
    }

    fn assert_err(user: &str, pass: &str) {
        let request = build_request_with(user, pass);
        let authenticator = AuthenticatorMock::new();
        let mut executable = AuthResponseExecutable::new(
            user.to_string(),
            pass.to_string(),
            Box::new(authenticator),
        );
        let response = executable.execute(request);
        assert!(response.is_err());
    }

    use super::*;
    use crate::utils::parser_constants::AUTH_CHALLENGE;
    #[test]
    fn test_01_successfull_response() {
        // En este test debería cambiar el body con los tokens si no me equivoco
        assert_ok(VALID_USER, VALID_PASS, AUTH_SUCCESS, Vec::new());
    }

    #[test]
    fn test_02_unsuccessfull_response_with_invalid_pass() {
        assert_err(VALID_USER, INVALID_PASS);
    }

    #[test]
    fn test_03_unsuccessfull_response_with_invalid_user() {
        assert_err(INVALID_USER, VALID_PASS);
    }

    #[test]
    fn test_04_unsuccessfull_response_with_invalid_user_and_password() {
        assert_err(INVALID_USER, INVALID_PASS);
    }
}
