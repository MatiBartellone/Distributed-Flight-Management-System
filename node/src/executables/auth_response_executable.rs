use crate::auth::authenticator::Authenticator;
use crate::executables::executable::Executable;
use crate::frame::Frame;
use crate::response_builders::frame_builder::FrameBuilder;
use crate::utils::errors::Errors;
use crate::utils::parser_constants::{AUTH_CHALLENGE, AUTH_SUCCESS};

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

    fn get_token(&self) -> Vec<u8> {
        Vec::<u8>::new()
    }
}

impl Executable for AuthResponseExecutable {
    fn execute(&self, request: Frame) -> Result<Frame, Errors> {
        let user = self.user.to_string();
        let password = self.password.to_string();
        let ok = self.authenticator.validate_credentials(user, password)?;

        if ok {
            let body = self.get_token();
            let ok_response = FrameBuilder::build_response_frame(request, AUTH_SUCCESS, body)?;
            return Ok(ok_response);
        }

        let response = FrameBuilder::build_response_frame(request, AUTH_CHALLENGE, Vec::new())?;
        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    struct AuthenticatorMock;
    impl AuthenticatorMock {
        fn new() -> AuthenticatorMock {
            AuthenticatorMock
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

    use super::*;
    #[test]
    fn test_01_successfull_response() {
        let request = Frame {
            version: 0x03,
            flags: 0x00,
            stream: 0x01,
            opcode: AUTH_CHALLENGE,
            length: 0x00000006,
            body: get_body_as_vec("my_usr", "my_pass"),
        };

        let authenticator = AuthenticatorMock::new();
    }
}
