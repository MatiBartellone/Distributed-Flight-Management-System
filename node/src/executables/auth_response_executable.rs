use crate::auth::authenticator::Authenticator;
use crate::executables::executable::Executable;
use crate::frame::Frame;
use crate::response_builders::frame_builder::FrameBuilder;

const AUTH_SUCCESS: u8 = 0x10;
const AUTH_CHALLENGE: u8 = 0x0E;
pub struct AuthResponseExecutable {
    user: String,
    password: String,
}

impl AuthResponseExecutable {
    pub fn new(user: String, password: String) -> AuthResponseExecutable {
        AuthResponseExecutable { user, password }
    }

    fn get_token(&self) -> Vec<u8> {
        Vec::<u8>::new()
    }
}

impl Executable for AuthResponseExecutable {
    fn execute(&self, request: Frame) -> Frame {
        // TODO: Sacar el unwrap
        let user = self.user.to_string();
        let password = self.password.to_string();
        let ok = Authenticator::validate_credentials(user, password).unwrap();
        if ok {
            let body = self.get_token();
            return FrameBuilder::build_response_frame(request, AUTH_SUCCESS, body).unwrap();
        }
        FrameBuilder::build_response_frame(request, AUTH_CHALLENGE, Vec::new()).unwrap()
    }
}
