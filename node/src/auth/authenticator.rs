use crate::utils::errors::Errors;

pub trait Authenticator {
    fn validate_credentials(&self, user: String, password: String) -> Result<bool, Errors>;
}
