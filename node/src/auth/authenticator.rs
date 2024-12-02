use crate::utils::errors::Errors;

pub trait Authenticator {
    fn validate_credentials(&self, user: String, password: String) -> Result<bool, Errors>;
    // fn create_user(&self, user: &str, pass: &str) -> Result<(), Errors>;
    // fn change_password(&self, user: &str, password: &str, new_password: &str) -> Result<(), Errors>;
}
