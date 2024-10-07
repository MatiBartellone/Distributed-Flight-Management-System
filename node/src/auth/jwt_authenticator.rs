use crate::utils::errors::Errors;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::Read;

use super::authenticator::Authenticator;

const CREDENTIALS: &str = "src/auth/credentials.json";

#[derive(Serialize, Deserialize, Debug)]
struct Credential {
    user: String,
    pass: String,
}

pub struct JWTAuthenticator;

impl Authenticator for JWTAuthenticator {
    fn validate_credentials(&self, user: String, pass: String) -> Result<bool, Errors> {
        self.access_credentials(&user, &pass)
    }
}
impl Default for JWTAuthenticator {
    fn default() -> Self {
        Self::new()
    }
}

impl JWTAuthenticator {
    pub fn new() -> JWTAuthenticator {
        JWTAuthenticator {}
    }

    fn access_credentials(&self, user: &str, pass: &str) -> Result<bool, Errors> {
        let mut file = File::open(CREDENTIALS)
            .map_err(|_| Errors::ServerError(String::from("Failed to validate credentials")))?;
        let mut data = String::new();
        file.read_to_string(&mut data)
            .map_err(|_| Errors::ServerError(String::from("Failed to validate credentials")))?;

        let credentials: Result<Vec<Credential>, Errors> = serde_json::from_str(&data)
            .map_err(|_| Errors::ServerError(String::from("Failed to validate credentials")));

        Ok(credentials?
            .into_iter()
            .any(|cred| cred.user == user && cred.pass == pass))
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    // LOS TESTS SE BASAN EN QUE EXISTA EL USER "admin" y PASS "password"
    #[test]
    fn test_valid_credentials() {
        let authenticator = JWTAuthenticator::new();
        let result =
            authenticator.validate_credentials("admin".to_string(), "password".to_string());
        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[test]
    fn test_invalid_password() {
        let authenticator = JWTAuthenticator::new();
        let result =
            authenticator.validate_credentials("admin".to_string(), "invalid_password".to_string());
        assert!(result.is_ok());
        assert!(!result.unwrap());
    }

    #[test]
    fn test_invalid_username() {
        let authenticator = JWTAuthenticator::new();
        let result =
            authenticator.validate_credentials("invalid_user".to_string(), "password".to_string());
        assert!(result.is_ok());
        assert!(!result.unwrap());
    }
}
