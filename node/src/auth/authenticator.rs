use std::fs::File;
use std::io::Read;
use serde::{Deserialize, Serialize};
use crate::utils::errors::Errors;

const CREDENTIALS: &str = "src/auth/credentials.json";

#[derive(Serialize, Deserialize, Debug)]
struct Credential {
    user: String,
    pass: String,
}

pub struct Authenticator;

impl Authenticator {
    pub fn validate_credentials(user: String, pass: String) -> Result<bool, Errors> {
        access_credentials(&user, &pass)
    }
}

fn access_credentials(user: &str, pass: &str) -> Result<bool, Errors> {
    let mut file = File::open(CREDENTIALS).map_err(|_| Errors::ServerError(String::from("Failed to validate credentials")))?;
    let mut data = String::new();
    file.read_to_string(&mut data).map_err(|_| Errors::ServerError(String::from("Failed to validate credentials")))?;

    let credentials: Result<Vec<Credential>, Errors> = serde_json::from_str(&data).map_err(|_| Errors::ServerError(String::from("Failed to validate credentials")));

    Ok(credentials?.into_iter().any(|cred| cred.user == user && cred.pass == pass))
}

#[cfg(test)]
mod tests {
    use super::*;

    // LOS TESTS SE BASAN EN QUE EXISTA EL USER "admin" y PASS "password"
    #[test]
    fn test_valid_credentials() {
        let result = Authenticator::validate_credentials("admin".to_string(), "password".to_string());
        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[test]
    fn test_invalid_password() {
        let result = Authenticator::validate_credentials("admin".to_string(), "invalid_password".to_string());
        assert!(result.is_ok());
        assert!(!result.unwrap());
    }

    #[test]
    fn test_invalid_username() {
        let result = Authenticator::validate_credentials("invalid_user".to_string(), "password".to_string());
        assert!(result.is_ok());
        assert!(!result.unwrap());
    }
}
