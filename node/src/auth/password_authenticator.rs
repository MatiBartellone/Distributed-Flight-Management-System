use super::authenticator::Authenticator;
use crate::utils::errors::Errors;
use crate::utils::functions::deserialize_from_str;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::Read;

use argon2::{
    password_hash::{rand_core::OsRng, PasswordHasher, SaltString},
    Argon2, PasswordHash, PasswordVerifier,
};

const CREDENTIALS: &str = "src/auth/credentials.json";

#[derive(Serialize, Deserialize, Debug)]
struct Credential {
    user: String,
    pass_hash: String,
}

/// PasswordAuthenticator uses user and password to validate credentials
pub struct PasswordAuthenticator;

impl Authenticator for PasswordAuthenticator {
    fn validate_credentials(&self, user: String, pass: String) -> Result<bool, Errors> {
        let credentials = self.get_credentials()?;
        for credential in credentials {
            if credential.user == user {
                return verify_password(&pass, &credential.pass_hash);
            }
        }
        Ok(false)
    }
}

impl Default for PasswordAuthenticator {
    fn default() -> Self {
        Self::new()
    }
}

impl PasswordAuthenticator {
    pub fn new() -> PasswordAuthenticator {
        PasswordAuthenticator {}
    }

    fn default_credentials(&self) -> Result<Vec<Credential>, Errors> {
        let credentials = vec![Credential {
            user: "admin".to_string(),
            pass_hash: hash_password("password")?,
        }];
        Ok(credentials)
    }

    fn save_credentials(&self, credentials: Vec<Credential>) -> Result<(), Errors> {
        let data = serde_json::to_string(&credentials)
            .map_err(|_| Errors::ServerError(String::from("Failed to save credentials")))?;
        std::fs::write(CREDENTIALS, data)
            .map_err(|_| Errors::ServerError(String::from("Failed to save credentials")))?;
        Ok(())
    }

    fn get_file(&self) -> Result<File, Errors> {
        let file = match File::open(CREDENTIALS) {
            Ok(file) => file,
            Err(_) => {
                let credentials = self.default_credentials()?;
                self.save_credentials(credentials)?;
                File::open(CREDENTIALS)
                    .map_err(|_| Errors::ServerError(String::from("Failed to open credentials")))?
            }
        };
        Ok(file)
    }

    fn get_credentials(&self) -> Result<Vec<Credential>, Errors> {
        let mut file = self.get_file()?;
        let mut data = String::new();
        file.read_to_string(&mut data)
            .map_err(|_| Errors::ServerError(String::from("Failed to read credentials")))?;

        let credentials: Result<Vec<Credential>, Errors> = deserialize_from_str(&data);
        credentials
    }

    pub fn create_user(&self, user: &str, pass: &str) -> Result<(), Errors> {
        let mut credentials = self.get_credentials()?;
        for credential in &credentials {
            if credential.user == user {
                return Ok(()); // User already exists
            }
        }

        let pass_hash = hash_password(pass)?;
        credentials.push(Credential {
            user: user.to_string(),
            pass_hash,
        });

        self.save_credentials(credentials)?;
        Ok(())
    }

    pub fn change_password(
        &self,
        user: &str,
        password: &str,
        new_password: &str,
    ) -> Result<(), Errors> {
        let mut credentials = self.get_credentials()?;
        for credential in &mut credentials {
            if credential.user == user && verify_password(password, &credential.pass_hash)? {
                credential.pass_hash = hash_password(new_password)?;
                self.save_credentials(credentials)?;
                return Ok(());
            }
        }
        Ok(())
    }
}

fn hash_password(password: &str) -> Result<String, Errors> {
    let salt = SaltString::generate(&mut OsRng);
    let argon2 = Argon2::default();
    let password_hash = argon2
        .hash_password(password.as_bytes(), &salt)
        .map_err(|_| Errors::ServerError(String::from("Failed to hash password")))?;
    Ok(password_hash.to_string())
}

fn verify_password(password: &str, hash: &str) -> Result<bool, Errors> {
    let argon2 = Argon2::default();
    let parsed_hash = PasswordHash::new(hash)
        .map_err(|_| Errors::ServerError(String::from("Failed to parse password hash")))?;
    Ok(argon2
        .verify_password(password.as_bytes(), &parsed_hash)
        .is_ok())
}

#[cfg(test)]
mod tests {

    use super::*;

    // LOS TESTS SE BASAN EN QUE EXISTA EL USER "admin" y PASS "password"

    #[test]
    fn test_new_client() {
        let authenticator = PasswordAuthenticator::new();
        let result = authenticator.create_user("admin", "password");
        assert!(result.is_ok());
    }

    #[test]
    fn test_valid_credentials() {
        let authenticator = PasswordAuthenticator::new();
        let result =
            authenticator.validate_credentials("admin".to_string(), "password".to_string());
        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[test]
    fn test_invalid_password() {
        let authenticator = PasswordAuthenticator::new();
        let result =
            authenticator.validate_credentials("admin".to_string(), "invalid_password".to_string());
        assert!(result.is_ok());
        assert!(!result.unwrap());
    }

    #[test]
    fn test_invalid_username() {
        let authenticator = PasswordAuthenticator::new();
        let result =
            authenticator.validate_credentials("invalid_user".to_string(), "password".to_string());
        assert!(result.is_ok());
        assert!(!result.unwrap());
    }

    #[test]
    fn test_verify_hashing_password() {
        let password = "password";
        let hashed_password = hash_password(password).unwrap();
        let result = verify_password("password", &hashed_password).unwrap();
        assert!(result);
    }

    #[test]
    fn test_verify_hashing_password_invalid() {
        let password = "password";
        let hashed_password = hash_password(password).unwrap();
        let result = verify_password("invalid_password", &hashed_password).unwrap();
        assert!(!result);
    }
}
