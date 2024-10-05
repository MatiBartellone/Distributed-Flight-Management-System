use std::fs::File;
use std::io::Read;
use serde::{Deserialize, Serialize};
use crate::utils::errors::Errors;

const CREDENTIALS: &str = "credentials.json";

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
    let mut file = File::open(CREDENTIALS).map_err(|_| Errors::ServerError(String::from("Failed to open credentials file")))?;
    let mut data = String::new();
    file.read_to_string(&mut data).map_err(|_| Errors::ServerError(String::from("Failed to read credentials file")))?;

    let credentials: Result<Vec<Credential>, Errors> = serde_json::from_str(&data).map_err(|_| Errors::ServerError(String::from("Failed to parse credentials file")));

    Ok(credentials?.into_iter().any(|cred| cred.user == user && cred.pass == pass))
}
