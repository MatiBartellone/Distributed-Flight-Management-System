use std::fs::OpenOptions;
use std::io::Write;
use std::sync::{Arc, Mutex};
use crate::utils::types::timestamp::Timestamp;

pub struct Logger {
    log_file: Arc<Mutex<std::fs::File>>,
}

impl Logger {
    pub fn new(file_path: &str) -> Logger {
        let log_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(file_path)
            .unwrap();
        
        Logger {
            log_file: Arc::new(Mutex::new(log_file)),
        }
    }

    fn write_log(&self, log_message: &str) {
        let mut file = match self.log_file.lock() {
            Ok(f) => f,
            Err(e) => {
                println!("Error al bloquear el archivo de log: {}", e);
                return;
            }
        };
    
        let timestamp = Timestamp::new();
        let log_entry = format!("[{}] {}", timestamp, log_message);
    
        if let Err(e) = writeln!(file, "{}", log_entry) {
            println!("Error al escribir en el archivo de log: {}", e);
        }
        println!("[{}] {}", timestamp, log_message);
    }

    pub fn log_message(&self, message: &str) {
        self.write_log(&format!("[INFO] {}", message));
    }

    pub fn log_response(&self, response: &str) {
        self.write_log(&format!("[RESPONSE] {}", response));
    }

    pub fn log_error(&self, error: &str) {
        self.write_log(&format!("[ERROR] {}", error));
    }
}
