use egui::{Color32, TextEdit, Window};
use eframe::App;

use crate::cassandra_comunication::cassandra_client::CassandraClient;

pub struct LoginApp<'a> {
    user: String,
    password: String,
    error_message: Option<String>,
    is_submitted: bool,
    client: &'a mut CassandraClient,
}

impl<'a> LoginApp<'a> {
    pub fn new(client: &'a mut CassandraClient) -> Self {
        Self {
            user: String::new(),
            password: String::new(),
            error_message: None,
            is_submitted: false,
            client,
        }
    }
}

impl<'a> App for LoginApp<'a> {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        Window::new("Login")
            .resizable(false)
            .collapsible(false)
            .show(ctx, |ui| {
                ui.label("User: ");
                ui.add(TextEdit::singleline(&mut self.user).hint_text("user"));

                ui.label("Password: ");
                ui.add(TextEdit::singleline(&mut self.password).hint_text("password"));

                if let Some(error_message) = &self.error_message {
                    ui.colored_label(Color32::RED, error_message);
                }

                if self.is_submitted {
                    ui.colored_label(Color32::GREEN, "Login successful");
                }

                if ui.button("Submit").clicked() {
                    match self.client.authenticate(&self.user, &self.password){
                        Ok(_) => {
                            self.is_submitted = true;
                            self.error_message = None;
                        }
                        Err(e) => self.error_message = Some(e),
                    }
                }
            });
    }
}

pub fn run_login_app(client: &mut CassandraClient) -> Result<(), String> {
    let options = eframe::NativeOptions::default();
    eframe::run_native(
        "Flight App",
        options,
        Box::new(|_| Ok(Box::new(LoginApp::new(client)))),
    ).map_err(|e| e.to_string())
}
