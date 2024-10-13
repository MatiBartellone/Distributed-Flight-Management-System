use glib::clone;
use gtk::prelude::*;
use gtk::{Application, ApplicationWindow, Button, Box as GtkBox, Orientation, Label};
use std::rc::Rc;
use std::cell::RefCell;

const WIDTH: i32 = 500;
const HEIGHT: i32 = 500;
const BUTTON_SIZE: i32 = 30;

// TODO: Separar en archivos (model, view, controller).
// TODO: Investigar el uso de los estilos.

fn main() {
    let app = Application::builder()
        .application_id("org.example.FlightApp")
        .build();

    app.connect_activate(|app| {
        let window = ApplicationWindow::builder()
            .application(app)
            .default_width(WIDTH)
            .default_height(HEIGHT)
            .title("Flight App Skel")
            .build();

        let main_container = GtkBox::new(Orientation::Horizontal, 0);

        let flight_info_container = GtkBox::new(Orientation::Vertical, 10);
        
        let button_container = GtkBox::new(Orientation::Vertical, 0);
        button_container.set_vexpand(true);
        button_container.set_hexpand(true);

        let image = gtk::Image::from_file("img/plane.jpg");

        let button = Button::builder()
            .image(&image)
            .always_show_image(true)
            .width_request(BUTTON_SIZE)
            .height_request(BUTTON_SIZE)
            .build();

        let is_info_visible = Rc::new(RefCell::new(false));

        let is_info_visible_clone = is_info_visible.clone();
        button.connect_clicked(clone!(@weak flight_info_container, @weak main_container => move |_| {
            let mut visible = is_info_visible_clone.borrow_mut();
            if *visible {
                flight_info_container.foreach(|child| flight_info_container.remove(child));
                *visible = false;
            } else {
                // Para armar esto se usaría un Select
                let flight_info = Label::new(Some("Información del Vuelo:\nVuelo: ABC123\nDestino: XYZ\nHora: 12:30 PM"));
                flight_info_container.add(&flight_info);
                *visible = true;
            }
            main_container.show_all();
        }));

        button_container.pack_start(&button, true, false, 0);

        main_container.pack_start(&flight_info_container, false, false, 10); // Dejar espacio en la izquierda
        main_container.pack_start(&button_container, true, true, 0); // El botón debe estar centrado

        window.add(&main_container);
        
        window.show_all();
    });

    app.run();
}
