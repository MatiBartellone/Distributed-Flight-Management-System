use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::thread::{sleep, self};
use std::time::Duration;

use node::frame::Frame;
use node::meta_data::keyspaces::keyspace::Keyspace;
use node::meta_data::keyspaces::keyspace_meta_data_acces::KeyspaceMetaDataAccess;
use node::parsers::parser_factory::ParserFactory;
use node::utils::errors::Errors;

fn main() -> Result<(), Errors> {
    /*let bytes = vec![
        0x03, 0x00, 0x00, 0x01, 0x03, 0x00, 0x00, 0x00, 0x05, 0x10, 0x03, 0x35, 0x12, 0x22,
    ];
    let frame = Frame::parse_frame(bytes.as_slice())?;
    frame.validate_request_frame()?;
    let parser = ParserFactory::get_parser(frame.opcode)?;
    let executable = parser.parse(frame.body.as_slice())?;
    executable.execute(frame)?;*/
    main3()
    //Ok(())
}


//de esta manera no se lockean
fn main2() -> Result<(), Errors> {
    let path = "node/src/meta_data/keyspaces/testing.json";
    let _ = create_json_file(path);
    thread::sleep(Duration::from_secs(5));
    let meta_data = KeyspaceMetaDataAccess::new(path)?;
    meta_data.add_keyspace("name", None, None)?;
    thread::sleep(Duration::from_secs(5));
    // Crear el primer hilo
    let handle1 = thread::spawn(move || {
        let meta_data = KeyspaceMetaDataAccess::new(path);
        match meta_data {
            Ok(meta) => {
                if let Err(e) = meta.add_keyspace("nombre", None, None) {
                    eprintln!("Error en hilo 1: {:?}", e);
                    return Err(e); // Propagar el error si ocurre
                }
                println!("Sali soy hilo 1");
                Ok(())
            }
            Err(e) => {
                eprintln!("Error al inicializar meta_data en hilo 1: {:?}", e);
                Err(e)
            }
        }
    });
    
    // Crear el segundo hilo
    let handle2 = thread::spawn(move || {
        let meta_data = KeyspaceMetaDataAccess::new(path);
        match meta_data {
            Ok(meta) => {
                if let Err(e) = meta.drop_keyspace("name") {
                    eprintln!("Error en hilo 2: {:?}", e);
                    return Err(e);
                }
                println!("Sali soy hilo 2");
                Ok(())
            }
            Err(e) => {
                eprintln!("Error al inicializar meta_data en hilo 2: {:?}", e);
                Err(e)
            }
        }
    });

    // Esperar a que ambos hilos terminen
    if let Err(e) = handle1.join().expect("Error al unir el hilo 1") {
        eprintln!("Error en hilo 1 (desde el join): {:?}", e);
    }

    if let Err(e) = handle2.join().expect("Error al unir el hilo 2") {
        eprintln!("Error en hilo 2 (desde el join): {:?}", e);
    }

    println!("Los hilos han terminado.");
    Ok(())
}


//De esta manera si se lockean
fn main3() -> Result<(), Errors> {
    let path = "node/src/meta_data/keyspaces/testing.json";
    let _ = create_json_file(path);
    thread::sleep(Duration::from_secs(5));

    // Envuelve `meta_data` en Arc<Mutex<>`
    let meta_data = Arc::new(Mutex::new(KeyspaceMetaDataAccess::new(path)?));

    // Crear el primer hilo
    let meta_data_clone1 = Arc::clone(&meta_data); // Clonar solo el Arc
    let handle1 = thread::spawn(move || {
        let meta_data = meta_data_clone1.lock().unwrap(); // Bloquear el Mutex para acceder
        if let Err(e) = meta_data.add_keyspace("nombre", None, None) {
            eprintln!("Error en hilo 1: {:?}", e);
            return Err(e); // Propagar el error si ocurre
        }
        println!("Sali soy hilo 1");
        Ok(())
    });
    thread::sleep(Duration::from_secs(2));
    // Crear el segundo hilo
    let meta_data_clone2 = Arc::clone(&meta_data); // Clonar solo el Arc
    let handle2 = thread::spawn(move || {
        let meta_data = meta_data_clone2.lock().unwrap(); // Bloquear el Mutex para acceder
        if let Err(e) = meta_data.drop_keyspace("name") {
            eprintln!("Error en hilo 2: {:?}", e);
            return Err(e);
        }
        println!("Sali soy hilo 2");
        Ok(())
    });

    // Esperar a que ambos hilos terminen
    if let Err(e) = handle1.join().expect("Error al unir el hilo 1") {
        eprintln!("Error en hilo 1 (desde el join): {:?}", e);
    }

    if let Err(e) = handle2.join().expect("Error al unir el hilo 2") {
        eprintln!("Error en hilo 2 (desde el join): {:?}", e);
    }

    println!("Los hilos han terminado.");
    Ok(())
}

fn create_json_file(file_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    // Crear un HashMap que asocia el nombre del keyspace con su configuración
    let mut keyspaces: HashMap<String, Keyspace> = HashMap::new();

    // Añadir keyspaces al HashMap
    keyspaces.insert("keyspace1".to_string(), Keyspace::new(None, None));
    keyspaces.insert("keyspace2".to_string(), Keyspace::new(None, None));
    keyspaces.insert("name".to_string(), Keyspace::new(None, None));

   

    // Serializar a JSON
    let json_data = serde_json::to_string_pretty(&keyspaces)?;

    // Guardar el JSON en un archivo
    let mut file = File::create(file_path)?;
    file.write_all(json_data.as_bytes())?;

    Ok(())
}
