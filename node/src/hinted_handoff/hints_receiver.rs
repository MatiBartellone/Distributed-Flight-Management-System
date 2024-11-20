use crate::hinted_handoff::stored_query::StoredQuery;
use crate::meta_data::meta_data_handler::MetaDataHandler;
use crate::utils::constants::{HINTED_HANDOFF_TIMEOUT_SECS, NODES_METADATA_PATH};
use crate::utils::errors::Errors;
use crate::utils::errors::Errors::ServerError;
use crate::utils::node_ip::NodeIp;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::time::{Duration, Instant};

pub struct HintsReceiver;

impl HintsReceiver {
    pub fn start_listening(ip: NodeIp) -> Result<(), Errors> {
        let listener = TcpListener::bind(ip.get_hints_receiver_socket())
            .map_err(|_| ServerError(String::from("Failed to set listener")))?;
        listener
            .set_nonblocking(true)
            .map_err(|_| ServerError(String::from("Could not set nonblocking")))?;
        let mut hints = Vec::new();
        let timeout = Duration::from_secs(HINTED_HANDOFF_TIMEOUT_SECS);
        let mut last_connection_time = Instant::now();
        println!("Booting...");
        loop {
            if last_connection_time.elapsed() >= timeout {
                break;
            }
            match listener.accept() {
                Ok((mut stream, _)) => {
                    Self::handle_connection(&mut stream, &mut hints)?;
                    last_connection_time = Instant::now();
                }
                _ => continue,
            }
        }
        Self::execute_queries(&mut hints)?;
        Self::finish_booting()?;
        Ok(())
    }

    fn handle_connection(
        stream: &mut TcpStream,
        hints: &mut Vec<StoredQuery>,
    ) -> Result<(), Errors> {
        loop {
            let mut buffer = [0; 1024];
            let size = stream
                .read(&mut buffer)
                .map_err(|_| ServerError(String::from("Failed to read from stream")))?;
            match serde_json::from_slice::<StoredQuery>(&buffer[..size]) {
                Ok(hint) => hints.push(hint),
                _ => break,
            }
            stream
                .flush()
                .map_err(|_| ServerError(String::from("Failed to flush stream")))?;
            stream
                .write_all(b"ACK")
                .map_err(|_| ServerError(String::from("Failed to write to stream")))?;
            stream
                .flush()
                .map_err(|_| ServerError(String::from("Failed to flush stream")))?;
        }
        Ok(())
    }

    fn execute_queries(hints: &mut [StoredQuery]) -> Result<(), Errors> {
        hints.sort_by_key(|stored_query| stored_query.timestamp.timestamp);
        for stored in hints.iter() {
            if stored.get_query().run().is_ok() {};
        }
        Ok(())
    }

    fn finish_booting() -> Result<(), Errors> {
        let mut meta_data_stream = MetaDataHandler::establish_connection()?;
        let node_metadata =
            MetaDataHandler::get_instance(&mut meta_data_stream)?.get_nodes_metadata_access();
        node_metadata.set_own_node_active(NODES_METADATA_PATH)?;
        println!("Finished booting");
        Ok(())
    }
}
