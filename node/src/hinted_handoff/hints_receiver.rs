use crate::hinted_handoff::stored_query::StoredQuery;
use crate::meta_data::meta_data_handler::use_node_meta_data;
use crate::utils::config_constants::HINTED_HANDOFF_TIMEOUT_SECS;
use crate::utils::constants::NODES_METADATA_PATH;
use crate::utils::errors::Errors;
use crate::utils::errors::Errors::ServerError;
use crate::utils::functions::{
    bind_listener, deserialize_from_slice, read_exact_from_stream, write_to_stream,
};
use crate::utils::types::node_ip::NodeIp;
use std::net::TcpStream;
use std::time::{Duration, Instant};

pub struct HintsReceiver;

impl HintsReceiver {
    pub fn start_listening(ip: NodeIp) -> Result<(), Errors> {
        let listener = bind_listener(ip.get_hints_receiver_socket())?;
        listener
            .set_nonblocking(true)
            .map_err(|_| ServerError(String::from("Could not set nonblocking")))?;
        let mut hints = Vec::new();
        let timeout = Duration::from_secs(HINTED_HANDOFF_TIMEOUT_SECS);
        let mut last_connection_time = Instant::now();
        println!("Recovering hints...");
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
        Self::finish_recovering()?;
        Ok(())
    }

    fn handle_connection(
        stream: &mut TcpStream,
        hints: &mut Vec<StoredQuery>,
    ) -> Result<(), Errors> {
        while let Ok(hint) = deserialize_from_slice(&read_exact_from_stream(stream)?) {
            hints.push(hint);
            write_to_stream(stream, b"ACK")?;
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

    fn finish_recovering() -> Result<(), Errors> {
        use_node_meta_data(|handler| handler.set_own_node_active(NODES_METADATA_PATH))?;
        println!("Finished recovering");
        Ok(())
    }
}
