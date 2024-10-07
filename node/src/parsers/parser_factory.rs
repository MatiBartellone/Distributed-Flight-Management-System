use crate::parsers::auth_response_parser::AuthResponseParser;
use crate::parsers::batch_parser::BatchParser;
use crate::parsers::event_parser::EventParser;
use crate::parsers::execute_parser::ExecuteParser;
use crate::parsers::options_parser::OptionsParser;
use crate::parsers::parser::Parser;
use crate::parsers::prepare_parser::PrepareParser;
use crate::parsers::query_parser::QueryParser;
use crate::parsers::register_parser::RegisterParser;
use crate::parsers::startup_parser::StartupParser;
use crate::utils::errors::Errors;
use crate::utils::parser_constants::*;

pub struct ParserFactory {}

impl ParserFactory {
    pub fn get_parser(opcode: u8) -> Result<Box<dyn Parser>, Errors> {
        match opcode {
            STARTUP => Ok(Box::new(StartupParser)),
            OPTIONS => Ok(Box::new(OptionsParser)),
            QUERY => Ok(Box::new(QueryParser)),
            PREPARE => Ok(Box::new(PrepareParser)),
            EXECUTE => Ok(Box::new(ExecuteParser)),
            REGISTER => Ok(Box::new(RegisterParser)),
            EVENT => Ok(Box::new(EventParser)),
            BATCH => Ok(Box::new(BatchParser)),
            AUTH_RESPONSE => Ok(Box::new(AuthResponseParser {})),
            _ => Err(Errors::ProtocolError(format!(
                "Opcode {} is invalid",
                opcode
            ))),
        }
    }
}
