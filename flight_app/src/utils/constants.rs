pub const READ_ERROR: &str = "Could not read byte";
pub const ERROR_WRITE: &str = "Could not write byte";
pub const FLUSH_ERROR: &str = "Error flushing socket";

// Op Code of the protocol
pub const OP_ERROR: u8 = 0;
pub const OP_STARTUP: u8 = 1;
pub const OP_READY: u8 = 2;
pub const OP_AUTHENTICATE: u8 = 3;
pub const OP_OPTIONS: u8 = 5;
pub const OP_SUPPORTED: u8 = 6;
pub const OP_QUERY: u8 = 7;
pub const OP_RESULT: u8 = 8;
pub const OP_PREPARE: u8 = 9;
pub const OP_EXECUTE: u8 = 10;
pub const OP_REGISTER: u8 = 11;
pub const OP_EVENT: u8 = 12;
pub const OP_BATCH: u8 = 13;
pub const OP_AUTH_CHALLENGE: u8 = 14;
pub const OP_AUTH_RESPONSE: u8 = 15;
pub const OP_AUTH_SUCCESS: u8 = 16;