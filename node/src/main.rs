use node::frame::Frame;
use node::parsers::parser_factory::ParserFactory;
use node::utils::errors::Errors;

fn main() -> Result<(), Errors> {
    let bytes = vec![
        0x03,
        0x00,
        0x00, 0x01,
        0x03,
        0x00, 0x00, 0x00, 0x05,
        0x10, 0x03, 0x35, 0x12, 0x22
    ];
    let frame = Frame::parse_frame(bytes.as_slice())?;
    let parser = ParserFactory::get_parser(frame.opcode)?;
    let executable = parser.parse(frame.body.as_slice());
    executable.execute();
    Ok(())
}
