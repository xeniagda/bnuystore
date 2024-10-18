use uuid::Uuid;
use serde::{Serialize, Deserialize};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

#[derive(Debug)]
pub struct MessageID(pub u32);

#[derive(Debug)]
#[allow(unused)]
pub enum ParseMessageError {
    IOError(std::io::Error),
    ParseJsonError(serde_json::Error),
    ParseUuidError(uuid::Error),
    RequestTooLarge(usize), // number of bytes to allocate
}
type Result<T> = std::result::Result<T, ParseMessageError>;

impl From<std::io::Error> for ParseMessageError {
    fn from(e: std::io::Error) -> Self {
        ParseMessageError::IOError(e)
    }
}

impl From<serde_json::Error> for ParseMessageError {
    fn from(e: serde_json::Error) -> Self {
        ParseMessageError::ParseJsonError(e)
    }
}

#[derive(Debug)]
pub enum Message {
    // requests
    GetVersion, // returns a MyVersionIs
    ReadFile(Uuid), // returns a FileContents
    WriteFile(Uuid, Vec<u8>), // data currently raw, may be compressed in the future. Returns a Response::Ack
    DeleteFile(Uuid), // Returns a Respanse::Ack
    // TODO: StorageInfo, ListFiles

    // responses
    MyVersionIs(String),
    FileContents(Vec<u8>),
    Ack,
    Error(String),
}

/// the representation of the message that is sent over the stream
/// differs from Message in that, Uuids are stringified and large data
/// are sent separately
#[derive(Debug, Serialize, Deserialize)]
enum MessageOverWire {
    GetVersion,
    ReadFile(String),
    WriteFile(String),
    DeleteFile(String),
    MyVersionIs(String),
    FileContents,
    Ack,
    Error(String),
}

pub async fn parse_message<F: AsyncRead + Unpin>(
    stream: &mut F,
) -> Result<(MessageID, Message)> {
    let id = MessageID(stream.read_u32().await?);
    let message_length = stream.read_u32().await?;
    let data_length = stream.read_u64().await?;

    let mut wire_message_buf = Vec::new();
    wire_message_buf.try_reserve(message_length as usize)
        .map_err(|_| ParseMessageError::RequestTooLarge(message_length as usize))?;
    wire_message_buf.resize(message_length as usize, 0);
    stream.read_exact(&mut wire_message_buf).await?;

    let mut data_buf = Vec::new();
    data_buf.try_reserve(data_length as usize)
        .map_err(|_| ParseMessageError::RequestTooLarge(data_length as usize))?;
    data_buf.resize(data_length as usize, 0);
    stream.read_exact(&mut data_buf).await?;

    let wire_message: MessageOverWire = serde_json::from_slice(&wire_message_buf)?;
    let message = wire_message.to_message(data_buf)?;

    Ok((id, message))
}

pub async fn write_message<F: AsyncWrite + Unpin>(
    stream: &mut F,
    id: MessageID,
    message: Message,
) -> Result<()> {
    stream.write_u32(id.0).await?;

    let (wire_message, data) = MessageOverWire::from_message(message);
    let wire_message_buf = serde_json::to_vec(&wire_message)?;

    stream.write_u32(wire_message_buf.len() as u32).await?;
    stream.write_u64(data.len() as u64).await?;

    stream.write_all(&wire_message_buf).await?;
    stream.write_all(&data).await?;

    Ok(())
}

fn stringify_uuid(uuid: Uuid) -> String {
    uuid.hyphenated().encode_lower(&mut Uuid::encode_buffer()).to_string()
}
fn parse_uuid(stringified: String) -> Result<Uuid> {
    Uuid::try_parse(&stringified).map_err(ParseMessageError::ParseUuidError)
}

impl MessageOverWire {
    fn from_message(cmd: Message) -> (MessageOverWire, Vec<u8>) {
        match cmd {
            Message::GetVersion => (MessageOverWire::GetVersion, vec![]),
            Message::ReadFile(u) => (MessageOverWire::ReadFile(stringify_uuid(u)), vec![]),
            Message::WriteFile(u, data) => (MessageOverWire::WriteFile(stringify_uuid(u)), data), // TODO: Compression
            Message::DeleteFile(u) => (MessageOverWire::DeleteFile(stringify_uuid(u)), vec![]),
            Message::MyVersionIs(v) => (MessageOverWire::MyVersionIs(v), vec![]),
            Message::FileContents(data) => (MessageOverWire::FileContents, data), // TODO: Compression
            Message::Ack => (MessageOverWire::Ack, vec![]),
            Message::Error(e) => (MessageOverWire::Error(e), vec![]),
        }
    }
    fn to_message(self, data: Vec<u8>) -> Result<Message> {
        Ok(match self {
            MessageOverWire::GetVersion => Message::GetVersion,
            MessageOverWire::ReadFile(u) => Message::ReadFile(parse_uuid(u)?),
            MessageOverWire::WriteFile(u) => Message::WriteFile(parse_uuid(u)?, data), // TODO: Compression
            MessageOverWire::DeleteFile(u) => Message::DeleteFile(parse_uuid(u)?),
            MessageOverWire::MyVersionIs(v) => Message::MyVersionIs(v),
            MessageOverWire::FileContents => Message::FileContents(data), // TODO: Compression
            MessageOverWire::Ack => Message::Ack,
            MessageOverWire::Error(e) => Message::Error(e),
        })
    }
}
