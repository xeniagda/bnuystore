use mysql_async::prelude::*;
use mysql_common::value::convert::ParseIrOpt;

use super::storage_node_connection::ConnectionError;

/// Corresponds to database nodes.id
#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy)]
pub struct StorageNodeID(pub i64);

impl From<StorageNodeID> for mysql_async::Value {
    fn from(x: StorageNodeID) -> Self {
        Self::Int(x.0)
    }
}
impl From<ParseIrOpt<i64>> for StorageNodeID {
    fn from(x: ParseIrOpt<i64>) -> Self {
        Self(x.commit())
    }
}
impl FromValue for StorageNodeID {
    type Intermediate = ParseIrOpt<i64>;
}


/// Corresponds to database directories.id
#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy)]
pub struct DirectoryID(pub i64);

impl From<DirectoryID> for mysql_async::Value {
    fn from(x: DirectoryID) -> Self {
        Self::Int(x.0)
    }
}
impl From<ParseIrOpt<i64>> for DirectoryID {
    fn from(x: ParseIrOpt<i64>) -> Self {
        Self(x.commit())
    }
}
impl FromValue for DirectoryID {
    type Intermediate = ParseIrOpt<i64>;
}


#[derive(Debug)]
#[allow(unused)]
pub enum Error {
    IO(std::io::Error),
    DatabaseError(mysql_async::Error),
    ConnectionError(ConnectionError),
    // tried to connect to a node we're not connected to
    NotConnectedToNode,
    MalformedUUIDError(Vec<u8>, uuid::Error),
    UnexpectedResponse(crate::message::Message),
    NotConnectedToAnyNode,

    NoSuchDirectory { topmost_existing_directory: String },
}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self { Error::IO(value) }
}

impl From<mysql_async::Error> for Error {
    fn from(value: mysql_async::Error) -> Self { Error::DatabaseError(value) }
}

impl From<ConnectionError> for Error {
    fn from(value: ConnectionError) -> Self { Error::ConnectionError(value) }
}

