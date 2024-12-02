use mysql_async::prelude::*;
use mysql_common::value::convert::ParseIrOpt;

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

