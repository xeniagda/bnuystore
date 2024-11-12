use mysql_async::prelude::*;
use uuid::Uuid;

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;

pub mod config;
pub mod storage_node_connection;

use storage_node_connection::{StorageNodeConnection, ConnectionError};

use crate::message::Message;

/// Corresponds to database nodes.id
#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy)]
struct StorageNodeID(i32);

pub struct FrontNode {
    #[allow(unused)]
    conn_pool: mysql_async::Pool,

    // active_connections has one reference in a task that monitors the nodes table
    // and tries to spawn/respawn/unspawn connections
    #[allow(unused)]
    active_connections: Arc<RwLock<HashMap<StorageNodeID, Arc<StorageNodeConnection>>>>,
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
    UnexpectedResponse(Message),
    NotConnectedToAnyNode,
}

fn parse_uuid(data: &[u8]) -> Result<Uuid, Error> {
    match Uuid::from_slice(data) {
        Ok(u) => Ok(u),
        Err(e) => Err(Error::MalformedUUIDError(data.to_vec(), e)),
    }
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

struct FileInfo {
    #[allow(unused)]
    data_length: usize,
}

impl FrontNode {
    pub async fn start_from_config(
        cfg: config::Config
    ) -> Result<FrontNode, Error> {
        let connection_options = cfg.connection_options.mysql_opts().await;
        let conn_pool = mysql_async::Pool::new(connection_options);

        let active_connections = Arc::new(RwLock::new(HashMap::new()));

        let _monitor_task = tokio::spawn(monitor_connections(conn_pool.clone(), active_connections.clone(), cfg));

        Ok(FrontNode {
            conn_pool,
            active_connections,
        })
    }

    // None = file not found
    pub async fn get_file(
        &self,
        filename: String,
        path: String,
    ) -> Result<Option<(Vec<u8>, Uuid)>, Error> {
        let query = "SELECT uuid, stored_on_node_id FROM files WHERE name = :filename AND path = :path;";

        let (uuid, id) = match query
            .with(params! { "filename" => filename, "path" => path })
            .first(&self.conn_pool)
            .await?
        {
            Some((uuid, id)) => (parse_uuid(Vec::as_slice(&uuid))?, StorageNodeID(id)),
            None => return Ok(None),
        };

        let conn = {
            let active_connections = self.active_connections.read().await;
            match active_connections.get(&id) {
                Some(conn) => conn.clone(),
                None => return Err(Error::NotConnectedToNode),
            }
        };

        match conn.communicate(Message::ReadFile(uuid)).await? {
            Message::FileContents(c) => Ok(Some((c, uuid))),
            x => Err(Error::UnexpectedResponse(x))
        }
    }

    async fn get_appropriate_node_for(
        &self,
        _file_info: &FileInfo,
    ) -> Result<StorageNodeID, Error> {
        let connections = self.active_connections.read().await;
        if let Some(i) = connections.keys().next() {
            Ok(*i)
        } else {
            Err(Error::NotConnectedToAnyNode)
        }
    }

    pub async fn upload_file(
        &self,
        filename: String,
        path: String,
        contents: Vec<u8>,
    ) -> Result<Uuid, Error> {
        let info = FileInfo {
            data_length: contents.len(),
        };

        let uuid = Uuid::now_v7();

        let id = {
            // We grab a read-lock for connections before we do get_appropriate_node_for.
            // As no write-lock can be obtained between this and getting the conneciton,
            // unwrapping the result is safe.
            let conns = self.active_connections.read().await;
            let id = self.get_appropriate_node_for(&info).await?;
            let conn = conns.get(&id).unwrap();

            match conn.communicate(Message::WriteFile(uuid, contents)).await? {
                Message::Ack => {},
                x => return Err(Error::UnexpectedResponse(x))
            }

            id
        };

        let query = "INSERT INTO files(uuid, name, path, stored_on_node_id) VALUES (:uuid, :name, :path, :stored_on_node_id);";

        query.with(params! {
            "uuid" => uuid,
            "name" => filename,
            "path" => path,
            "stored_on_node_id" => id.0,
        }).ignore(&self.conn_pool).await?;

        Ok(uuid)
    }
}

async fn monitor_connections(
    conn_pool: mysql_async::Pool,
    active_connections: Arc<RwLock<HashMap<StorageNodeID, Arc<StorageNodeConnection>>>>,
    cfg: config::Config,
) {
    // insert all nodes not in db into db
    for (name, _cfg) in &cfg.storage_nodes {
        let query = "SELECT count(*) FROM nodes WHERE name = :name;";
        let count: u32 = query.with(params! {
            "name" => name,
        }).first(&conn_pool).await.unwrap().unwrap();
        if count == 0 {
            let query = "INSERT INTO nodes(name) VALUES (:name);";
            query.with(params! {
                "name" => name,
            }).run(&conn_pool).await.unwrap();
        }
    }

    // spawn connections for all nodes
    {
        let mut active_connections = active_connections.write().await;
        for (name, node_cfg) in &cfg.storage_nodes {
            let query = "SELECT id FROM nodes WHERE name = :name;";
            let id: StorageNodeID = query.with(params! {
                "name" => name,
            }).map(&conn_pool, |id| StorageNodeID(id)).await.unwrap()[0];

            let conn = match StorageNodeConnection::connect(node_cfg).await {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("Could not connect to {name}: {e}");
                    continue;
                }
            };
            eprintln!("Connected to {name}");
            active_connections.insert(id, Arc::new(conn));
        }
    }
    eprintln!("Done connecting");
}
