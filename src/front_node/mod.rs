use mysql_async::prelude::*;
use uuid::Uuid;

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;

pub mod config;
pub mod storage_node_connection;
pub mod tys;

use storage_node_connection::{StorageNodeConnection, ConnectionError};

use crate::message::Message;
use tys::{StorageNodeID, DirectoryID};


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

    NoSuchDirectory { topmost_existing_directory: String },
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

struct UploadFileInfo {
    #[allow(unused)]
    data_length: usize,
}

pub struct GetFileInfo {
    pub uuid: Uuid,
    pub node_name: String,
}

#[derive(serde::Serialize)]
pub struct DirectoryListing {
    file_names: Vec<String>,
    directory_names: Vec<String>,
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

    async fn directory_id_for_path(
        &self,
        path: String,
    ) -> Result<DirectoryID, Error> {
        let root: DirectoryID = {
            let root_query = r#"SELECT directory_id FROM root_directory"#;
            root_query
                .first(&self.conn_pool)
                .await?
                .expect("root_directory table is empty")
        };

        if path.len() == 0 {
            return Ok(root);
        }

        let mut current_directory = root;

        let mut topmost_existing_directory = String::new();

        for segment in path.split('/') {
            current_directory = {
                let subdir_query = r#"
                    SELECT id FROM directories WHERE name = :segment AND parent_id = :current_directory;
                "#;
                let next_directory = subdir_query
                    .with(params! { "segment" => segment, "current_directory" => current_directory })
                    .first(&self.conn_pool)
                    .await?;
                if let Some(next_directory) = next_directory {
                    topmost_existing_directory.push_str(&segment);
                    topmost_existing_directory.push('/');
                    next_directory
                } else {

                    return Err(Error::NoSuchDirectory { topmost_existing_directory });
                }
            };
        }

        Ok(current_directory)
    }

    // None = file not found
    // TODO: Add NoSuchFile to Error?
    pub async fn get_file(
        &self,
        filename: String,
        path: String,
    ) -> Result<Option<(Vec<u8>, GetFileInfo)>, Error> {
        let dir = self.directory_id_for_path(path).await?;

        let query = r#"
            SELECT files.uuid, files.stored_on_node_id, nodes.name
                FROM files INNER JOIN nodes ON files.stored_on_node_id = nodes.id
                WHERE files.name = :filename AND directory_id = :dir;
            "#;

        let (uuid, id, node_name) = match query
            .with(params! { "filename" => filename, "dir" => dir.0 })
            .first(&self.conn_pool)
            .await?
        {
            Some((uuid, id, name)) => (parse_uuid(Vec::as_slice(&uuid))?, StorageNodeID(id), name),
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
            Message::FileContents(c) => {
                let info = GetFileInfo {
                    uuid,
                    node_name,
                };
                Ok(Some((c, info)))
            }
            x => Err(Error::UnexpectedResponse(x))
        }
    }

    pub async fn list_directory(
        &self,
        path: String,
    ) -> Result<DirectoryListing, Error> {
        let dir = self.directory_id_for_path(path).await?;

        let query_files = r#"
            SELECT name FROM files
                WHERE directory_id = :dir;
            "#;

        let query_dirs = r#"
            SELECT name FROM directories
                WHERE parent_id = :dir;
            "#;

        let file_names: Vec<String> = query_files.with(params! { "dir" => &dir })
            .fetch(&self.conn_pool)
            .await?;

        let directory_names: Vec<String> = query_dirs.with(params! { "dir" => &dir })
            .fetch(&self.conn_pool)
            .await?;

        Ok(DirectoryListing { file_names, directory_names })
    }

    pub async fn create_directory(
        &self,
        parent_path: String,
        dir_name: String,
    ) -> Result<(), Error> {
        let parent = self.directory_id_for_path(parent_path).await?;
        let query = r#"
            INSERT INTO directories
                (name, parent_id) VALUES
                (:dir_name, :parent);
        "#;
        query
            .with(params! { "dir_name" => dir_name, "parent" => parent })
            .ignore(&self.conn_pool)
            .await?;
        Ok(())
    }

    async fn get_appropriate_node_for(
        &self,
        _file_info: &UploadFileInfo,
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
        let dir = self.directory_id_for_path(path).await?;

        let info = UploadFileInfo {
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

        let query = r#"
            INSERT INTO files
                (uuid, name, directory_id, stored_on_node_id) VALUES
                (:uuid, :name, :dir, :stored_on_node_id);
        "#;

        query.with(params! {
            "uuid" => uuid,
            "name" => filename,
            "dir" => dir,
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
