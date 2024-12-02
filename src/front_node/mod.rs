#[allow(unused)]
use tracing::{trace, debug, info, warn, error, instrument, Level};

use mysql_async::prelude::*;
use uuid::Uuid;

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;

pub mod config;
pub mod storage_node_connection;
pub mod tys;

use storage_node_connection::StorageNodeConnection;

use crate::message::Message;
use tys::{StorageNodeID, DirectoryID, Error};

pub struct FrontNode {
    #[allow(unused)]
    conn_pool: mysql_async::Pool,

    // active_connections has one reference in a task that monitors the nodes table
    // and tries to spawn/respawn/unspawn connections
    #[allow(unused)]
    active_connections: Arc<RwLock<HashMap<StorageNodeID, Arc<StorageNodeConnection>>>>,
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
        trace!("Opening database connection");
        let conn_pool = mysql_async::Pool::new(connection_options);

        let active_connections = Arc::new(RwLock::new(HashMap::new()));

        let _monitor_task = tokio::spawn(monitor_connections(conn_pool.clone(), active_connections.clone(), cfg));

        Ok(FrontNode {
            conn_pool,
            active_connections,
        })
    }

    #[instrument(level = "trace", skip(self))]
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
        trace!(?root, "found root");

        if path.len() == 0 {
            return Ok(root);
        }

        let mut current_directory = root;

        let mut topmost_existing_directory = String::new();

        for segment in path.split('/') {
            trace!(?segment, ?current_directory, "Following");

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
                    trace!(?next_directory, "Found");
                    next_directory
                } else {
                    debug!("Not found");
                    return Err(Error::NoSuchDirectory { topmost_existing_directory });
                }
            };
        }

        Ok(current_directory)
    }

    // None = file not found
    // TODO: Add NoSuchFile to Error?
    #[instrument(skip(self))]
    pub async fn get_file(
        &self,
        filename: String,
        path: String,
    ) -> Result<Option<(Vec<u8>, GetFileInfo)>, Error> {
        let dir = self.directory_id_for_path(path).await?;
        trace!(?dir, "Found directory");

        let query = r#"
            SELECT files.uuid, files.stored_on_node_id, nodes.name
                FROM files INNER JOIN nodes ON files.stored_on_node_id = nodes.id
                WHERE files.name = :filename AND directory_id = :dir;
            "#;

        let Some((uuid, id, node_name)) = query
            .with(params! { "filename" => filename, "dir" => dir.0 })
            .first(&self.conn_pool)
            .await?
        else {
            debug!("No such file");
            return Ok(None);
        };
        trace!(?uuid, ?id, ?node_name, "Found file");

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

    #[instrument(skip(self))]
    pub async fn list_directory(
        &self,
        path: String,
    ) -> Result<DirectoryListing, Error> {
        let dir = self.directory_id_for_path(path).await?;
        trace!(?dir, "Found directory");

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

        trace!(file_names.len = file_names.len(), directory_names.len = directory_names.len(), "Listed contents");

        Ok(DirectoryListing { file_names, directory_names })
    }

    #[instrument(skip(self))]
    pub async fn create_directory(
        &self,
        parent_path: String,
        dir_name: String,
    ) -> Result<(), Error> {
        let parent = self.directory_id_for_path(parent_path).await?;
        trace!(?parent, "Found parent");

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

    #[instrument(skip(self, contents), fields(contents.len = contents.len()))]
    pub async fn upload_file(
        &self,
        filename: String,
        path: String,
        contents: Vec<u8>,
    ) -> Result<Uuid, Error> {
        let dir = self.directory_id_for_path(path).await?;
        trace!(?dir, "Found parent");

        let info = UploadFileInfo {
            data_length: contents.len(),
        };

        let uuid = Uuid::now_v7();

        let storage_node_id = {
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
            "stored_on_node_id" => storage_node_id,
        }).ignore(&self.conn_pool).await?;

        Ok(uuid)
    }
}

#[instrument(skip_all)]
async fn monitor_connections(
    conn_pool: mysql_async::Pool,
    active_connections: Arc<RwLock<HashMap<StorageNodeID, Arc<StorageNodeConnection>>>>,
    cfg: config::Config,
) {
    // insert all nodes not in db into db
    debug!("Making nodes consistent");
    for (name, _cfg) in &cfg.storage_nodes {
        trace!(name, "Checking");
        let query = "SELECT count(*) FROM nodes WHERE name = :name;";
        let count: u32 = query.with(params! {
            "name" => name,
        }).first(&conn_pool).await.unwrap().unwrap();
        if count == 0 {
            debug!(name, "Not in nodes table; inserting");
            let query = "INSERT INTO nodes(name) VALUES (:name);";
            query.with(params! {
                "name" => name,
            }).run(&conn_pool).await.unwrap();
        }
    }

    // spawn connections for all nodes
    debug!("Spawning connections to all nodes");
    {
        let mut active_connections = active_connections.write().await;
        for (name, node_cfg) in &cfg.storage_nodes {
            trace!(name, "Finding id");
            let query = "SELECT id FROM nodes WHERE name = :name;";

            // raw indexing should be safe because we inserted all of these into the table before
            let id: StorageNodeID = query.with(params! {
                "name" => name,
            }).first(&conn_pool).await.unwrap().expect("Node not in nodes table");

            debug!(name, ?id, "Connecting");
            match StorageNodeConnection::connect(node_cfg).await {
                Ok(conn) => {
                    info!(name, "Connected successfully");
                    active_connections.insert(id, Arc::new(conn));
                }
                Err(e) => {
                    error!(name, ?e, "Could not connect");
                    continue;
                }
            };
        }
    }
    debug!("All nodes connected to");
}
