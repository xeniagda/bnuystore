#[allow(unused)]
use tracing::{trace, debug, info, warn, error, instrument, Level};

use mysql_async::prelude::*;
use uuid::Uuid;

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;

pub mod tys;
pub mod config;
pub mod storage_node_connection;
pub mod sftp;

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
    file_uuids_and_names: Vec<(Uuid, String)>,
    directory_ids_and_names: Vec<(DirectoryID, String)>,
}

impl FrontNode {
    pub async fn start_from_config(
        cfg: &config::Config
    ) -> Result<FrontNode, Error> {
        let connection_options = cfg.database_connection.mysql_opts().await;
        trace!("Opening database connection");
        let conn_pool = mysql_async::Pool::new(connection_options);

        let active_connections = Arc::new(RwLock::new(HashMap::new()));

        let _monitor_task = tokio::spawn(monitor_connections(conn_pool.clone(), active_connections.clone(), cfg.clone()));

        Ok(FrontNode {
            conn_pool,
            active_connections,
        })
    }

    // path should NOT have a starting slash
    // base == None selects the root directory
    #[instrument(level = "trace", skip(self))]
    pub async fn directory_id_for_path(
        &self,
        path: &str,
        base: Option<DirectoryID>,
    ) -> Result<DirectoryID, Error> {
        let base = match base {
            Some(base) => base,
            None => {
                let root_query = r#"SELECT directory_id FROM root_directory"#;
                root_query
                    .first(&self.conn_pool)
                    .await?
                    .expect("root_directory table is empty")
            }
        };

        if path.len() == 0 {
            return Ok(base);
        }

        let mut current_directory = base;

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

    // full_path should NOT have a starting slash
    // base == None selects the root directory
    #[instrument(level = "trace", skip(self))]
    pub async fn file_uuid_for_path(
        &self,
        full_path: &str,
        base: Option<DirectoryID>,
    ) -> Result<Uuid, Error> {
        let (path, file) = full_path.rsplit_once('/')
            .map(|(path, file)| (path.to_string(), file.to_string()))
            .unwrap_or(("".to_string(), full_path.to_string()));

        trace!(?path, ?file, "Split file from parent");

        let dir = self.directory_id_for_path(&path, base).await?;
        trace!(?dir, "Found directory");

        let query = r#"
            SELECT files.uuid
                FROM files
                WHERE files.name = :filename AND directory_id = :dir;
            "#;

        if let Some(uuid) = query
            .with(params!("filename" => file, "dir" => dir))
            .first(&self.conn_pool)
            .await?
        {
            Ok(uuid)
        } else {
            Err(Error::NoSuchFile)
        }
    }

    #[instrument(level = "trace", skip(self))]
    pub async fn home_for_user(
        &self,
        name: &str,
    ) -> Result<DirectoryID, Error> {
        let query = r#"
            SELECT home_directory
                FROM users
                WHERE username = :name;
            "#;

        if let Some(id) = query
            .with(params! { "name" => name })
            .first(&self.conn_pool)
            .await?
        {
            Ok(id)
        } else {
            Err(Error::NoSuchUser { name: name.to_owned() })
        }
    }

    // None = file not found
    // TODO: Add NoSuchFile to Error?
    #[instrument(level = "debug", skip(self))]
    pub async fn get_file(
        &self,
        uuid: Uuid,
    ) -> Result<(Vec<u8>, GetFileInfo), Error> {
        let query = r#"
            SELECT files.stored_on_node_id, nodes.name
                FROM files INNER JOIN nodes ON files.stored_on_node_id = nodes.id
                WHERE files.uuid = :uuid
            "#;

        let Some((id, node_name)) = query
            .with(params! { "uuid" => uuid })
            .first(&self.conn_pool)
            .await?
        else {
            return Err(Error::UnknownUUID);
        };
        trace!(?id, ?node_name, "Found file");

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
                Ok((c, info))
            }
            x => Err(Error::UnexpectedResponse(x))
        }
    }

    #[instrument(level = "debug", skip(self))]
    pub async fn list_directory(
        &self,
        dir: DirectoryID,
    ) -> Result<DirectoryListing, Error> {
        let query_files = r#"
            SELECT uuid, name FROM files
                WHERE directory_id = :dir;
            "#;

        let query_dirs = r#"
            SELECT id, name FROM directories
                WHERE parent_id = :dir;
            "#;

        let file_uuids_and_names: Vec<(Uuid, String)> = query_files.with(params! { "dir" => &dir })
            .fetch(&self.conn_pool)
            .await?;

        let directory_ids_and_names: Vec<(DirectoryID, String)> = query_dirs.with(params! { "dir" => &dir })
            .fetch(&self.conn_pool)
            .await?;

        trace!(file_uuids_and_names.len = file_uuids_and_names.len(), directory_ids_and_names.len = directory_ids_and_names.len(), "Listed contents");

        Ok(DirectoryListing { file_uuids_and_names, directory_ids_and_names })
    }

    #[instrument(level = "info", skip(self))]
    pub async fn create_directory(
        &self,
        parent: DirectoryID,
        dir_name: String,
    ) -> Result<(), Error> {
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

    #[instrument(level = "info", skip(self, contents), fields(contents.len = contents.len()))]
    pub async fn upload_file(
        &self,
        filename: String,
        dir: DirectoryID,
        contents: Vec<u8>,
    ) -> Result<Uuid, Error> {
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

#[instrument(level = "info", skip_all)]
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
