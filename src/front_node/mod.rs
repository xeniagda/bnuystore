use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinHandle;

use mysql_async::{prelude::*, TxOpts};

pub mod config;
pub mod storage_node_connection;

use storage_node_connection::{StorageNodeConnection, ConnectionError};
use crate::owned_task::OwnedTask;
use crate::message::Message;

#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy)]
struct StorageNodeID(i32);

// The FrontNode outer type contains an inner node which has a database connection, connections to storage nodes etc
//
// The outer node also contains all connection_monitors which are responsible for spawning and respawning all
// per-connection tasks.
pub struct FrontNode {
    inner: Arc<RwLock<FrontNodeInner>>,
    connection_monitors: HashMap<StorageNodeID, OwnedTask>,
}

// The inner part of the node contains all the tasks that are running to make sure
// the system is functioning
//
// All tasks that require a reference to the inner node must do so through an Arc<RwLock<FrontNodeInner>>
// Only read-access to the node is required for most actions (internal locks ensure safety). The node is
// only locked in write-mode by a connection monitor on the outer node when it needs to respawn a dead connection.
// This does lock the whole system for the time of spawning the connection but this should be fine :3
//
// However, this means one task should NOT hold on to the lock for any significant amount of time.
struct FrontNodeInner {
    // referenced by various tasks in this struct
    db_conn_pool: mysql_async::Pool,

    // a bunch of tasks may be accessing each StorageNodeConnection
    // if a connection dies, all these tasks should die as well
    //
    // the respective connection_monitor watches the StorageNodeConnection.disconnect Notify
    // and once it notices the connection has disconnected, it waits until all tasks have finished,
    // attempts to reestablish the connection and re-spawn all the tasks
    active_connections: HashMap<StorageNodeID, Arc<StorageNodeConnection>>,
}

impl FrontNode {
    /// creates the front node and start all the connection monitors,
    /// which in turn start all tasks
    pub async fn start_from_config(
        cfg: config::Config
    ) -> Result<FrontNode, std::io::Error> {
        let connection_options = cfg.connection_options.mysql_opts().await;
        let db_conn_pool = mysql_async::Pool::new(connection_options);

        let inner = FrontNodeInner {
            db_conn_pool,
            active_connections: HashMap::new(),
        };

        let inner = Arc::new(RwLock::new(inner));

        let mut connection_monitors = HashMap::new();
        for (storage_node_name, storage_node_config) in cfg.storage_nodes {
            let id = {
                let inner = inner.read().await;
                inner.id_for_node(&storage_node_name).await
            };
            let task = OwnedTask::spawn(spawn_and_monitor_connection(inner.clone(), storage_node_name.clone(), storage_node_config.clone()));
            connection_monitors.insert(id, task);
        }

        Ok(FrontNode {
            inner,
            connection_monitors,
        })
    }
}

async fn spawn_and_monitor_connection(
    node: Arc<RwLock<FrontNodeInner>>,
    storage_node_name: String,
    storage_node_config: config::StorageNodeConfig,
) {
    let id = {
        let node = node.read().await;
        node.id_for_node(&storage_node_name).await
    };

    loop {
        // create the connection
        eprintln!("Trying to spawn a connection for {storage_node_name}");
        let conn = match storage_node_connection::StorageNodeConnection::connect(&storage_node_config).await {
            Ok(conn) => conn,
            Err(e) => {
                eprintln!("Could not spawn a connection for storage node {storage_node_name}: {e:?}");
                // wait for 5 seconds, try again
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                continue;
            }
        };
        eprintln!("Successfully established a connection for {storage_node_name}");
        let conn = Arc::new(conn);
        {
            let mut node = node.write().await;
            node.active_connections.insert(id, conn.clone());
        }

        let ping_task = OwnedTask::spawn(ping_task(node.clone(), id));

        eprintln!("Going to sleep until a disconnect");
        conn.disconnect.notified().await;
        eprintln!("Connection disconnected. Waiting until all tasks die.");
        tokio::join!(ping_task.wait_until_finished());
    }
}

async fn ping_task(node: Arc<RwLock<FrontNodeInner>>, id: StorageNodeID) -> Result<(), ConnectionError> {
    let conn = {
        let node = node.read().await;
        let Some(conn) = node.active_connections.get(&id) else {
            eprintln!("No connection open for {id:?}");
            return Ok(());
        };
        conn.clone()
    };

    loop {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        eprintln!("Sending ping to id={id:?}");
        match conn.communicate(Message::GetVersion).await? {
            Message::MyVersionIs(ver) => {
                eprintln!("Got response {ver}");
            }
            _ => {
            }
        }
    }
}

impl FrontNodeInner {
    /// Gets the ID for the storage node by that name from the `nodes` table in the database
    /// Or inserts the node into the database
    pub async fn id_for_node(&self, storage_node_name: &str) -> StorageNodeID {
        let mut tx = self.db_conn_pool.start_transaction(TxOpts::new()).await.expect("Could not make a transaction");
        let query = "SELECT id FROM nodes WHERE name = :name;";

        let resp: Option<i32> = query
            .with(params! { "name" => storage_node_name })
            .first(&mut tx).await
            .expect("Could not query nodes");
        if let Some(id) = resp {
            tx.commit().await.expect("Could not commit transaction");
            StorageNodeID(id)
        } else {
            let creation = "INSERT INTO nodes(name) VALUES (:name) RETURNING id;";
            let resp: Option<i32> = creation
                .with(params! { "name" => storage_node_name })
                .first(&mut tx).await
                .expect("Could not insert new node");
            assert!(resp.is_some());
            tx.commit().await.expect("Could not commit transaction");

            StorageNodeID(resp.unwrap())
        }
    }
}

