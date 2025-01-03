#[allow(unused)]
use tracing::{trace, debug, info, warn, error, instrument};

use std::path::PathBuf;

use std::collections::HashMap;

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct Config {
    pub database_connection: DatabaseConnectionOptions,
    pub http_server: HTTPServerOptions,
    pub sftp_server: SFTPServerOptions,

    pub storage_nodes: HashMap<String, StorageNodeConfig>,
}

impl Config {
    // prints error and exists if the config is malformed
    pub async fn read_from_path(path: PathBuf) -> Self {
        let contents = match tokio::fs::read_to_string(&path).await {
            Ok(c) => c,
            Err(e) => {
                if e.kind() == std::io::ErrorKind::NotFound {
                    error!(path = %path.display(), "Could not find config file");
                } else {
                    error!(?e, "Could not read config file");
                }
                std::process::exit(1);
            }
        };
        match toml::from_str(&contents) {
            Ok(c) => c,
            Err(e) => {
                error!(?e, "Could not parse config file");
                std::process::exit(1);
            }
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct DatabaseConnectionOptions {
    // TODO: allow to connect using host-port-password?
    pub database: String,
    pub socket_path: String,
    pub user: String,
}

impl DatabaseConnectionOptions {
    pub async fn mysql_opts(&self) -> mysql_async::Opts {
        mysql_async::OptsBuilder::default()
            .socket(Some(&self.socket_path))
            .user(Some(&self.user))
            .db_name(Some(&self.database))
            .into()
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct HTTPServerOptions {
    pub listen_addr: String,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct SFTPServerOptions {
    pub listen_addr: String,
    pub public_key: String,
    pub private_key: String,
}

const fn default_timeout() -> u64 { 1 }

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct StorageNodeConfig {
    pub addr: String,
    #[serde(default = "default_timeout")]
    pub timeout_s: u64,
}

