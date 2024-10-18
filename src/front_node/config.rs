use std::path::PathBuf;

use std::collections::HashMap;

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct Config {
    pub connection_options: ConnectionOptions,

    pub storage_nodes: HashMap<String, StorageNodeConfig>,
}

impl Config {
    // prints error and exists if the config is malformed
    pub async fn read_from_path(path: PathBuf) -> Self {
        let contents = match tokio::fs::read_to_string(&path).await {
            Ok(c) => c,
            Err(e) => {
                if e.kind() == std::io::ErrorKind::NotFound {
                    eprintln!("Could not find config file at {}", path.display());
                } else {
                    eprintln!("Could not read config file: {e:?}");
                }
                std::process::exit(1);
            }
        };
        match toml::from_str(&contents) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("Could not parse config file:");
                eprintln!("{}", e.message());
                std::process::exit(1);
            }
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct ConnectionOptions {
    // TODO: allow to connect using host-port-password?
    database: String,
    socket_path: String,
    user: String,
}

impl ConnectionOptions {
    pub async fn mysql_opts(&self) -> mysql_async::Opts {
        mysql_async::OptsBuilder::default()
            .socket(Some(&self.socket_path))
            .user(Some(&self.user))
            .db_name(Some(&self.database))
            .into()
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct StorageNodeConfig {
    ip: String,
    port: u16,
    // todo: auth token
}

