use std::sync::Arc;
use std::path::PathBuf;
use std::net::SocketAddr;
use clap::Parser;

use axum::{
    routing::{get, post},
    extract::{Path, State},
    response::Response,
    body::{Bytes, Body},
    Router,
};
use uuid::Uuid;

mod front_node;
mod message;

#[derive(Parser)]
struct CLI {
    /// Path to config toml file
    #[arg(short='c', long="config-file")]
    config_file: PathBuf,

    /// address to serve HTTP over, ip:port
    #[arg(long="http-listen")]
    http_addr: String,
}

#[derive(Clone)]
struct AppState {
    node: Arc<front_node::FrontNode>,
}

#[tokio::main]
async fn main() {
    let cli = CLI::parse();
    let cfg = front_node::config::Config::read_from_path(cli.config_file).await;

    let front_node = front_node::FrontNode::start_from_config(cfg).await.expect("could not start front node");

    let state = AppState {
        node: Arc::new(front_node),
    };

    let Ok(addr) = cli.http_addr.parse::<SocketAddr>() else {
        eprintln!("Could not parse HTTP address {}. Format must be IP:PORT", cli.http_addr);
        return;
    };

    let router = Router::new()
        .route("/version", get(|| async {
            format!("{name} {bin} {ver}", name=env!("CARGO_PKG_NAME"), bin=env!("CARGO_BIN_NAME"), ver=env!("CARGO_PKG_VERSION"))
        }))
        .route("/get/file-by-path/*full_path", get(get_file_by_name))
        .route("/upload/file-by-path/*full_path", post(upload_file))
        .with_state(state)
        ;

    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("Could not bind to HTTP address {addr}: {e}");
            return;
        }
    };

    axum::serve(listener, router).await.expect("HTTP server failed");

    // loop {
    //     tokio::task::yield_now().await;

    // }
}

async fn get_file_by_name(
    Path(full_path): Path<String>,
    State(state): State<AppState>,
) -> Response {
    let (path, file) = full_path.rsplit_once('/')
        .map(|(path, file)| (path.to_string(), file.to_string()))
        .unwrap_or(("".to_string(), full_path));

    match state.node.get_file(file, path).await {
        Ok(Some((data, uuid))) => {
            let uuid_str = uuid.as_hyphenated().encode_lower(&mut Uuid::encode_buffer()).to_string();
            Response::builder()
                .status(200)
                .header("X-File-UUID", uuid_str)
                .body(Body::from(data))
                .unwrap()
        }
        Ok(None) => {
            Response::builder()
                .status(404)
                .body(Body::from("No such file"))
                .unwrap()
        }
        Err(e) => {
            Response::builder()
                .status(500)
                .body(Body::from(format!("Error finding file: {e:?}")))
                .unwrap()
        }
    }
}

async fn upload_file(
    Path(full_path): Path<String>,
    State(state): State<AppState>,
    body: Bytes,
) -> Response {
    let (path, file) = full_path.rsplit_once('/')
        .map(|(path, file)| (path.to_string(), file.to_string()))
        .unwrap_or(("".to_string(), full_path));

    match state.node.upload_file(file, path, body.to_vec()).await {
        Ok(uuid) => {
            let uuid_str = uuid.as_hyphenated().encode_lower(&mut Uuid::encode_buffer()).to_string();
            Response::builder()
                .status(200)
                .header("X-File-UUID", uuid_str)
                .body(Body::from("upload successful"))
                .unwrap()
        }
        Err(e) => {
            Response::builder()
                .status(500)
                .body(Body::from(format!("Error finding file: {e:?}")))
                .unwrap()
        }
    }
}
