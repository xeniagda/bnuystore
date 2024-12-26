#[allow(unused)]
use tracing::{trace, debug, info, warn, error, instrument};

use tracing_subscriber::fmt::{self, format::FmtSpan};
use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::prelude::*;

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
use http::status::StatusCode;
use uuid::Uuid;

mod front_node;
mod message;

use front_node::tys::Error;

#[derive(Parser)]
struct CLI {
    /// Path to config toml file
    #[arg(short='c', long="config-file")]
    config_file: PathBuf,
}

#[derive(Clone)]
struct AppState {
    node: Arc<front_node::FrontNode>,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(
            fmt::layer()
                .compact()
                .with_target(false)
                .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        )
        .with(EnvFilter::from_default_env())
        .init();

    let cli = CLI::parse();


    let cfg = front_node::config::Config::read_from_path(cli.config_file).await;


    let Ok(addr) = cfg.http_server.listen_addr.parse::<SocketAddr>() else {
        error!("Could not parse HTTP address {}. Format must be IP:PORT", cfg.http_server.listen_addr);
        return;
    };

    debug!("Loaded config. Starting node");
    let front_node = front_node::FrontNode::start_from_config(&cfg).await.expect("could not start front node");
    let front_node = Arc::new(front_node);

    info!("Starting SSH server");

    // TODO: Grab handle to monitor ssh task status maybe
    // or create some channel to monitor more than just if it's alive?
    tokio::task::spawn({
        let front_node = front_node.clone();
        async move {
            front_node::sftp::launch_sftp_server(&cfg.sftp_server, front_node).await;
            error!("SFTP server shut down. Not restarting.");
        }
    });

    let state = AppState {
        node: front_node,
    };

    info!("Starting HTTP router.");
    let router = Router::new()
        .route("/version", get(|| async {
            format!("{name} {bin} {ver}", name=env!("CARGO_PKG_NAME"), bin=env!("CARGO_BIN_NAME"), ver=env!("CARGO_PKG_VERSION"))
        }))
        .route("/get/file-by-path/*full_path", get(get_file_by_name))
        .route("/upload/file-by-path/*full_path", post(upload_file))
        .route("/create/directory-by-path/*full_path", post(create_directory))
        .route("/list-directory/*full_path", get(list_directory))
        .route("/list-directory/", get(|state| list_directory(Path("".to_string()), state)))
        .with_state(state)
        ;

    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            error!(%addr, ?e, "Could not bind to HTTP address");
            return;
        }
    };

    info!("Front node starting.");
    axum::serve(listener, router).await.expect("HTTP server failed");
}

fn error_response(status: StatusCode, message: &str) -> Response {
    Response::builder()
        .status(status)
        .body(Body::from(message.to_string()))
        .unwrap()
}

#[instrument(skip(state))]
async fn get_file_by_name(
    Path(full_path): Path<String>,
    State(state): State<AppState>,
) -> Response {
    let uuid = match state.node.file_uuid_for_path(&full_path, None).await {
        Ok(uuid) => uuid,
        Err(Error::NoSuchFile) => {
            debug!("No such file");
            return error_response(StatusCode::NOT_FOUND, "No such file");
        }
        Err(Error::NoSuchDirectory { topmost_existing_directory: _ }) => {
            debug!("No such directory");
            return error_response(StatusCode::NOT_FOUND, "No such parent directory");
        }
        Err(e) => {
            error!(?e, "Error finding file");
            return error_response(StatusCode::INTERNAL_SERVER_ERROR, "Could not find file.");
        }
    };

    match state.node.get_file(uuid).await {
        Ok((data, info)) => {
            debug!(data.len = data.len(), %info.uuid, info.node_name, "Got file");
            let uuid_str = info.uuid.as_hyphenated().encode_lower(&mut Uuid::encode_buffer()).to_string();
            Response::builder()
                .status(StatusCode::OK)
                .header("X-File-UUID", uuid_str)
                .header("X-Node-Name", info.node_name)
                .body(Body::from(data))
                .unwrap()
        }
        Err(e) => {
            error!(?e, "Error reading file");
            return error_response(StatusCode::INTERNAL_SERVER_ERROR, "Could not read file.");
        }
    }
}

#[instrument(skip(state, body), fields(body.len = body.len()))]
async fn upload_file(
    Path(full_path): Path<String>,
    State(state): State<AppState>,
    body: Bytes,
) -> Response {
    let (path, file) = full_path.rsplit_once('/')
        .map(|(path, file)| (path.to_string(), file.to_string()))
        .unwrap_or(("".to_string(), full_path));

    info!("Uploading file");

    let dir = match state.node.directory_id_for_path(&path, None).await {
        Ok(id) => id,
        Err(Error::NoSuchDirectory { topmost_existing_directory: _ }) => {
            debug!("No such directory");
            return Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::from("No such directory"))
                .unwrap();
        }
        Err(e) => {
            error!(?e, "Error finding directory");
            return Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::from(format!("Error finding directory")))
                .unwrap();
        }
    };

    match state.node.upload_file(file, dir, body.to_vec()).await {
        Ok(uuid) => {
            let uuid_str = uuid.as_hyphenated().encode_lower(&mut Uuid::encode_buffer()).to_string();
            info!(uuid_str, "File uploaded");
            Response::builder()
                .status(StatusCode::OK)
                .header("X-File-UUID", uuid_str)
                .body(Body::from("upload successful"))
                .unwrap()
        }
        Err(e) => {
            error!(?e, "Error uploading file");
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::from(format!("Error finding file")))
                .unwrap()
        }
    }
}

#[instrument(skip(state))]
async fn create_directory(
    Path(full_path): Path<String>,
    State(state): State<AppState>,
) -> Response {
    let (parent_path, dir) = full_path.rsplit_once('/')
        .map(|(parent, dir)| (parent.to_string(), dir.to_string()))
        .unwrap_or(("".to_string(), full_path));

    info!(parent_path, dir, "Creating directory");

    let parent = match state.node.directory_id_for_path(&parent_path, None).await {
        Ok(id) => id,
        Err(Error::NoSuchDirectory { topmost_existing_directory: _ }) => {
            debug!("No parent directory");
            return Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::from("No parent directory"))
                .unwrap();
        }
        Err(e) => {
            error!(?e, "Error finding parent");
            return Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::from(format!("Error finding parent")))
                .unwrap();
        }
    };

    match state.node.create_directory(parent, dir).await {
        Ok(()) => {
            Response::builder()
                .status(StatusCode::OK)
                .body(Body::from("create successful"))
                .unwrap()
        }
        Err(e) => {
            error!(?e, "Error creating directory");
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::from(format!("Error creating directory: {e:?}")))
                .unwrap()
        }
    }
}

#[instrument(skip(state))]
async fn list_directory(
    Path(path): Path<String>,
    State(state): State<AppState>,
) -> Response {
    debug!(path, "Listing directory contents.");

    let dir = match state.node.directory_id_for_path(&path, None).await {
        Ok(id) => id,
        Err(Error::NoSuchDirectory { topmost_existing_directory: _ }) => {
            debug!("No such directory");
            return Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::from("No such directory"))
                .unwrap();
        }
        Err(e) => {
            error!(?e, "Error finding parent");
            return Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::from(format!("Error finding directory")))
                .unwrap();
        }
    };

    match state.node.list_directory(dir).await {
        Ok(list) => {
            use axum::response::IntoResponse;
            (StatusCode::OK, axum::Json(list)).into_response()
        }
        Err(e) => {
            error!(?e, "Error listing directory");
            Response::builder()
                .status(500)
                .body(Body::from(format!("Error finding file: {e:?}")))
                .unwrap()
        }
    }
}

