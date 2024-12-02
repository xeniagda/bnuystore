#[allow(unused)]
use tracing::{trace, debug, info, warn, error, instrument};

use tracing_subscriber::fmt::{self, format::FmtSpan};
use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::prelude::*;

use clap::Parser;
use std::path::PathBuf;
use std::net::SocketAddr;
use tokio::net::TcpSocket;

mod message;
use message::Message;

mod storage_node;
use storage_node::{Node, OperationError};

#[derive(Debug, Parser)]
#[command(version, about)]
struct CLI {
    /// address to bind on, ip:port
    #[arg(short='a', long="addr")]
    bind_addr: String,
    /// interface to bind on. make sure to pick an interface not directly exposed to the internet!
    #[arg(short='I', long="iface")]
    bind_iface: Option<String>,

    /// folder to store all files in
    #[arg(short='d', long="data-dir")]
    data_directory: PathBuf,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(
            fmt::layer()
                .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        )
        .with(EnvFilter::from_default_env())
        .init();

    let cli = CLI::parse();

    let addr: SocketAddr = cli.bind_addr.parse().expect("Could not parse socket address");

    let socket = match addr {
        SocketAddr::V4(_) => TcpSocket::new_v4(),
        SocketAddr::V6(_) => TcpSocket::new_v6(),
    }.expect("Could not create TCP socket");

    if let Some(iface) = cli.bind_iface {
        let mut bytes = iface.as_bytes().to_vec();
        bytes.push(0); // zero terminator for linux moment
        socket.bind_device(Some(bytes.as_slice())).expect("Could not bind to interface");
    }

    socket.bind(addr).expect("Could not bind socket to address");
    let listener = socket.listen(1).expect("Could not listen on socket"); // backlog of 1, we should never have more than one connection

    info!("Listening for connections");

    let node = Node::new(cli.data_directory).await.expect("Could not initialize node");

    loop {
        let (mut stream, addr) = listener.accept().await.expect("Could not accept connection");
        info!(%addr, "Got a connection");

        let node = node.clone();
        tokio::task::spawn(async move {
            loop {
                let (id, message) = match message::parse_message(&mut stream).await {
                    Ok(x) => x,
                    Err(message::ParseMessageError::IOError(e) ) => {
                        error!(?e, "IO error parsing command. Terminating");
                        break;
                    }
                    Err(e) => {
                        error!(?e, "(recoverable?) Error parsing command");
                        continue;
                    }
                };

                debug!(?id, %message, "Got a message");
                match handle_message(&node, &message).await {
                    Ok(reply) => {
                        debug!(?id, %reply, "Replying");
                        message::write_message(&mut stream, id, reply)
                            .await
                            .expect("Could not send response")
                    }
                    Err(e) => {
                        debug!(?e, %message, ?e, "Error handling message");
                        let reply = Message::Error(format!("{e:?}"));
                        message::write_message(&mut stream, id, reply)
                            .await
                            .expect("Could not send response")
                    }
                }
            }
        });
    }
}

async fn handle_message(
    node: &Node,
    message: &Message,
) -> Result<Message, OperationError> {
    Ok(match message {
        Message::GetVersion => {
            Message::MyVersionIs(env!("CARGO_PKG_VERSION").to_string())
        }
        Message::ReadFile(uuid) => {
            let lock = node.lock_file(uuid, "ReadFile request").await;
            let data = lock.read().await.expect("could not read specified file");

            Message::FileContents(data)
        }
        Message::WriteFile(uuid, data) => {
            let lock = node.lock_file(uuid, "WriteFile request").await;
            lock.write(data.clone()).await.expect("could not read specified file");

            Message::Ack
        }
        Message::DeleteFile(_) => todo!(),
        Message::MyVersionIs(_) => todo!(),
        Message::FileContents(_) => todo!(),
        Message::Ack => todo!(),
        Message::Error(_) => todo!(),
    })
}
