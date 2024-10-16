use std::ffi::OsString;
use std::path::PathBuf;
use std::net::SocketAddr;
use std::io::IsTerminal;

use clap::{Parser, Subcommand};
use tokio::net::{TcpSocket, TcpStream};
use tokio::io::{BufReader, AsyncBufReadExt, AsyncWriteExt};

mod message;
mod node;

use message::Message;
use node::OperationError;
use uuid::Uuid;

#[derive(Debug, Parser)]
#[command(version, about)]
struct CLI {
    /// interface to connect through
    #[arg(short='I', long="iface")]
    bind_iface: Option<String>,

    /// address connect to, ip:port
    bind_addr: String,

    /// command to execute against the server
    #[command(subcommand)]
    command: Option<DiagnosticsCommand>,
}

#[tokio::main]
async fn main() {
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
    let mut stream = socket.connect(addr).await.expect("Could not bind socket to address");

    if let Some(command) = cli.command {
        command.run(&mut stream).await;
    } else {
        let mut stdin = BufReader::new(tokio::io::stdin());
        loop {
            let mut line = String::new();
            eprint!("> ");
            if let Err(e) = stdin.read_line(&mut line).await {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    break;
                } else {
                    eprintln!("error reading line from stdin: {e}. Exitting");
                }
                return;
            }
            if line.len() == 0 {
                eprintln!("\nbunny bye 🐇");
                return;
            }

            let Some(mut words) = shlex::split(&line) else {
                eprintln!("Invalid quoted line: {line:?}");
                continue;
            };

            if words.len() == 0 {
                continue;
            }

            words.insert(0, "cli".to_string());

            #[derive(Debug, Parser)]
            struct DiagnosticsCLI {
                #[command(subcommand)]
                cmd: DiagnosticsCommand,
            }

            match DiagnosticsCLI::try_parse_from(words).map(|x| x.cmd) {
                Ok(DiagnosticsCommand::Bye) => {
                    break;
                }
                Ok(cmd) => cmd.run(&mut stream).await,
                Err(e) => e.print().expect("could not print command error"),
            }
        }
        eprintln!("bunny bye 🐇");
    }
}

#[derive(Debug, Subcommand, Clone)]
enum DiagnosticsCommand {
    /// exists interactive mode
    Bye,
    /// sends a GetVersion message to the node
    GetVersion,
    /// sends a WriteFile to the node
    WriteFile {
        /// UUID for file. if left empty, a UUID is generated
        #[arg(short='u', long="uuid")]
        uuid: Option<String>,

        /// local path of file to write
        #[arg(short='f', long="file")]
        file: Option<PathBuf>,

        /// contents to write, verbatim
        contents: Option<OsString>,
    },
    /// sends a ReadFile to the node
    ReadFile {
        /// UUID for file
        uuid: String,

        /// local path to write the output to
        #[arg(short='o', long="output")]
        output_path: Option<PathBuf>,
    },
}

impl DiagnosticsCommand {
    async fn run(self, connection: &mut TcpStream) {
        match self {
            DiagnosticsCommand::Bye => {
                eprintln!("whar the hell");
            }
            DiagnosticsCommand::GetVersion => {
                let request = message::Message::GetVersion;
                let id = message::MessageID(0);
                message::write_message(connection, id, request).await.expect("Could not send request");
                let (_rid, response) = message::parse_message(connection).await.expect("Could not acquire reply");
                eprintln!("Got response: {response:?}");
            }
            DiagnosticsCommand::WriteFile { uuid, file, contents } => {
                let uuid = match uuid.map(|x| Uuid::parse_str(&x)) {
                    Some(Ok(u)) => u,
                    Some(Err(e)) => {
                        eprintln!("Could not parse UUID: {e:?}");
                        return;
                    }
                    None => {
                        let u = Uuid::now_v7();
                        eprintln!("Writing to UUID {}", u.hyphenated().encode_lower(&mut Uuid::encode_buffer()));
                        u
                    }
                };

                let data: Vec<u8> = match (file, contents) {
                    (Some(path), None) => {
                        // TODO: This fails if the file contains invalid UTF-8
                        match tokio::fs::read_to_string(&path).await {
                            Ok(data) => data.bytes().collect(),
                            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                                eprintln!("Could not find file {}!", path.display());
                                return;
                            }
                            Err(e) => {
                                eprintln!("Could not read file {}: {e:?}", path.display());
                                return;
                            }
                        }
                    }
                    (None, Some(data)) => data.as_encoded_bytes().to_vec(),
                    (None, None) => {
                        eprintln!("Must specify either -f or supply data to write!");
                        return;
                    }
                    (Some(_), Some(_)) => {
                        eprintln!("Must not specify both -f and supply data to write!");
                        return;
                    }
                };
                eprintln!("Writing {} bytes", data.len());

                let request = message::Message::WriteFile(uuid, data);
                let id = message::MessageID(0);
                message::write_message(connection, id, request).await.expect("Could not send request");
                let (_rid, response) = message::parse_message(connection).await.expect("Could not acquire reply");
                eprintln!("Got response: {response:?}");
            }
            DiagnosticsCommand::ReadFile { uuid, output_path } => {
                let uuid = match Uuid::parse_str(&uuid) {
                    Ok(u) => u,
                    Err(e) => {
                        eprintln!("Could not parse UUID: {e:?}");
                        return;
                    }
                };

                let request = message::Message::ReadFile(uuid);
                let id = message::MessageID(0);
                message::write_message(connection, id, request).await.expect("Could not send request");
                let (_rid, response) = message::parse_message(connection).await.expect("Could not acquire reply");

                let message::Message::FileContents(data) = response else {
                    eprintln!("got wrong response type from node; expected FileContents, got {response:?}");
                    return;
                };

                if let Some(path) = output_path {
                    match tokio::fs::write(&path, data).await {
                        Ok(()) => {}
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                            eprintln!("Could not find output file {}!", path.display());
                            return;
                        }
                        Err(e) => {
                            eprintln!("Could not write to output file {}: {e:?}", path.display());
                            return;
                        }
                    }
                } else {
                    let pager = std::env::var("PAGER").unwrap_or("less".to_string());
                    let mut child = tokio::process::Command::new(pager)
                        .stdin(std::process::Stdio::piped())
                        .spawn()
                        .expect("Could not spawn $PAGER");

                    let mut child_stdin = child.stdin.take().unwrap();
                    child_stdin.write_all(&data).await.expect("Could not write stdin of $PAGER");
                    std::mem::drop(child_stdin);
                    child.wait().await.expect("Could not wait for $PAGER to quit");
                }
            }
        }
    }
}
