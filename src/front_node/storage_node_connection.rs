use std::collections::HashMap;
use std::sync::Arc;

use tokio::net::{tcp, TcpStream};
use tokio::sync::{Mutex, Notify, oneshot};

use crate::message::{Message, MessageID, ParseMessageError, parse_message, write_message};
use super::config::StorageNodeConfig;

/// A connection to a storage node
/// An "inner" connection is not thread-safe, but must be wrapped in a Mutex to use
struct StorageNodeConnectionInner {
    stream: tcp::OwnedWriteHalf,
    next_message_id: MessageID,

    /// If the channel dies, all senders are dropped
    waiting_responses: HashMap<MessageID, oneshot::Sender<Message>>,
    // todo: auth token

    /// In case any communication error occurs, we want any attempt to `communicate`
    /// with this connection to fail. This bool is "sticky", it cannot be unset
    is_disconnected: bool,
}

/// Only locks the mutex while a message is being sent
pub struct StorageNodeConnection {
    inner: Arc<Mutex<StorageNodeConnectionInner>>,
    #[allow(unused)]
    pub disconnect: Arc<Notify>,
}

/// If an error occurs, the calling code should unconditionally abort
/// An long-living task
#[derive(Debug, Clone, Copy)]
pub enum ConnectionError {
    ClientDisconnected,
}

impl StorageNodeConnection {
    pub async fn connect(cfg: &StorageNodeConfig) -> Result<Self, std::io::Error> {
        let stream = TcpStream::connect((cfg.ip.clone(), cfg.port)).await?;
        let (mut read, write) = stream.into_split();

        let inner = StorageNodeConnectionInner {
            stream: write,
            next_message_id: MessageID(0),
            waiting_responses: HashMap::new(),
            is_disconnected: false,
        };
        let inner = Arc::new(Mutex::new(inner));
        let disconnect = Arc::new(Notify::new());

        // TODO: do we wanna store the task somewhere?
        // It could outlive the connection which is not great
        let _recv_task = tokio::spawn({
            let inner = inner.clone();
            let disconnect = disconnect.clone();

            async move {
                loop {
                    match parse_message(&mut read).await {
                        Ok((id, msg)) => {
                            let mut inner = inner.lock().await;
                            let Some(sender) = inner.waiting_responses.remove(&id) else {
                                eprintln!("Got response to non-existant request {id:?}. Ignoring");
                                continue;
                            };
                            std::mem::drop(inner);
                            if let Err(_) = sender.send(msg) {
                                eprintln!("Got response to request that does exist, but no one's waiting for it. Ignoring");
                            }
                        }
                        Err(e) => {
                            eprintln!("Parsing message failed:");
                            match e {
                                ParseMessageError::IOError(e) => {
                                    eprintln!("IO Error: {e:?}");
                                }
                                ParseMessageError::ParseJsonError(e) => {
                                    eprintln!("Invalid JSON received: {e:?}");
                                }
                                ParseMessageError::ParseUuidError(e) => {
                                    eprintln!("Invalid UUID received: {e:?}");
                                }
                                ParseMessageError::RequestTooLarge(n) => {
                                    eprintln!("Tried to allocate {} MiB", n>>20);
                                }
                            }
                            eprintln!("Killing connection.");
                            disconnect.notify_waiters();

                            let mut inner = inner.lock().await;
                            inner.is_disconnected = true;
                            for (_id, sender) in inner.waiting_responses.drain() {
                                std::mem::drop(sender);
                            }
                            break;
                        }
                    }
                }
            }
        });

        Ok(StorageNodeConnection {
            inner,
            disconnect,
        })
    }

    // TODO: Register a timeout task
    pub async fn communicate(
        &self,
        message: Message,
    ) -> Result<Message, ConnectionError> {
        let listener = {
            let mut inner = self.inner.lock().await;
            let id = {
                let id = inner.next_message_id;

                while {
                    inner.next_message_id.0 = inner.next_message_id.0.wrapping_add(1);
                    inner.waiting_responses.contains_key(&inner.next_message_id)
                } {}

                id
            };

            let (sender, listener) = oneshot::channel();
            inner.waiting_responses.insert(id, sender);

            write_message(&mut inner.stream, id, message)
                .await
                .map_err(|_| ConnectionError::ClientDisconnected)?;
            listener
        };

        match listener.await {
            Ok(m) => Ok(m),
            Err(_recverror) => Err(ConnectionError::ClientDisconnected),
        }
    }
}
