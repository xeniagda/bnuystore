#[allow(unused)]
use tracing::{trace, debug, info, warn, error, instrument};

use std::path::PathBuf;
use std::mem::drop;
use std::collections::HashMap;
use std::sync::Arc;

use uuid::Uuid;
use tokio::sync::{RwLock, Notify};
use tokio::runtime::Handle;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::io::ErrorKind;

#[derive(Debug)]
#[allow(unused)]
pub enum OperationError {
    NoFileWithUuid(Uuid),
    IOError(std::io::Error),
}

type Result<T> = std::result::Result<T, OperationError>;

#[derive(Clone)]
pub struct Node(Arc<NodeInner>);

struct NodeInner {
    /// Safety: while running, this folder may not be modified. Files may not be deleted etc.
    data_folder: PathBuf,

    // TODO: We should really track whether each file is being read or written to
    // If multiple threads wanna read from the same file, that is okay

    /// List of locked files on disk. Each item in the map is locked, with a debugging string
    /// attached, saying why it's locked. Debugging strings are useful for diagnosing deadlocks
    locked_files: RwLock<HashMap<Uuid, String>>,

    /// Whenever a file is unlocked, this notify is notified to make any pending lock_file calls
    /// re-check if their file has been unlocked.
    file_unlocked: Notify,
}

pub struct FileLock {
    for_uuid: Uuid,
    node: Node,
    runtime_handle: Option<Handle>,
}

impl std::fmt::Debug for FileLock {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "FileLock {{ for_uuid = {}, .. }}", self.for_uuid)
    }
}

impl Drop for FileLock {
    fn drop(&mut self) {
        let for_uuid = self.for_uuid;
        trace!(%for_uuid, "Releasing lock");

        let node = self.node.0.clone();
        let runtime_handle = self.runtime_handle.take().expect("Dropped FileLock multiple times");
        let for_uuid = self.for_uuid.clone();

        runtime_handle.spawn(async move {
            let mut locked_files = node.locked_files.write().await;
            match locked_files.remove(&for_uuid) {
                Some(reason) => {
                    trace!(%for_uuid, reason, "Lock released");
                }
                None => {
                    warn!(%for_uuid, "Lock was not held");
                }
            }
            drop(locked_files);
            node.file_unlocked.notify_waiters();
        });
    }
}

impl FileLock {
    pub fn basename(&self) -> String {
        self.for_uuid.hyphenated().encode_lower(&mut Uuid::encode_buffer()).to_string()
    }

    pub fn path(&self) -> PathBuf {
        let mut path = self.node.0.data_folder.clone();
        path.push(self.basename());
        path
    }

    #[instrument(level = "debug")]
    pub async fn read(&self) -> Result<Vec<u8>> {
        let path = self.path();
        let fres = File::options()
            .read(true)
            .open(&path)
            .await;

        let mut f = match fres {
            Ok(f) => f,
            Err(e) if e.kind() == ErrorKind::NotFound => {
                error!("Could not read file: not found");
                return Err(OperationError::NoFileWithUuid(self.for_uuid.clone()));
            }
            Err(e) => {
                error!(?e, "Could not read file");
                return Err(OperationError::IOError(e));
            }
        };
        trace!(path = %path.display(), "File opened");

        let mut buf = Vec::new();
        f.read_to_end(&mut buf).await.map_err(OperationError::IOError)?;

        trace!(n_bytes = buf.len(), "Read file");

        Ok(buf)
    }

    #[instrument(level = "debug", skip(data), fields(data.len = data.len()))]
    pub async fn write(&self, data: Vec<u8>) -> Result<()> {
        let path = self.path();
        let mut f = File::options()
            .write(true)
            .create(true)
            .open(&path)
            .await
            .map_err(OperationError::IOError)?;

        trace!(path = %path.display(), "File opened");

        f.write_all(&data).await.map_err(OperationError::IOError)?;

        trace!(path = %path.display(), "Wrote");

        Ok(())
    }

    #[allow(unused)]
    #[instrument(level = "debug")]
    pub async fn delete(&self) -> Result<()> {
        let path = self.path();
        match tokio::fs::remove_file(&path).await {
            Ok(_) => Ok(()),
            Err(e) if e.kind() == ErrorKind::NotFound => {
                error!(path = %path.display(), "Could not delete file: not found");
                return Err(OperationError::NoFileWithUuid(self.for_uuid.clone()));
            }
            Err(e) => {
                error!(?e, path = %path.display(), "Could not delete file");
                return Err(OperationError::IOError(e));
            }
        }
    }
}

impl Node {
    pub async fn new(data_folder: PathBuf) -> Result<Node> {
        if !data_folder.exists() {
            debug!(data_folder = %data_folder.display(), "Creating data folder");
            tokio::fs::create_dir(&data_folder).await.map_err(OperationError::IOError)?;
        }

        Ok(Node(Arc::new(NodeInner {
            data_folder,
            locked_files: RwLock::new(HashMap::new()),
            file_unlocked: Notify::new(),
        })))
    }

    /// Block any other task from accessing this file.
    /// If the file is already locked, this function waits until the file is unlocked to continue
    /// MAKE SURE to call `unlock_file` to drop the lock.

    // TODO: maybe start a task that waits for 3 seconds or something, sees if the file is still locked and logs a
    // warning (we probably don't want files to be locked for that long)
    #[instrument(level = "trace", skip(self))]
    pub async fn lock_file(&self, uuid: &Uuid, reason: &str) -> FileLock {
        loop {
            let mut locked_files = self.0.locked_files.write().await;
            if !locked_files.contains_key(&uuid) {
                trace!(%uuid, reason, "Locked file");
                locked_files.insert(uuid.clone(), reason.to_string());
                return FileLock {
                    for_uuid: uuid.clone(),
                    node: self.clone(),
                    runtime_handle: Some(Handle::current()),
                };
            }
            debug!(%uuid, reason, "File already locked, waiting...");
            drop(locked_files);
            // if the file is locked, we wait until some file has been unlocked and we try again
            self.0.file_unlocked.notified().await;
        }
    }
}
