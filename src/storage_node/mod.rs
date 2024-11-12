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

impl Drop for FileLock {
    fn drop(&mut self) {
        let node = self.node.0.clone();
        let runtime_handle = self.runtime_handle.take().expect("Dropped FileLock multiple times");
        let for_uuid = self.for_uuid.clone();

        runtime_handle.spawn(async move {
            let mut locked_files = node.locked_files.write().await;
            locked_files.remove(&for_uuid);
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

    pub async fn read(&self) -> Result<Vec<u8>> {
        let fres = File::options()
            .read(true)
            .open(self.path())
            .await;

        let mut f = match fres {
            Ok(f) => f,
            Err(e) if e.kind() == ErrorKind::NotFound => {
                return Err(OperationError::NoFileWithUuid(self.for_uuid.clone()));
            }
            Err(e) => return Err(OperationError::IOError(e)),
        };

        let mut buf = Vec::new();
        f.read_to_end(&mut buf).await.map_err(OperationError::IOError)?;

        Ok(buf)
    }

    pub async fn write(&self, data: Vec<u8>) -> Result<()> {
        let mut f = File::options()
            .write(true)
            .create(true)
            .open(self.path())
            .await
            .map_err(OperationError::IOError)?;

        f.write_all(&data).await.map_err(OperationError::IOError)?;

        Ok(())
    }

    #[allow(unused)]
    pub async fn delete(&self) -> Result<()> {
        match tokio::fs::remove_file(self.path()).await {
            Ok(_) => Ok(()),
            Err(e) if e.kind() == ErrorKind::NotFound => {
                return Err(OperationError::NoFileWithUuid(self.for_uuid.clone()));
            }
            Err(e) => return Err(OperationError::IOError(e)),
        }
    }
}

impl Node {
    pub async fn new(data_folder: PathBuf) -> Result<Node> {
        if !data_folder.exists() {
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
    pub async fn lock_file(&self, uuid: Uuid, reason: &str) -> FileLock {
        loop {
            let mut locked_files = self.0.locked_files.write().await;
            if !locked_files.contains_key(&uuid) {
                locked_files.insert(uuid.clone(), reason.to_string());
                return FileLock {
                    for_uuid: uuid,
                    node: self.clone(),
                    runtime_handle: Some(Handle::current()),
                };
            }
            drop(locked_files);
            // if the file is locked, we wait until some file has been unlocked and we try again
            self.0.file_unlocked.notified().await;
        }
    }
}
