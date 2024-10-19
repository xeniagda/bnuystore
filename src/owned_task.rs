use std::sync::Arc;

use tokio::task::JoinHandle;
use tokio::sync::Notify;

/// A simple wrapper that represents an "owned" task,
/// only in the sense that when this struct is dropped, the internal task
/// is cancelled.
pub struct OwnedTask {
    handle: JoinHandle<()>,
    was_finished: Arc<Notify>,
}

impl OwnedTask {
    pub fn spawn<F>(future: F) -> Self
    where
        F: std::future::Future + Send + 'static,
    {
        let was_finished = Arc::new(Notify::new());

        let handle = tokio::task::spawn({
            let was_finished = was_finished.clone();
            async move {
                future.await; // discard result
                was_finished.notify_waiters();
                // TODO: Log that the future was finished?
            }
        });
        OwnedTask {
            handle,
            was_finished,
        }
    }

    pub async fn wait_until_finished(&self) {
        if !self.handle.is_finished() {
            // for the was_finished to be notified, either the internal task needs to finish
            // or we must be dropped.
            self.was_finished.notified().await;
        }
    }
}

impl Drop for OwnedTask {
    fn drop(&mut self) {
        self.handle.abort();
        // This is technically wrong, the task could still be running after we call abort.
        // However, in practice this shouldn't matter, the task can't *do* anything after abort has been called
        self.was_finished.notify_waiters();
    }
}
