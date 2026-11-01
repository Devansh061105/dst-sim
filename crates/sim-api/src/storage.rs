use std::error::Error;
use std::fmt;

/// A node's disk: named files with page-cache semantics.
///
/// Writes land in the operating system's page cache first. They are visible to reads at once,
/// but only survive a crash once [`sync`](Storage::sync) succeeds on that file, which is why only
/// `sync` can fail. Calls complete immediately in virtual time; disk latency is not modelled.
pub trait Storage {
    /// The file's current contents as this process sees them, including unsynced writes. A file
    /// that was never written is empty.
    fn read(&self, file: &str) -> Vec<u8>;

    fn file_len(&self, file: &str) -> u64;

    /// Writes `data` at `offset`, extending the file (with zeros if needed) past its end.
    fn write(&mut self, file: &str, offset: u64, data: &[u8]);

    /// Writes `data` at the end of the file.
    fn append(&mut self, file: &str, data: &[u8]) {
        let end = self.file_len(file);
        self.write(file, end, data);
    }

    /// Cuts the file to `len` bytes, or extends it with zeros.
    fn truncate(&mut self, file: &str, len: u64);

    /// Makes every earlier write and truncate of `file` durable (fsync).
    fn sync(&mut self, file: &str) -> Result<(), SyncError>;
}

/// `fsync` failed. As on Linux, the unsynced writes may already be gone: they will not become
/// durable even if a later sync succeeds. The only safe reaction is to stop trusting the file
/// until it is re-read after a restart.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SyncError;

impl fmt::Display for SyncError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "fsync failed; unsynced writes may have been lost")
    }
}

impl Error for SyncError {}
