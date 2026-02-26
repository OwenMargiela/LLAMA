//! Filepath: src/caching_layer.rs
//!
//! Log-Structured Storage Flush Protocols
//!
//! LLAMA leverages the power of append-only writes—that is, writes that are exclusively localized
//! at the end of a log file.
//!
//! Across both spinning disk-based as well as SSD-backed storage schemas, the performance gains from
//! sequential writes and reads are substantial enough to design systems entirely built around these
//! access patterns.
//!
//!
//!
//! With that being said, there are scenarios that don't guarantee that writes are performed exclusively
//! at the very tip of the log. Buffers are all allotted unique [`FOUR_KB_PAGE`] sized address spaces
//! that directly map onto data regions on secondary storage. After a buffer has been sealed, it will be
//! given a new address space and its contents will be written to a region on disk that maps to its previous
//! address space.
//!
//! In special cases where a buffer may be sealed before other neighboring flush buffers, but its contents are
//! written to secondary storage after said buffers, this results in writing the buffer's payload into a fragmented
//! section on storage which would be, by definition, a random access pattern.
//!
//! Writes are, however, guaranteed to be localized around the tail of the log. Write distances are <= ( [`RING_SIZE`] * [`FOUR_KB_PAGE`] )
//! KB away from the tail of the buffer. We call these Tail-Localized Writes, or just Localized Writes.
//!
//!
//!
//! Strict append-only patterns are guaranteed by Ordered Serialized writes.
//!
//! Benchmarks will be used to test whether Localized or Serialized Ordered Writes are the most optimal.
//!
//! For now only localized writes will be implemented. Mechanism to implement Serialized Writes
//!
//!

use rio::Rio;

#[allow(unused_imports)]
use crate::{data_objects::FOUR_KB_PAGE, flush_buffer::RING_SIZE};
use std::{fs::File, io, sync::Arc};

/// Flush Buffers must adherer to Strict Serialized Ordered Writes
const SERIALIZED_ORDERING: u8 = 0;

/// Flag Buffers are permitted to write within a localized region
/// within [`RING_SIZE`] * [`FOUR_KB_PAGE`] of the tail
const LOCALIZED_WRITES: u8 = 1;

/// Controls whether writes are ordered (drain) or parallel
#[derive(Clone, Copy)]
pub enum WriteMode {
    /// Parallel localized writes — lock-free
    Localized,
    /// Serialized ordered writes — drain ordering enforced
    Serialized,
}

/// Unified appender — handles both localized and serialized flush strategies.
pub struct Appender {
    store: Arc<File>,
    flusher: Arc<Rio>,
    mode: WriteMode,
}

impl Appender {
    pub fn new(io_uring: Rio, file_handle: Arc<File>, mode: WriteMode) -> Self {
        Self {
            flusher: Arc::new(io_uring),
            store: file_handle,
            mode,
        }
    }

    /// Flush the given buffer contents to its assigned LSS range.
    ///
    /// - `buffer_data`: slice of the buffer (0..used_bytes)
    /// - `base_offset`: the logical LSS base claimed at seal time
    ///
    /// Returns Ok(()) on success, or error if I/O failed.
    ///

    pub async fn flush(&self, buffer_data: &[u8], at: u64) -> io::Result<()> {
        match self.mode {
            WriteMode::Localized => {
                self.flusher
                    .write_at(&*self.store, &buffer_data, at)
                    .await?;
            }
            WriteMode::Serialized => {
                self.flusher
                    .write_at_ordered(&*self.store, &buffer_data, at, rio::Ordering::Drain)
                    .await?;
            }
        }
        Ok(())
    }
}

pub enum FlushBehavior {
    /// Strict Ordered writes
    WaitAppender(Appender),
    /// localized writes
    NoWaitAppender(Appender),
}

impl FlushBehavior {
    pub fn with_wait_appender(io_uring: Rio, file: Arc<File>) -> Self {
        FlushBehavior::WaitAppender(Appender::new(io_uring, file, WriteMode::Serialized))
    }

    pub fn with_no_wait_appender(io_uring: Rio, file: Arc<File>) -> Self {
        FlushBehavior::NoWaitAppender(Appender::new(io_uring, file, WriteMode::Localized))
    }

    pub async fn flush_buffer(&self, buffer_data: &[u8], at: u64) -> io::Result<()> {
        match self {
            FlushBehavior::WaitAppender(a) | FlushBehavior::NoWaitAppender(a) => {
                a.flush(buffer_data, at).await
            }
        }
    }
}
