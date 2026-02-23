//! Filepath: src/flush_buffer.rs
//!
//! flush buffer for [`LLAMA`]
//!
//! Latch free I/O buffer ring
//! Ammortises writes by batching deltas into an in-memory flush buffer
//! All threads participate in managing this buffer.
//! As outline in the literature, the  flush procedure is as follows

//! 1.  Identify the state of the page that we intend to flush.
//!
//! 2.  Seize space in the LSS buffer into which to write the state.
//!
//! 3.  Perform Atomic operations to determine whether the flush will succeed.
//!
//! 4.  If  step  3  succeeds,  write  the  state  to  be  saved  into
//!     the  LSS.  While  we  are  writing  into  the  LSS,  LLAMA  prevents  
//!     the  buffer from being written to LSS secondary storage.
//!
//! 5.  If step 3 fails, write "Failed Flush" into the reserved space in the
//!     buffer.  This consumes storage but removes any ambiguity as to which
//!     flushes have succeeded or failed



// If you are doing concurrent program, PACK AS MUCH DATA AS YOU CAN into single values.
// The only thing I'm worried about is cache lines

use std::{
    cell::UnsafeCell,
    pin::Pin,
    sync::{
        atomic::{AtomicPtr, AtomicUsize, Ordering},
        Arc,
    },
    usize,
};

use crate::data_objects::FOUR_KB_PAGE;

/// A lightweight `Send + Sync` byte buffer with non-global cursor management.
///
/// Alternative containers such as `BytesMut` use a global mutable cursor, meaning
/// any change in cursor position is visible to all threads sharing the resource.
/// Here, cursor management is delegated to [`FlushBuffer`], which uses atomic
/// fetch-and-add to hand out non-overlapping byte ranges. This is what makes
/// the `unsafe impl Sync` sound.
///
/// # Safety
/// `Sync` is manually implemented because [`UnsafeCell`] opts out of it by default.
/// The invariant that upholds this is: all mutable access to the inner buffer is
/// mediated by [`FlushBuffer`], which guarantees no two threads are ever granted
/// overlapping regions.
#[derive(Debug)]
pub(crate) struct Buffer {
    buffer: UnsafeCell<Box<[u8]>>,
}

unsafe impl Send for Buffer {}
unsafe impl Sync for Buffer {}

/// A reference-counted handle to a shared [`Buffer`].
pub(crate) type SharedBuffer = Arc<Buffer>;

/// Flush buffer size in bytes
pub const BUFFER_SIZE: usize = 65536;

//  ┌──────────┬───────────┬────────────┬─────────────┐
//  │  offset  │  writers  │  reserved  │ flush│sealed│
//  │ (32 bit) │ (24 bit)  │  (6 bit)   │  [1] │  [0] │
//  └──────────┴───────────┴────────────┴─────────────┘
//
//  offset  — next free byte position inside the backing buffer.
//  writers — number of threads that have reserved space but not yet finished writing. capped at around 16mil concurrent writers. Which, should be enough
//
//  flush   — set while a flush is in progress; blocks new reservations.
//  sealed  — set once the buffer is full; blocks new reservations.
// ─────────────────────────────────────────────────────────────────────────────

/// Bit 0 — sealed flag. When set, the buffer is closed to new writers.
const SEALED_BIT: usize = 1 << 0;

/// Set while a flush is in progress; prevents new writers from entering a
/// buffer that is being drained to stable storage.
const FLUSH_IN_PROGRESS_BIT: usize = 1 << 1;

/// Each active writer contributes this value to the packed state word.
/// Writer count lives in bits `8..32` so a single writer is represented as `WRITER_ONE`.
const WRITER_SHIFT: usize = 8;
const WRITER_ONE: usize = 1 << WRITER_SHIFT;

/// Mask covering all writer-count bits (`8..32`).
//  Remember
//  FF == 11111111
//  00 == 00000000
const WRITER_MASK: usize = 0x00FF_FFFF00;

/// Offset lives in the top 32 bits.
const OFFSET_SHIFT: usize = 32;

/// Unit added to the state word to advance the offset by one byte.
const OFFSET_ONE: usize = 1 << OFFSET_SHIFT;

// Helpers to extract bit packed values

#[inline(always)]
fn state_offset(state: usize) -> usize {
    state >> OFFSET_SHIFT
}

#[inline(always)]
fn state_writers(state: usize) -> usize {

    // Writer mask singles out the writer count bits
    (state & WRITER_MASK) >> WRITER_SHIFT
}

#[inline(always)]
fn state_sealed(state: usize) -> bool {
    state & SEALED_BIT != 0
}

#[inline(always)]
fn state_flush_in_progress(state: usize) -> bool {
    state & FLUSH_IN_PROGRESS_BIT != 0
}

/// Errors a thread may encounter while managing the buffer.
#[derive(Debug, Clone, Copy)]
pub enum BufferError {
    /// The payload exceeds the remaining capacity of the flush buffer.
    InsufficientSpace,

    /// The buffer is sealed and no longer accepting writes.
    EncounteredSealedBuffer,

    /// CAS observed an already-sealed buffer.
    EncounteredSealedBufferDuringCOMPEX,

    /// CAS observed an already-unsealed buffer.
    EncounteredUnSealedBufferDuringCOMPEX,

    /// A flush was attempted while writers are still active.
    ActiveUsers,

    /// Undefined / corrupt state.
    InvalidState,

    /// Ring is exhausted — all buffers are sealed or being flushed.
    RingExhausted,

    FailedReservation,

    FailedUnsealed,
}

/// Successful operation outcomes.
#[derive(Debug, Clone, Copy)]
pub enum BufferMsg {
    /// Buffer has been sealed.
    SealedBuffer,

    /// Data written successfully.
    SuccessfullWrite,

    /// Data written and buffer flushed to stable storage.
    SuccessfullWriteFlush,

    /// Buffer is ready to flush; carries the ring position of the sealed buffer.
    FreeToFlush(usize),
}

/// A LLAMA I/O flush buffer.
///
/// # State word layout
///
/// ```text
/// ┌────────────────┬────────────────┬──────────────────┬───────────────────┬──────────┐
/// │  Bits 63..32   │  Bits 31..8    │  Bits 7..2       │  Bit 1            │  Bit 0   │
/// │  (offset)      │  (writers)     │  (reserved)      │  flush-in-prog    │  sealed  │
/// └────────────────┴────────────────┴──────────────────┴───────────────────┴──────────┘
/// ```
///
/// All four fields are read and written through a single [`AtomicUsize`], so any
/// snapshot of `state` is self-consistent: offset, writer count, flush flag, and
/// sealed flag are always observed together, eliminating TOCTOU races.
#[derive(Debug)]
pub struct FlushBuffer {
    /// Packed atomic state — see type-level docs.
    ///
    /// Contains (from LSB to MSB): sealed bit, flush-in-progress bit,
    /// reserved bits, writer count, and current write offset.
    state: AtomicUsize,

    /// Backing byte store.
    buf: SharedBuffer,

    /// Position of this buffer inside the [`FlushBufferRing`].
    pos: usize,
}

unsafe impl Send for FlushBuffer {}
unsafe impl Sync for FlushBuffer {}

impl FlushBuffer {
    pub fn new_buffer(buffer_number: usize, size: usize) -> FlushBuffer {
        Self {
            state: AtomicUsize::new(0),
            buf: Arc::new(Buffer {
                buffer: UnsafeCell::new(Box::new(vec![0u8; size]).into_boxed_slice()),
            }),

            pos: buffer_number,
        }
    }

    /// Returns `true` if this buffer is ready to accept new writers.
    ///
    /// Used by [`FlushBufferRing::rotate_after_seal`] to skip over sealed buffers.
    pub fn is_available(&self) -> bool {
        self.state.load(Ordering::Acquire) & (SEALED_BIT & FLUSH_IN_PROGRESS_BIT) == 0
    }

    /// Attempts to reserve `payload_size` bytes inside the buffer.
    ///
    /// On success returns the byte offset at which the caller should write
    /// while registering the caller as an active writer.
    /// 
    /// The called  **must** call [`decrement_writers`] once it has finished writing.
    ///
    /// # Errors
    ///
    /// - [`BufferError::EncounteredSealedBuffer`] — sealed or flush-in-progress
    ///   bits are set
    /// 
    /// - [`BufferError::InsufficientSpace`] — advancing the offset would exceed
    ///   [`FOUR_KB_PAGE`]; the caller is responsible for sealing and rotating.
    ///
    pub fn reserve_space(&self, payload_size: usize) -> Result<usize, BufferError> {
        assert!(payload_size <= FOUR_KB_PAGE, "payload larger than buffer");

        // We need to atomically:
        //   1. reject if sealed or flush-in-progress
        //   2. claim `payload_size` bytes from the offset
        //   3. register ourselves as an active writer
        //
        // A single fetch_add cannot do all three. The paper outlines a method use a CAS
        // on a state  that  packs offset + writer-count + flags into one word.

        let state = self.state.load(Ordering::Acquire);

        if state & (SEALED_BIT | FLUSH_IN_PROGRESS_BIT) != 0 {
            return Err(BufferError::EncounteredSealedBuffer);
        }

        let offset = state_offset(state);

        if offset + payload_size > FOUR_KB_PAGE {
            return Err(BufferError::InsufficientSpace);
        }

        // Advance offset by payload_size and bump the writer count by 1.
        let new = state
            .wrapping_add(payload_size * OFFSET_ONE) // advance offset
            .wrapping_add(WRITER_ONE); // register as writer

        match self
            .state
            .compare_exchange(state, new, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => return Ok(offset),
            Err(_) => return Err(BufferError::FailedReservation), // lost the race; retry with fresh snapshot
        }
    }

    /// Decrements the active-writer count by one.
    ///
    /// Returns the state word *before* the decrement so the caller can inspect
    /// whether the buffer was sealed and this was the last writer.
    #[inline]
    pub fn decrement_writers(&self) -> usize {
        self.state.fetch_sub(WRITER_ONE, Ordering::AcqRel)
    }

    /// Increments the active-writer count by one.
    ///
    /// This is a raw increment with no sealed-bit check; callers that need
    /// that gurantee shoudl go through [`reserve_space`] instead.
    #[inline]
    pub fn increment_writers(&self) -> usize {
        self.state.fetch_add(WRITER_ONE, Ordering::AcqRel)
    }

    /// Sets the flush-in-progress bit, signalling that a flush is underway.
    ///
    /// Returns the state word *before* the bit was set.
    #[inline]
    pub fn set_flush_in_progress(&self) -> usize {
        self.state.fetch_or(FLUSH_IN_PROGRESS_BIT, Ordering::AcqRel)
    }

    /// Clears the flush-in-progress bit once a flush has completed.
    ///
    /// Returns the state word *before* the bit was cleared.
    #[inline]
    pub fn clear_flush_in_progress(&self) -> usize {
        self.state
            .fetch_and(!FLUSH_IN_PROGRESS_BIT, Ordering::AcqRel)
    }

    /// Writes `payload` into the buffer at `offset` bytes from the start.
    ///
    /// The caller is responsible for obtaining a valid `offset` via
    /// [`reserve_space`] and for calling [`decrement_writers`] once done.
    ///
    /// # Safety
    /// `offset` must have been obtained from [`reserve_space`] on this buffer
    /// and the range `offset..offset + payload.len()` must lie within the
    /// backing allocation.
    pub(crate) fn write(&self, offset: usize, payload: &[u8]) {
        let payload_size = payload.len();
        debug_assert!(
            offset + payload_size <= FOUR_KB_PAGE,
            "write would exceed buffer bounds"
        );

        // SAFETY: `reserve_space` guaranteed this range is exclusively ours.
        let ptr = unsafe { (*self.buf.buffer.get()).as_mut_ptr().add(offset) };
        let slice = unsafe { std::slice::from_raw_parts_mut(ptr, payload_size) };
        slice.copy_from_slice(payload);
    }

    /// Explicitly sets the sealed bit, preventing any new writes.
    pub fn set_sealed_bit_true(&self) -> Result<(), BufferError> {
        let prev = self.state.fetch_or(SEALED_BIT, Ordering::AcqRel);
        if state_sealed(prev) {
            Err(BufferError::EncounteredSealedBufferDuringCOMPEX)
        } else {
            Ok(())
        }
    }

    /// Clears the sealed bit, re-opening the buffer for writes.
    ///
    /// # Errors
    ///
    /// - [`BufferError::ActiveUsers`] — writers or a flush are still active.
    /// - [`BufferError::EncounteredUnSealedBufferDuringCOMPEX`] — the buffer
    ///   was not sealed to begin with, or a concurrent CAS won the race.
    pub(crate) fn set_sealed_bit_false(&self) -> Result<(), BufferError> {
        let current = self.state.load(Ordering::Acquire);

        // Refuse to unseal while writers or a flush are active.
        if state_writers(current) != 0 || state_flush_in_progress(current) {
            return Err(BufferError::ActiveUsers);
        }

        if !state_sealed(current) {
            return Err(BufferError::EncounteredUnSealedBufferDuringCOMPEX);
        }

        match self.state.compare_exchange(
            current,
            current & !SEALED_BIT,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => return Ok(()),
            // Another thread raced us; re-evaluate from a fresh snapshot.
            Err(_) => return Err(BufferError::FailedUnsealed),
        }
    }



    /// For testing only !!!
    pub fn reset_offset(&self) {
        loop {
            let current = self.state.load(Ordering::Acquire);
            let zeroed = current & 0x0000_0000_FFFF_FFFF;
            if self
                .state
                .compare_exchange(current, zeroed, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                break;
            }
        }
    }
}

/// A ring of [`FlushBuffer`]s with an atomic pointer to the buffer currently
/// servicing write requests.
///
/// # Examples
///
/// ```
/// let ring = FlushBufferRing::with_buffer_amount(RING_SIZE);
/// let mut payload = make_payload("FILL", 4096);
/// 
/// let reserve_result = current.reserve_space(payload.len());
/// 
/// match ring.put(current, reserve_result, payload) {
///     Ok(BufferMsg::SuccessfullWrite) | Ok(BufferMsg::SuccessfullWriteFlush) => {}
///     other => panic!("unexpected: {other:?}"),
/// }
/// ```
pub struct FlushBufferRing {
    /// The single buffer currently servicing writes.
    ///
    /// Updated exclusively by the thread that won the seal race, via
    /// [`rotate_after_seal`].
    pub current_buffer: AtomicPtr<FlushBuffer>,

    /// Pinned ring — buffer addresses are stable for the lifetime of the ring.
    ring: Pin<Box<[Arc<FlushBuffer>]>>,

    /// Monotonically incrementing counter used to derive the next buffer index.
    next_index: AtomicUsize,

    /// Total number of buffers in the ring.
    _size: usize,
}

impl FlushBufferRing {
    pub fn with_buffer_amount(num_of_buffer: usize, buffer_size: usize) -> FlushBufferRing {
        let buffers: Vec<Arc<FlushBuffer>> = (0..num_of_buffer)
            .map(|i| Arc::new(FlushBuffer::new_buffer(i, buffer_size)))
            .collect();

        let buffers = Pin::new(buffers.into_boxed_slice());
        let current = &*buffers[0] as *const FlushBuffer as *mut FlushBuffer; // Magic Rust Bullshit

        FlushBufferRing {
            current_buffer: AtomicPtr::new(current),
            ring: buffers,
            next_index: AtomicUsize::new(1),
            _size: num_of_buffer,
        }
    }

    /// Writes `payload` into the current buffer, rotating and retrying as needed.
    ///
    /// # Correctness
    ///
    /// - [`reserve_space`] atomically claims the byte range *and* registers the
    ///   caller as an active writer in a single CAS, so offset accounting and
    ///   writer bookkeeping are always consistent.
    /// - Only the thread that wins the seal CAS calls [`rotate_after_seal`].
    ///   All other threads simply retry; they never attempt rotation.
    /// - [`write`] is called after a successful reservation; it does not touch
    ///   the state word.
    /// - [`decrement_writers`] is the final step; the returned snapshot lets us
    ///   detect the last-writer-after-seal condition for flush triggering.
    pub async fn put(
        &self,
        current: &FlushBuffer,
        reserve_result: Result<usize, BufferError>,
        payload: &[u8],
    ) -> Result<BufferMsg, BufferError> {
        match reserve_result {
            Err(BufferError::InsufficientSpace) => {
                // fetch_or returns the state *before* the OR.
                // Exactly one thread will observe the previous sealed bit as 0;
                // that thread is the designated sealer and the only one allowed to rotate.
                let prev = current.state.fetch_or(SEALED_BIT, Ordering::AcqRel);

                if prev & SEALED_BIT != 0 {
                    // Another thread sealed first; just retry against the new buffer.
                    return Err(BufferError::EncounteredSealedBuffer);
                }

                // We are the unique sealer. Rotate immediately so that
                // seal + rotate appear atomic to all observers.
                self.rotate_after_seal(current.pos)?;

                // Check whether any writers were still active at seal time.
                // If not, we are responsible for triggering the flush.
                let writers_at_seal = state_writers(prev);

                if writers_at_seal == 0 {
                    // No concurrent writers; safe to flush now.
                    let flush_buffer = self.ring.get(current.pos).unwrap().clone();
                    let _ = FlushBufferRing::flush(&flush_buffer).await;
                    return Ok(BufferMsg::SuccessfullWriteFlush);
                }

                // Writers were active at seal time; one of them will observe
                // the sealed bit on their decrement_writers path and trigger the flush.
                return Err(BufferError::ActiveUsers);
            }

            Err(BufferError::EncounteredSealedBuffer) => {
                // Buffer is sealed or flushing; spin until the ring rotates.
                return Err(BufferError::EncounteredSealedBuffer);
            }

            Err(e) => return Err(e),

            Ok(offset) => {
                // Reservation succeeded — write the payload at the claimed offset.
                current.write(offset, payload);

                // Decrement writer count and capture the pre-decrement snapshot.
                let prev = current.decrement_writers();

                let was_last_writer = state_writers(prev) == 1;
                let was_sealed = state_sealed(prev);

                if was_last_writer && was_sealed {
                    // We were the final writer in a sealed buffer; flush now.
                    let flush_buffer = self.ring.get(current.pos).unwrap().clone();
                    let _ = FlushBufferRing::flush(&flush_buffer).await;
                    return Ok(BufferMsg::SuccessfullWriteFlush);
                }

                return Ok(BufferMsg::SuccessfullWrite);
            }
        }
    }

    /// Rotates `current_buffer` to the next slot in the ring.
    ///
    /// **Must only be called by the thread that won the seal race.**
    ///
    /// The `sealed_pos` guard means the call is idempotent: if another thread
    /// (or a retry) already advanced `current_buffer` past `sealed_pos`, we
    /// return immediately without touching anything.
    pub(crate) fn rotate_after_seal(&self, sealed_pos: usize) -> Result<(), BufferError> {
        let current = self.current_buffer.load(Ordering::Acquire);
        let current_ref = unsafe { current.as_ref().ok_or(BufferError::InvalidState)? };

        // Guard: if current_buffer no longer points at the buffer we sealed,
        // someone else already rotated — nothing to do.
        if current_ref.pos != sealed_pos {
            return Ok(());
        }

        // Scan forward through the ring for the next slot that is actually
        // available (neither sealed nor flush-in-progress).
        let ring_len = self.ring.len();

        for _ in 0..ring_len {
            let raw = self.next_index.fetch_add(1, Ordering::AcqRel);

            let next_index = raw % ring_len;
            let new_buffer = &self.ring[next_index];

            if new_buffer.is_available() {
                // CAS ensures only one thread installs the new pointer even if
                // rotate_after_seal is somehow called concurrently.
                let _ = self.current_buffer.compare_exchange(
                    current,
                    Arc::as_ptr(new_buffer) as *const FlushBuffer as *mut FlushBuffer,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                );
                return Ok(());
            }
            // This slot is busy; try the next one.
        }

        // Every buffer in the ring is currently sealed or flushing.
        // This should not occur with a correctly sized ring but we return
        // an error rather than deadlock.
        //
        // We may also continually spin untill a buffer has been flushed successfully
        Err(BufferError::RingExhausted)
    }

    /// Unimplemented Asynchrosnous i/o using io/uring
    ///
    /// On completion the buffer's flush-in-progress and sealed bits are cleared
    /// and its offset is reset to zero, making it available for reuse.
    pub(crate) async fn flush(buffer: &FlushBuffer) -> Result<(), BufferError> {
        // TODO: implement actual LSS write

        // Mark flush as in-progress so no new writers can enter.
        buffer.set_flush_in_progress();

        // … LSS write would happen here …

        // We loop on a CAS rather than a naked fetch_and so that any writer-count
        // or flag changes that race with us are not silently overwritten.

        loop {
            let current = buffer.state.load(Ordering::Acquire);

            // Reset the offset and clear both the sealed and flush-in-progress bits
            // in a single atomic AND, making the buffer available for reuse.

            let reset =
                current & !(SEALED_BIT | FLUSH_IN_PROGRESS_BIT | (usize::MAX << OFFSET_SHIFT));
            if buffer
                .state
                .compare_exchange(current, reset, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                break;
            }
        }

        Ok(())
    }
}

// ==========================================================================+==
//  Tests
// ─============================================================================
#[cfg(test)]
mod tests {
    use crate::data_objects::FOUR_KB_PAGE;

    use super::*;

    use std::{
        collections::HashSet,
        sync::{Arc, Barrier, Mutex},
        thread,
        time::Instant,
    };

    /// Very small, very lightweight, very unimpressive Linear Congruential Generator for deterministic pseudorandom number generation in tests.
    ///
    /// # Linear Congruential Generator
    ///
    /// The method yields a sequence of pseudo-randomized numbers and represents
    /// one of the oldest and best-known pseudorandom number generator algorithms.
    ///
    /// source: https://en.wikipedia.org/wiki/Linear_congruential_generator
    struct Lcg {
        state: u64,
    }
    impl Lcg {
        fn new(seed: u64) -> Self {
            Self { state: seed }
        }
        fn next_usize(&mut self, bound: usize) -> usize {
            self.state = self
                .state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            ((self.state >> 33) as usize) % bound
        }
    }

    const RING_SIZE: usize = 4;
    const OPS_PER_THREAD: usize = 2_000;

    /// Payload sizes ranging from tiny, medium, and near-capacity.
    const SIZES: &[usize] = &[
        1, 2, 4, 7, 8, 15, 16, 32, 64, 100, 128, 200, 256, 512, 1024, 2048, 4090, 4095, 4096,
    ];

    /// Build a recognisable, size-stamped payload.
    fn make_payload(tag: &str, size: usize) -> Vec<u8> {
        let meta = format!("[{tag}:{size}]");
        let mut buf = vec![0xAA_u8; size];
        let n = meta.len().min(size);
        buf[..n].copy_from_slice(&meta.as_bytes()[..n]);
        buf
    }

    // =========================================================================
    // Helper: caller-owned retry loop
    //
    // Since reserve_space and put no longer retry internally, every call site
    // that wants "eventually succeeds" semantics must drive the loop itself.
    // This helper encapsulates the canonical retry pattern so tests stay
    // readable without duplicating the loop logic everywhere.
    //
    // The loop:
    //   1. Load the current buffer from the ring.
    //   2. Call reserve_space.
    //   3. Pass the result (Ok or Err) straight into put — put decides whether
    //      to seal/rotate (InsufficientSpace) or bubble the error up.
    //   4. Retry on FailedReservation (lost CAS) or EncounteredSealedBuffer
    //      (ring is rotating), which are both transient conditions.
    //   5. Any other outcome (success or a hard error) breaks the loop.
    // =========================================================================
    async fn put_with_retry(
        ring: &FlushBufferRing,
        payload: &[u8],
    ) -> Result<BufferMsg, BufferError> {
        loop {
            // Safety: current_buffer is always initialised by with_buffer_amount
            // and only ever points at live ring slots.
            let current = unsafe {
                ring.current_buffer
                    .load(Ordering::Acquire)
                    .as_ref()
                    .ok_or(BufferError::InvalidState)?
            };

            let reserve_result = current.reserve_space(payload.len());

            match &reserve_result {
                // Transient: lost the CAS race — re-read current and retry.
                Err(BufferError::FailedReservation) => continue,
                // Transient: buffer sealed or flushing — spin until ring rotates.
                Err(BufferError::EncounteredSealedBuffer) => continue,
                // Everything else (Ok, InsufficientSpace, hard errors) falls
                // through to put, which handles sealing, rotating, and writing.
                _ => {}
            }

            match ring.put(current, reserve_result, payload).await {
                // put signals that the sealer found active writers; the last of
                // those writers will trigger the flush, so we just need a new buffer.
                Err(BufferError::ActiveUsers) => continue,
                // Another thread sealed before us; retry against the new buffer.
                Err(BufferError::EncounteredSealedBuffer) => continue,
                // Any other result (success or hard error) is final.
                other => return other,
            }
        }
    }

    // =========================================================================
    // Single-threaded tests
    // =========================================================================

    /// reserve_space on an already-sealed buffer must return EncounteredSealedBuffer.
    #[tokio::test]
    async fn reserve_on_sealed_buffer_returns_error() {
        let buf = FlushBuffer::new_buffer(0, FOUR_KB_PAGE);
        buf.set_sealed_bit_true().unwrap();
        assert!(matches!(
            buf.reserve_space(16),
            Err(BufferError::EncounteredSealedBuffer)
        ));
    }

    /// Sealing an already-sealed buffer must return the COMPEX error.
    #[tokio::test]
    async fn double_seal_returns_error() {
        let buf = FlushBuffer::new_buffer(0, FOUR_KB_PAGE);
        buf.set_sealed_bit_true().unwrap();
        assert!(matches!(
            buf.set_sealed_bit_true(),
            Err(BufferError::EncounteredSealedBufferDuringCOMPEX)
        ));
    }

    /// Unsealing an already-unsealed buffer must return the COMPEX error.
    #[tokio::test]
    async fn unseal_unsealed_returns_error() {
        let buf = FlushBuffer::new_buffer(0, FOUR_KB_PAGE);
        assert!(matches!(
            buf.set_sealed_bit_false(),
            Err(BufferError::EncounteredUnSealedBufferDuringCOMPEX)
        ));
    }

    /// Two concurrent threads race to reserve disjoint regions. Uses a shared
    /// [`HashSet`] to assert no (buf_pos, offset) pair is ever issued twice.
    ///
    /// NOTE: This test interacts with [`FlushBuffer::reserve_space`] directly
    /// rather than going through the ring so we can capture the offsets.
    #[tokio::test]
    async fn concurrent_reserve_space_no_overlap() {
        let buf = Arc::new(FlushBuffer::new_buffer(99, FOUR_KB_PAGE));
        let seen: Arc<Mutex<HashSet<(usize, usize)>>> = Arc::new(Mutex::new(HashSet::new()));

        const THREADS: usize = 8;
        const RESERVES_PER_THREAD: usize = 32; // 32 × 8 × 16 = 4096 — exactly fills one buffer

        let barrier = Arc::new(Barrier::new(THREADS));
        let handles: Vec<_> = (0..THREADS)
            .map(|_tid| {
                let buf = Arc::clone(&buf);
                let seen = Arc::clone(&seen);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    for _ in 0..RESERVES_PER_THREAD {
                        // Each thread drives its own retry loop for reserve_space
                        // since a lost CAS (FailedReservation) is transient.
                        loop {
                            match buf.reserve_space(16) {
                                Ok(offset) => {
                                    let mut lock = seen.lock().unwrap();
                                    assert!(
                                        lock.insert((99, offset)),
                                        "[OVERLAP] offset {offset} in buffer 99 was issued twice!"
                                    );
                                    break;
                                }
                                // Transient CAS loss — retry immediately.
                                Err(BufferError::FailedReservation) => continue,
                                // Buffer exhausted — acceptable terminal condition.
                                Err(BufferError::InsufficientSpace) => break,
                                Err(BufferError::EncounteredSealedBuffer) => break,
                                Err(e) => panic!("reserve_space error: {e:?}"),
                            }
                        }
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().expect("reserve worker panicked");
        }
    }

    /// Writes of random sizes across a full ring, single-threaded.
    #[tokio::test]
    async fn single_threaded_offset_uniqueness() {
        let ring = FlushBufferRing::with_buffer_amount(RING_SIZE, FOUR_KB_PAGE);

        let mut rng = Lcg::new(0);
        let mut writes = 0usize;
        let mut flushes = 0usize;
        let mut data_written = 0usize;
        let mut i = 0usize;

        loop {
            let size = SIZES[rng.next_usize(SIZES.len())];

            if data_written + size > FOUR_KB_PAGE * RING_SIZE {
                break;
            }

            let payload = make_payload(&format!("s{i:05}"), size);
            data_written += size;

            match put_with_retry(&ring, &payload).await {
                Ok(BufferMsg::SuccessfullWrite) => writes += 1,
                Ok(BufferMsg::SuccessfullWriteFlush) => {
                    writes += 1;
                    flushes += 1;
                }
                other => panic!("single_threaded: unexpected {other:?}"),
            }
            i += 1;
        }

        println!(
            "single_threaded: {writes} writes, {flushes} logical flushes — OK, data_written: {data_written} bytes"
        );
    }

    /// A single FOUR_KB_PAGE payload must be accepted exactly once.
    #[tokio::test]
    async fn exact_fill() {
        let ring = FlushBufferRing::with_buffer_amount(RING_SIZE, FOUR_KB_PAGE);
        let payload = make_payload("FILL", FOUR_KB_PAGE);
        match put_with_retry(&ring, &payload).await {
            Ok(BufferMsg::SuccessfullWrite) | Ok(BufferMsg::SuccessfullWriteFlush) => {}
            other => panic!("exact_fill: unexpected {other:?}"),
        }
    }

    /// Stress test: many random-sized writes, single thread.
    #[tokio::test]
    async fn single_threaded_stress() {
        let ring = FlushBufferRing::with_buffer_amount(RING_SIZE, FOUR_KB_PAGE);
        let mut writes = 0usize;
        let mut flushes = 0usize;
        let mut rng = Lcg::new(0x1234_5678);
        let start = Instant::now();

        for op in 0..OPS_PER_THREAD {
            let size = SIZES[rng.next_usize(SIZES.len())];
            let payload = make_payload(&format!("S:O{op:04}"), size);

            match put_with_retry(&ring, &payload).await {
                Ok(BufferMsg::SuccessfullWrite) => writes += 1,
                Ok(BufferMsg::SuccessfullWriteFlush) => {
                    writes += 1;
                    flushes += 1;
                }
                other => panic!("op {op}: unexpected {other:?}"),
            }
        }

        let elapsed = start.elapsed();
        println!(
            "single_threaded_stress: {writes} writes, {flushes} logical flushes in {elapsed:.2?} ({:.0} ops/s)",
            (writes + flushes) as f64 / elapsed.as_secs_f64()
        );
    }

    // =========================================================================
    // Multi-threaded stress tests
    // =========================================================================

    const NUM_THREADS_SMALL: usize = 2;
    const NUM_THREADS_MEDIUM: usize = 4;
    const NUM_THREADS_LARGE: usize = 8;

    #[tokio::test]
    async fn multi_threaded_test_small() {
        multi_threaded_stress_helper(NUM_THREADS_SMALL).await;
    }

    #[tokio::test]
    async fn multi_threaded_test_medium() {
        multi_threaded_stress_helper(NUM_THREADS_MEDIUM).await;
    }

    #[tokio::test]
    async fn multi_threaded_test_large() {
        multi_threaded_stress_helper(NUM_THREADS_LARGE).await;
    }

    async fn multi_threaded_stress_helper(num_threads: usize) {
        let ring = Arc::new(FlushBufferRing::with_buffer_amount(RING_SIZE, FOUR_KB_PAGE));

        let barrier = Arc::new(Barrier::new(num_threads));

        let total_writes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let total_flushes = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let handles: Vec<thread::JoinHandle<()>> = (0..num_threads)
            .map(|tid| {
                let ring = Arc::clone(&ring);
                let barrier = Arc::clone(&barrier);
                let total_writes = Arc::clone(&total_writes);
                let total_flushes = Arc::clone(&total_flushes);

                let seed = 0x1234_5678_u64
                    .wrapping_add(tid as u64)
                    .wrapping_mul(0xDEAD_CAFE);

                thread::spawn(move || {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("failed to build runtime");

                    rt.block_on(async move {
                        let mut rng = Lcg::new(seed);
                        let mut local_writes = 0usize;
                        let mut local_flushes = 0usize;

                        barrier.wait();

                        for op in 0..OPS_PER_THREAD {
                            let size = SIZES[rng.next_usize(SIZES.len())];
                            let payload = make_payload(&format!("T{tid}:O{op:04}"), size);

                            // Inline retry loop with per-event watchdog increments.
                            // No locks — every counter update is a standalone fetch_add.
                            let result = loop {
                                let current = unsafe {
                                    ring.current_buffer
                                        .load(Ordering::Acquire)
                                        .as_ref()
                                        .expect("null current_buffer")
                                };

                                let reserve_result = current.reserve_space(payload.len());

                                match &reserve_result {
                                    Ok(_) => {}
                                    Err(BufferError::FailedReservation) => {
                                        continue;
                                    }
                                    Err(BufferError::EncounteredSealedBuffer) => {
                                        continue;
                                    }
                                    Err(BufferError::InsufficientSpace) => {}
                                    Err(_) => {}
                                }

                                match ring.put(current, reserve_result, &payload).await {
                                    Err(BufferError::ActiveUsers) => {
                                        // Seal was won by us but writers still active;
                                        // check whether we also won the seal.

                                        continue;
                                    }
                                    Err(BufferError::EncounteredSealedBuffer) => {
                                        continue;
                                    }
                                    Err(BufferError::RingExhausted) => {
                                        // Surface instead of spinning forever.
                                        break Err(BufferError::RingExhausted);
                                    }
                                    Ok(BufferMsg::SuccessfullWriteFlush) => {
                                        break Ok(BufferMsg::SuccessfullWriteFlush);
                                    }
                                    Ok(BufferMsg::SuccessfullWrite) => {
                                        break Ok(BufferMsg::SuccessfullWrite);
                                    }
                                    Ok(BufferMsg::SealedBuffer) => {
                                        continue;
                                    }
                                    other => break other,
                                }
                            };

                            match result {
                                Ok(BufferMsg::SuccessfullWrite) => local_writes += 1,
                                Ok(BufferMsg::SuccessfullWriteFlush) => {
                                    local_writes += 1;
                                    local_flushes += 1;
                                }
                                other => panic!("thread {tid} op {op}: unexpected {other:?}"),
                            }
                        }

                        total_writes.fetch_add(local_writes, Ordering::Relaxed);
                        total_flushes.fetch_add(local_flushes, Ordering::Relaxed);
                    });
                })
            })
            .collect();

        for (tid, handle) in handles.into_iter().enumerate() {
            handle
                .join()
                .unwrap_or_else(|_| panic!("worker thread {tid} panicked"));
        }

        let writes = total_writes.load(Ordering::Relaxed);
        let flushes = total_flushes.load(Ordering::Relaxed);
        let ops = writes + flushes;
    }

    /// Barrier-synchronised: all threads race with large payloads to maximise
    /// the seal/rotate contention window.
    #[tokio::test]
    async fn hammer_seal_concurrent_rotation() {
        let ring = Arc::new(FlushBufferRing::with_buffer_amount(RING_SIZE, FOUR_KB_PAGE));
        let barrier = Arc::new(Barrier::new(NUM_THREADS_SMALL));

        let handles: Vec<_> = (0..NUM_THREADS_SMALL)
            .map(|tid| {
                let ring = Arc::clone(&ring);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .unwrap();

                    rt.block_on(async move {
                        barrier.wait();
                        for iter in 0..100_usize {
                            let payload = make_payload(&format!("H{tid}:{iter}"), 2048);
                            match put_with_retry(&ring, &payload).await {
                                Ok(_) => {}
                                Err(e) => panic!("hammer thread {tid} iter {iter}: error {e:?}"),
                            }
                        }
                    });
                })
            })
            .collect();

        for h in handles {
            h.join().expect("hammer worker panicked");
        }
    }
}
