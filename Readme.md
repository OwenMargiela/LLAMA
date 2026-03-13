

# Bloom_lfs

A high-performance, latch-free log-structured storage layer built for modern flash storage and multi-core systems.

## Overview

Bloom_lfs implements the storage foundation of LLAMA (Latch-free, Log-structured, Access-Method Aware) — a concurrent caching and storage subsystem designed to exploit the performance characteristics of append-only writes on flash media while avoiding the costs of random I/O and excessive write amplification.

The project currently implements the **log-structured secondary storage** component, which provides:

- **High write throughput** through batched, sequential writes
- **Latch-free concurrency** via atomic operations on packed state words
- **Low write amplification** with tail-localized append patterns
- **Direct I/O (`O_DIRECT`)** to bypass kernel page cache overhead
- **Asynchronous I/O** via `io_uring` for efficient kernel interactions

## Architecture

### Core Components

```
┌─────────────────────────────────────────────────────────────┐
│                    FlushBufferRing                          │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐     │
│  │ Buffer 0 │  │ Buffer 1 │  │ Buffer 2 │  │ Buffer 3 │     │
│  │  4 KiB   │  │  4 KiB   │  │  4 KiB   │  │  4 KiB   │     │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘     │
│      ▲                                                      │
│      └─── current_buffer (atomic pointer)                   │
└─────────────────────────────────────────────────────────────┘
                          │
                          ▼
              ┌───────────────────────┐
              │   FlushBehavior       │
              │  ┌─────────────────┐  │
              │  │ io_uring        │  │
              │  │ (async I/O)     │  │
              │  └─────────────────┘  │
              └───────────────────────┘
                          │
                          ▼
              ┌───────────────────────┐
              │ LogStructuredStore    │
              │  (O_DIRECT backing    │
              │   file on disk)       │
              └───────────────────────┘
```

### 1. **FlushBuffer & FlushBufferRing** (`flush_buffer.rs`)

A fixed-size ring of 4 KiB-aligned buffers that amortizes individual writes into batched sequential I/O operations.

**Key Features:**
- **Latch-free writes**: Single packed `AtomicUsize` state word per buffer eliminates locks
- **Concurrent filling**: Multiple threads write to one buffer simultaneously via atomic fetch-add
- **Automatic rotation**: Buffers seal when full and trigger asynchronous flush
- **`O_DIRECT` compatibility**: All allocations aligned to 4 KiB page boundaries

**State Word Layout:**
```
┌────────────────┬────────────────┬──────────────┬─────────────┬─────────┐
│  Bits 63..32   │  Bits 31..8    │  Bits 7..2   │  Bit 1      │  Bit 0  │
│  write offset  │  writer count  │  (reserved)  │  flushing   │  sealed │
└────────────────┴────────────────┴──────────────┴─────────────┴─────────┘
```

### 2. **LogStructuredStore** (`log_structured_store.rs`)

The durable backing file for page state. All writes flow through the `FlushBufferRing` and are dispatched via `FlushBehavior`.

**Key Features:**
- **Stability tracking**: `hi_stable` pointer tracks highest contiguous durable slot
- **Out-of-order handling**: `completed_islands` set manages non-contiguous completions
- **Crash recovery**: Can rebuild consistent state from `hi_stable` watermark
- **Write amplification**: Near-zero — buffers are written once at their assigned slots

**Address Space:**
```
byte_offset = local_lss_address_slot × FOUR_KB_PAGE
```

Slots are claimed atomically at buffer seal time, guaranteeing no two buffers ever map to the same file region.

### 3. **FlushBehavior** (`flush_behaviour.rs`)

Handles `io_uring`-backed write dispatch with two operating modes:

| Mode                      | Description                                    | Use Case                    |
|---------------------------|------------------------------------------------|-----------------------------|
| **TailLocalizedWrites**   | Parallel writes within bounded tail region     | High-throughput ingestion   |
| **SerializedWrites**      | Strict append order (`IO_LINK` flag)           | WAL segments, recovery logs |

**Tail-Localized Writes:**
- Buffers write concurrently; flushes may complete out-of-order
- Maximum write distance from tail: `RING_SIZE × FOUR_KB_PAGE`
- Suitable for page-state storage where absolute ordering is not critical

**Serialized Writes:**
- Each write waits for the previous to complete (`IO_LINK`)
- Enforces submission-order on disk
- Reduced parallelism but guaranteed ordering

## Write Path

```
  Caller
    │
    │  write_payload(&[u8], Reservation)
    ▼
  reserve_space(payload_size)      ← atomic fetch-add on state word
    │
    ▼
  FlushBufferRing::put()            ← copy payload into aligned buffer
    │
    ├─ buffer not full ───────────────────► Ok(SuccessfulWrite)
    │
    └─ buffer full (sealed)
         │
         ├─ rotate ring to next available buffer
         │
         └─ FlushBehavior::submit_buffer()
                │
                ├─ NoWaitAppender  (TailLocalizedWrites)
                │    └─ io_uring write_at(offset)           ← unordered
                │
                └─ WaitAppender   (SerializedWrites)
                     └─ io_uring write_at(offset, IO_LINK)  ← strict order
```

## Design Rationale

### Why Log-Structured?

**Flash-Optimized:**
- Sequential writes exploit flash erase-block characteristics
- Reduces write amplification (each page written once)
- Enables wear leveling across the device

**Concurrency-Friendly:**
- No in-place updates → no contention on individual slots
- Latch-free buffer management via atomic operations
- Caller threads participate in flush without blocking

**Recovery-Efficient:**
- `hi_stable` watermark provides instant crash-consistent snapshot
- No need to scan entire file during recovery
- Out-of-order completions handled transparently via island set

### Why `O_DIRECT`?

- **Bypasses kernel page cache** — DMA directly to device
- **Predictable latency** — no cache eviction surprises
- **Lower memory pressure** — userspace controls all buffering

**Tradeoff:** Requires strict alignment (4 KiB) for all buffers.

### Why `io_uring`?

- **Asynchronous I/O** — submit returns immediately, no blocking
- **Batched operations** — submit multiple SQEs in one syscall
- **Zero-copy** — kernel reads directly from userspace buffers
- **Polled completions** — no context switch per completion


## Usage Example

```rust
use bloom_lfs::log_structured_store::{LogStructuredStore, WriteMode};

// Open store with tail-localized writes (high throughput)
let store = LogStructuredStore::open_with_behavior(
    "data.lss",
    WriteMode::TailLocalizedWrites,
)?;

// Reserve space, write payload, poll completions
let reservation = store.reserve_space(64)?;
store.write_payload(b"hello, LLAMA", reservation)?;
store.check_async_cque(); // Process completed writes

// For WAL or recovery log, use serialized mode
let wal = LogStructuredStore::open_with_behavior(
    "wal.lss",
    WriteMode::SerializedWrites,
)?;
```

## Performance Characteristics

| Metric                    | Value                                          |
|---------------------------|------------------------------------------------|
| Buffer size               | 4 KiB (aligned to page boundary)               |
| Ring size                 | 4 buffers (default, configurable)              |
| Max tail write distance   | `RING_SIZE × 4 KiB` = 16 KiB (tail-localized)  |
| Concurrency model         | Latch-free (atomic operations only)            |
| Write amplification       | ~1.0 (each page written once)                  |
| I/O pattern               | Sequential append (flash-optimized)            |

## Requirements

- **Linux kernel ≥ 5.1** (for `io_uring` support)
- **Rust ≥ 1.70** (for atomic operations and `UnsafeCell` patterns)

## Dependencies

```toml
[dependencies]
io-uring = "0.6"
parking_lot = "0.12"
libc = "0.2"
```

## Safety

All `unsafe` code is audited and justified:

- **`FlushBuffer::write`**: Raw pointer dereference into aligned allocation
  - **Invariant**: `reserve_space` grants exclusive, non-overlapping ranges
- **`FlushBufferRing::current_buffer`**: Raw atomic pointer load
  - **Invariant**: Ring guarantees pointer validity for store lifetime
- **`io_uring` SQE construction**: Pointers stored in user_data
  - **Invariant**: Buffers pinned in memory, flush-in-progress bit prevents reuse

## Future Work

- [ ] Add caching layer (LRU eviction policy)
- [ ] Recovery protocol (replay log from `hi_stable`)
- [ ] Remote storage support (networked backing file)

## References

- **LLAMA Paper**: Lev-Ari et al., "LLAMA: A Cache/Storage Subsystem for Modern Hardware" (VLDB 2013)

