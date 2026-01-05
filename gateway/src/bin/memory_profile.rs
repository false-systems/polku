//! Memory profiling binary using dhat
//!
//! Run with: cargo run --release --bin memory_profile --features dhat-heap
//!
//! This measures:
//! - Bytes allocated per message
//! - Buffer memory overhead
//! - Peak memory under load
//! - Allocation counts per operation

#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

use bytes::Bytes;
use polku_gateway::buffer_lockfree::{LockFreeBuffer, SharedBuffer};
use polku_gateway::buffer_tiered::TieredBuffer;
use polku_gateway::message::Message;
use polku_gateway::shared_message::SharedMessage;
use std::hint::black_box;

fn main() {
    #[cfg(not(feature = "dhat-heap"))]
    {
        eprintln!("Run with: cargo run --release --bin memory_profile --features dhat-heap");
        std::process::exit(1);
    }

    #[cfg(feature = "dhat-heap")]
    {
        let _profiler = dhat::Profiler::new_heap();
        run_profiles();
    }
}

#[cfg(feature = "dhat-heap")]
fn run_profiles() {
    println!("=== POLKU Memory Profiling ===\n");

    profile_message_creation();
    profile_message_clone();
    profile_shared_message();
    profile_lockfree_buffer();
    profile_shared_buffer();
    profile_tiered_buffer();
    profile_pipeline_simulation();

    println!("\n=== dhat summary will print on exit ===");
}

#[cfg(feature = "dhat-heap")]
fn profile_message_creation() {
    println!("--- Message Creation (1000 messages) ---");

    let stats_before = dhat::HeapStats::get();

    let messages: Vec<Message> = (0..1000)
        .map(|i| {
            Message::new(
                "test-source",
                format!("event-type-{}", i),
                Bytes::from(vec![0u8; 100]), // 100 byte payload
            )
        })
        .collect();

    black_box(&messages);
    let stats_after = dhat::HeapStats::get();

    let bytes_per_msg =
        (stats_after.curr_bytes - stats_before.curr_bytes) as f64 / 1000.0;
    let allocs_per_msg =
        (stats_after.curr_blocks - stats_before.curr_blocks) as f64 / 1000.0;

    println!("  Bytes per message: {:.1}", bytes_per_msg);
    println!("  Allocations per message: {:.1}", allocs_per_msg);
    println!(
        "  Total for 1000 msgs: {} bytes",
        stats_after.curr_bytes - stats_before.curr_bytes
    );
    println!();

    drop(messages);
}

#[cfg(feature = "dhat-heap")]
fn profile_message_clone() {
    println!("--- Message Clone (1000 clones) ---");

    let original = Message::new(
        "test-source",
        "test-event",
        Bytes::from(vec![0u8; 1000]), // 1KB payload
    );

    let stats_before = dhat::HeapStats::get();

    let clones: Vec<Message> = (0..1000).map(|_| original.clone()).collect();

    black_box(&clones);
    let stats_after = dhat::HeapStats::get();

    let bytes_per_clone =
        (stats_after.curr_bytes - stats_before.curr_bytes) as f64 / 1000.0;

    println!("  Original payload: 1000 bytes");
    println!("  Bytes per clone: {:.1}", bytes_per_clone);
    println!("  (Bytes clone is zero-copy, strings are cloned)");
    println!();

    drop(clones);
    drop(original);
}

#[cfg(feature = "dhat-heap")]
fn profile_shared_message() {
    println!("--- SharedMessage (Arc) Clone (1000 clones) ---");

    let original = Message::new(
        "test-source",
        "test-event",
        Bytes::from(vec![0u8; 1000]), // 1KB payload
    );
    let shared = SharedMessage::new(original);

    let stats_before = dhat::HeapStats::get();

    let clones: Vec<SharedMessage> = (0..1000).map(|_| shared.clone()).collect();

    black_box(&clones);
    let stats_after = dhat::HeapStats::get();

    let bytes_per_clone =
        (stats_after.curr_bytes - stats_before.curr_bytes) as f64 / 1000.0;

    println!("  Original payload: 1000 bytes");
    println!("  Bytes per SharedMessage clone: {:.1}", bytes_per_clone);
    println!("  (Should be ~0 - just Arc refcount increment)");
    println!();

    drop(clones);
    drop(shared);
}

#[cfg(feature = "dhat-heap")]
fn profile_lockfree_buffer() {
    println!("--- LockFreeBuffer (capacity 10,000) ---");

    let stats_before = dhat::HeapStats::get();

    let buffer = LockFreeBuffer::new(10_000);

    let stats_after_create = dhat::HeapStats::get();

    // Fill buffer
    for i in 0..10_000 {
        let msg = Message::new("src", format!("evt-{}", i), Bytes::from(vec![0u8; 100]));
        buffer.push(msg);
    }

    let stats_after_fill = dhat::HeapStats::get();

    println!(
        "  Buffer creation: {} bytes",
        stats_after_create.curr_bytes - stats_before.curr_bytes
    );
    println!(
        "  After filling 10K msgs: {} bytes",
        stats_after_fill.curr_bytes - stats_before.curr_bytes
    );
    println!(
        "  Bytes per buffered message: {:.1}",
        (stats_after_fill.curr_bytes - stats_after_create.curr_bytes) as f64 / 10_000.0
    );
    println!();

    drop(buffer);
}

#[cfg(feature = "dhat-heap")]
fn profile_shared_buffer() {
    println!("--- SharedBuffer (capacity 10,000) ---");

    let stats_before = dhat::HeapStats::get();

    let buffer = SharedBuffer::new(10_000);

    let stats_after_create = dhat::HeapStats::get();

    // Fill buffer
    for i in 0..10_000 {
        let msg = Message::new("src", format!("evt-{}", i), Bytes::from(vec![0u8; 100]));
        buffer.push(msg);
    }

    let stats_after_fill = dhat::HeapStats::get();

    println!(
        "  Buffer creation: {} bytes",
        stats_after_create.curr_bytes - stats_before.curr_bytes
    );
    println!(
        "  After filling 10K msgs: {} bytes",
        stats_after_fill.curr_bytes - stats_before.curr_bytes
    );
    println!(
        "  Bytes per buffered message: {:.1}",
        (stats_after_fill.curr_bytes - stats_after_create.curr_bytes) as f64 / 10_000.0
    );
    println!();

    drop(buffer);
}

#[cfg(feature = "dhat-heap")]
fn profile_tiered_buffer() {
    println!("--- TieredBuffer (1K primary, 10K secondary) ---");

    let stats_before = dhat::HeapStats::get();

    let buffer = TieredBuffer::new(1_000, 10_000, 100);

    let stats_after_create = dhat::HeapStats::get();

    // Fill beyond primary to trigger compression
    for i in 0..5_000 {
        let msg = Message::new("src", format!("evt-{}", i), Bytes::from(vec![0u8; 100]));
        buffer.push(msg);
    }

    let stats_after_fill = dhat::HeapStats::get();

    println!(
        "  Buffer creation: {} bytes",
        stats_after_create.curr_bytes - stats_before.curr_bytes
    );
    println!(
        "  After 5K msgs (1K hot + 4K compressed): {} bytes",
        stats_after_fill.curr_bytes - stats_before.curr_bytes
    );
    println!("  Primary (hot): {} msgs", buffer.primary_len());
    println!("  Secondary (compressed): {} msgs", buffer.secondary_len());
    println!("  Bytes saved by compression: {}", buffer.bytes_saved());
    println!();

    drop(buffer);
}

#[cfg(feature = "dhat-heap")]
fn profile_pipeline_simulation() {
    println!("--- Pipeline Simulation (10K messages through buffer) ---");

    let buffer = LockFreeBuffer::new(1_000);

    let stats_before = dhat::HeapStats::get();

    // Simulate pipeline: push and drain in batches
    for batch in 0..100 {
        // Push 100 messages
        for i in 0..100 {
            let msg = Message::new(
                "producer",
                format!("batch-{}-evt-{}", batch, i),
                Bytes::from(vec![0u8; 100]),
            );
            buffer.push(msg);
        }

        // Drain batch
        let drained = buffer.drain(100);
        black_box(drained);
    }

    let stats_after = dhat::HeapStats::get();

    println!(
        "  Total allocations: {}",
        stats_after.total_blocks - stats_before.total_blocks
    );
    println!(
        "  Total bytes allocated: {}",
        stats_after.total_bytes - stats_before.total_bytes
    );
    println!(
        "  Bytes per message (amortized): {:.1}",
        (stats_after.total_bytes - stats_before.total_bytes) as f64 / 10_000.0
    );
    println!(
        "  Peak memory: {} bytes",
        stats_after.max_bytes
    );
    println!();
}
