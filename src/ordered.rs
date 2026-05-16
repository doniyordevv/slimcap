//! Streaming `log_time`-ordered message reader.
//!
//! `mcap::MessageStream` yields messages in physical file/chunk order. Bags
//! recorded with overlapping chunks (the norm for multi-sensor rigs) are not
//! globally — nor even per-topic — sorted by `log_time` that way. The whole
//! transcode pipeline assumes capture order: the encoder's VFR pts / forced-IDR
//! logic, monotonic per-topic `log_time` for SLAM consumers, and `mcap doctor`
//! conformance all depend on it. So we re-establish ordering here instead of
//! requiring callers to pre-sort with an external tool.
//!
//! Approach: a k-way merge over chunks. Chunks are visited in
//! `message_start_time` order; a chunk is fully loaded (its messages copied to
//! owned buffers, then its decompression buffer dropped) only once the merge
//! frontier reaches its start time. Because `message_start_time` is the
//! minimum `log_time` within a chunk, any not-yet-loaded chunk cannot contain
//! a message earlier than the smallest already-buffered one — so popping the
//! heap minimum yields a globally monotonic stream. Peak memory is bounded by
//! the number of chunks whose time ranges overlap (the bag's chunk-overlap
//! depth), not by the file size.

use std::cmp::Reverse;
use std::collections::BinaryHeap;

use anyhow::Result;
use mcap::records::ChunkIndex;
use mcap::Summary;

/// The subset of `mcap::Message` the pipeline needs, detached from the chunk
/// decompression buffer so the chunk can be freed while this outlives it.
pub struct OwnedMessage {
    pub channel_id: u16,
    pub sequence: u32,
    pub log_time: u64,
    pub publish_time: u64,
    pub data: Vec<u8>,
}

struct HeapItem {
    log_time: u64,
    /// Global load order — stable tiebreak so equal-`log_time` messages keep
    /// their original relative order and heap keys stay unique.
    seq: u64,
    msg: OwnedMessage,
}

impl PartialEq for HeapItem {
    fn eq(&self, o: &Self) -> bool {
        (self.log_time, self.seq) == (o.log_time, o.seq)
    }
}
impl Eq for HeapItem {}
impl PartialOrd for HeapItem {
    fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for HeapItem {
    fn cmp(&self, o: &Self) -> std::cmp::Ordering {
        (self.log_time, self.seq).cmp(&(o.log_time, o.seq))
    }
}

/// Iterator yielding every message in the MCAP in non-decreasing `log_time`.
pub struct OrderedReader<'a> {
    mcap: &'a [u8],
    summary: &'a Summary,
    /// Chunk indexes sorted by `message_start_time`.
    chunks: Vec<ChunkIndex>,
    next_chunk: usize,
    heap: BinaryHeap<Reverse<HeapItem>>,
    seq: u64,
}

impl<'a> OrderedReader<'a> {
    pub fn new(mcap: &'a [u8], summary: &'a Summary) -> Result<Self> {
        let mut chunks = summary.chunk_indexes.clone();
        if chunks.is_empty() {
            anyhow::bail!(
                "input MCAP has no chunk index; cannot read in log_time order"
            );
        }
        chunks.sort_by_key(|c| c.message_start_time);
        Ok(Self {
            mcap,
            summary,
            chunks,
            next_chunk: 0,
            heap: BinaryHeap::new(),
            seq: 0,
        })
    }

    fn load_chunk(&mut self, idx: usize) -> Result<()> {
        let ci = self.chunks[idx].clone();
        // Copy the chunk's messages out, then let `stream_chunk`'s
        // decompression buffer drop before we touch `self.heap`.
        let mut owned = Vec::new();
        for m in self.summary.stream_chunk(self.mcap, &ci)? {
            let m = m?;
            owned.push(OwnedMessage {
                channel_id: m.channel.id,
                sequence: m.sequence,
                log_time: m.log_time,
                publish_time: m.publish_time,
                data: m.data.into_owned(),
            });
        }
        for msg in owned {
            let seq = self.seq;
            self.seq += 1;
            self.heap.push(Reverse(HeapItem {
                log_time: msg.log_time,
                seq,
                msg,
            }));
        }
        Ok(())
    }
}

impl Iterator for OrderedReader<'_> {
    type Item = Result<OwnedMessage>;

    fn next(&mut self) -> Option<Self::Item> {
        // Load every chunk that could still contain a message <= the current
        // heap minimum. A chunk whose `message_start_time` exceeds the
        // smallest buffered `log_time` cannot hold an earlier message, so once
        // we reach one we can safely pop.
        loop {
            let need_more = match self.heap.peek() {
                None => self.next_chunk < self.chunks.len(),
                Some(Reverse(top)) => {
                    self.next_chunk < self.chunks.len()
                        && self.chunks[self.next_chunk].message_start_time
                            <= top.log_time
                }
            };
            if !need_more {
                break;
            }
            let i = self.next_chunk;
            self.next_chunk += 1;
            if let Err(e) = self.load_chunk(i) {
                return Some(Err(e));
            }
        }
        self.heap.pop().map(|Reverse(it)| Ok(it.msg))
    }
}
