//! Bounded in-memory per-operation log buffers (drop oldest, count drops).

use crate::backend::{LogChunk, LogStream};
use crate::model::OperationId;
use orbd_proto::control::v1 as pb;
use std::collections::{HashMap, VecDeque};

const MAX_CHUNKS_PER_OP: usize = 4096;
const MAX_BYTES_PER_OP: usize = 1024 * 1024;
const MAX_OPS: usize = 256;

#[derive(Default)]
struct OpLog {
    chunks: VecDeque<LogChunk>,
    bytes: usize,
    last_seq: u64,
}

#[derive(Default)]
pub struct LogStore {
    ops: HashMap<OperationId, OpLog>,
    order: VecDeque<OperationId>,
}

pub fn to_pb(op: OperationId, c: &LogChunk, dropped_before: u64) -> pb::LogChunk {
    pb::LogChunk {
        operation_id: op.to_string(),
        seq: c.seq,
        stream: match c.stream {
            LogStream::Stdout => pb::LogStream::Stdout,
            LogStream::Stderr => pb::LogStream::Stderr,
        } as i32,
        data: c.data.to_vec(),
        dropped_before,
    }
}

impl LogStore {
    /// Stores a chunk; returns false for duplicates (replayed by the executor).
    pub fn push(&mut self, op: OperationId, chunk: LogChunk) -> bool {
        if !self.ops.contains_key(&op) {
            self.order.push_back(op);
            while self.order.len() > MAX_OPS {
                if let Some(old) = self.order.pop_front() {
                    self.ops.remove(&old);
                }
            }
        }
        let log = self.ops.entry(op).or_default();
        if chunk.seq <= log.last_seq {
            return false;
        }
        log.last_seq = chunk.seq;
        log.bytes += chunk.data.len();
        log.chunks.push_back(chunk);
        while log.chunks.len() > MAX_CHUNKS_PER_OP || log.bytes > MAX_BYTES_PER_OP {
            match log.chunks.pop_front() {
                Some(old) => {
                    log.bytes -= old.data.len();
                    metrics::counter!("orbd_dispatch_log_chunks_dropped_total").increment(1);
                }
                None => break,
            }
        }
        true
    }

    /// Chunks with seq > `after`, with any gap reported in `dropped_before`.
    pub fn replay(&self, op: OperationId, after: u64) -> Vec<pb::LogChunk> {
        let Some(log) = self.ops.get(&op) else {
            return Vec::new();
        };
        let mut out: Vec<pb::LogChunk> = Vec::new();
        let mut expected = after + 1;
        for c in log.chunks.iter().filter(|c| c.seq > after) {
            let gap = c.seq.saturating_sub(expected);
            out.push(to_pb(op, c, c.dropped_before.max(gap)));
            expected = c.seq + 1;
        }
        out
    }
}
