//! Bounded operation-log queue, separate from control messages. When full it drops the
//! oldest chunk; the gap shows up as `dropped_before` on the next chunk of that
//! operation (derived from sequence numbers, so reconnects are not miscounted).

use crate::model::OperationId;
use crate::runtime::{LogSink, LogStream};
use orbd_proto::control::v1 as pb;
use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use tokio::sync::Notify;

const MAX_CHUNK: usize = 16 * 1024;

struct Chunk {
    op: OperationId,
    seq: u64,
    stream: LogStream,
    data: bytes::Bytes,
}

#[derive(Default)]
struct Inner {
    queue: VecDeque<Chunk>,
    next_seq: HashMap<OperationId, u64>,
    last_sent: HashMap<OperationId, u64>,
}

pub struct LogQueue {
    inner: Mutex<Inner>,
    notify: Notify,
    capacity: usize,
}

impl LogQueue {
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Mutex::default(),
            notify: Notify::new(),
            capacity,
        }
    }

    pub fn push(&self, op: OperationId, stream: LogStream, data: bytes::Bytes) {
        for part in data.chunks(MAX_CHUNK) {
            let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            let seq = g.next_seq.entry(op).or_insert(0);
            *seq += 1;
            let seq = *seq;
            if g.queue.len() >= self.capacity {
                g.queue.pop_front();
            }
            g.queue.push_back(Chunk {
                op,
                seq,
                stream,
                data: bytes::Bytes::copy_from_slice(part),
            });
            drop(g);
            self.notify.notify_one();
        }
    }

    pub fn pop(&self) -> Option<pb::LogChunk> {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let c = g.queue.pop_front()?;
        let last = g.last_sent.insert(c.op, c.seq).unwrap_or(0);
        Some(pb::LogChunk {
            operation_id: c.op.to_string(),
            seq: c.seq,
            stream: match c.stream {
                LogStream::Stdout => pb::LogStream::Stdout,
                LogStream::Stderr => pb::LogStream::Stderr,
            } as i32,
            data: c.data.to_vec(),
            dropped_before: c.seq.saturating_sub(last + 1),
        })
    }

    pub async fn wait(&self) {
        self.notify.notified().await
    }

    /// Forgets sequence state of a finished operation once none of its chunks are queued.
    pub fn forget(&self, op: OperationId) {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if !g.queue.iter().any(|c| c.op == op) {
            g.next_seq.remove(&op);
            g.last_sent.remove(&op);
        }
    }
}

/// A sink handed to runtimes; a forwarder task moves chunks into the queue.
pub fn sink(queue: std::sync::Arc<LogQueue>, op: OperationId) -> LogSink {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<(LogStream, bytes::Bytes)>(64);
    tokio::spawn(async move {
        while let Some((stream, data)) = rx.recv().await {
            queue.push(op, stream, data);
        }
    });
    tx
}
