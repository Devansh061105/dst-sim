//! Real-mode runtime: runs the same node code as the simulator, over TCP and real files.
//!
//! A [`Runtime`] hosts one node in this process. It implements the same [`Context`] the simulator
//! does, but every call reaches the real world: `send` writes a length-prefixed frame to a TCP
//! connection, `set_timer` uses the real clock, `storage` reads and writes files in a data
//! directory and `sync` is a real `fsync`. Raft and the key-value store don't know the
//! difference; that is the point of writing them against `sim-api` only.
//!
//! The event loop is single-threaded like the simulator's: a listener thread and one reader
//! thread per incoming connection only decode frames and queue them, and the loop hands queued
//! messages and due timers to the node one at a time.

// This crate is the real world: wall-clock time, threads and OS randomness are its job, so the
// determinism lint is switched off here (and only here and in the CLI).
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use sim_api::{Context, Node, NodeId, Storage, SyncError, Time, TimerId};

/// Largest frame accepted off the network, so a garbage length can't make us allocate gigabytes.
const MAX_FRAME: u32 = 64 << 20;
/// How long a send waits to connect or write before giving up on the message (the network may
/// lose messages; Raft retries).
const SEND_TIMEOUT: Duration = Duration::from_millis(200);

/// How messages become bytes and back.
pub struct Codec<M> {
    /// Turns a message into bytes.
    pub encode: fn(&M) -> Vec<u8>,
    /// `None` for bytes that are not a message (they are dropped).
    pub decode: fn(&[u8]) -> Option<M>,
}

/// Why [`Runtime::run_until`] returned.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stopped {
    /// The condition became true.
    Done,
    /// The time limit passed first.
    TimedOut,
    /// The node called [`Context::halt`] (e.g. after a failed fsync). Exit the process and let
    /// a supervisor restart it.
    Halted,
}

/// One node of a cluster, running for real.
pub struct Runtime<N: Node> {
    node: N,
    id: NodeId,
    ids: Vec<NodeId>,
    started: Instant,
    inbox: Receiver<(NodeId, Vec<u8>)>,
    links: Links,
    timers: Timers<N::Timer>,
    disk: Disk,
    rng: Xorshift,
    codec: Codec<N::Message>,
}

impl<N: Node> Runtime<N> {
    /// Listens on `addrs[id]`, keeps files in `data_dir`, and starts `node` (which recovers from
    /// whatever is on disk there).
    pub fn start(
        id: NodeId,
        addrs: Vec<SocketAddr>,
        data_dir: PathBuf,
        node: N,
        codec: Codec<N::Message>,
    ) -> io::Result<Self> {
        std::fs::create_dir_all(&data_dir)?;
        let listener = TcpListener::bind(addrs[id.0 as usize])?;
        let (tx, inbox) = mpsc::channel();
        std::thread::spawn(move || accept(listener, tx));
        let seed = SystemTime::now().duration_since(UNIX_EPOCH).map_or(1, |d| d.as_nanos() as u64)
            ^ u64::from(std::process::id()).rotate_left(32);
        let mut runtime = Runtime {
            node,
            id,
            ids: (0..addrs.len() as u32).map(NodeId).collect(),
            started: Instant::now(),
            inbox,
            links: Links { id, addrs, streams: BTreeMap::new() },
            timers: Timers::default(),
            disk: Disk { dir: data_dir, files: BTreeMap::new(), failed: BTreeSet::new() },
            rng: Xorshift(seed | 1),
            codec,
        };
        runtime.dispatch(|n, ctx| n.on_start(ctx));
        Ok(runtime)
    }

    pub fn node(&self) -> &N {
        &self.node
    }

    pub fn node_mut(&mut self) -> &mut N {
        &mut self.node
    }

    /// Delivers messages and fires timers until `done` returns true, `limit` passes, or the node
    /// halts.
    pub fn run_until(&mut self, mut done: impl FnMut(&mut N) -> bool, limit: Duration) -> Stopped {
        // `None`: no limit (a duration too long to add to the clock, such as `Duration::MAX`).
        let give_up = Instant::now().checked_add(limit);
        loop {
            if self.timers.halted {
                return Stopped::Halted;
            }
            if done(&mut self.node) {
                return Stopped::Done;
            }
            let now = Instant::now();
            if give_up.is_some_and(|g| now >= g) {
                return Stopped::TimedOut;
            }
            if let Some((id, timer)) = self.timers.pop_due(now) {
                self.dispatch(|n, ctx| n.on_timer(ctx, id, timer));
                continue;
            }
            let wake = match (self.timers.next_deadline(), give_up) {
                (Some(t), Some(g)) => Some(t.min(g)),
                (t, g) => t.or(g),
            };
            // With nothing scheduled and no limit, wait up to a second at a time.
            let wait = wake.map_or(Duration::from_secs(1), |w| w.saturating_duration_since(now));
            match self.inbox.recv_timeout(wait) {
                Ok((from, bytes)) => {
                    if let Some(msg) = (self.codec.decode)(&bytes) {
                        self.dispatch(|n, ctx| n.on_message(ctx, from, msg));
                    }
                }
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => return Stopped::Halted,
            }
        }
    }

    fn dispatch(&mut self, callback: impl FnOnce(&mut N, &mut dyn Context<N::Message, N::Timer>)) {
        let mut ctx = RealContext {
            id: self.id,
            ids: &self.ids,
            now: Time::from_nanos(self.started.elapsed().as_nanos() as u64),
            links: &mut self.links,
            timers: &mut self.timers,
            disk: &mut self.disk,
            rng: &mut self.rng,
            encode: self.codec.encode,
        };
        callback(&mut self.node, &mut ctx);
    }
}

/// Accepts connections and starts a reader thread for each.
fn accept(listener: TcpListener, inbox: Sender<(NodeId, Vec<u8>)>) {
    for stream in listener.incoming().flatten() {
        let inbox = inbox.clone();
        std::thread::spawn(move || read_frames(stream, inbox));
    }
}

/// Frames are `[length: u32][sender: u32][message bytes]`, little-endian; the length counts the
/// sender and the message.
fn read_frames(mut stream: TcpStream, inbox: Sender<(NodeId, Vec<u8>)>) {
    let mut header = [0; 4];
    while stream.read_exact(&mut header).is_ok() {
        let len = u32::from_le_bytes(header);
        if !(4..=MAX_FRAME).contains(&len) {
            return; // not our protocol: drop the connection
        }
        let mut frame = vec![0; len as usize];
        if stream.read_exact(&mut frame).is_err() {
            return;
        }
        let from = NodeId(u32::from_le_bytes(frame[..4].try_into().expect("4 bytes")));
        if inbox.send((from, frame.split_off(4))).is_err() {
            return; // the runtime is gone
        }
    }
}

/// Outgoing connections, opened on first use and reopened after an error.
struct Links {
    id: NodeId,
    addrs: Vec<SocketAddr>,
    streams: BTreeMap<NodeId, TcpStream>,
}

impl Links {
    /// Best effort, like the network Raft assumes: if the peer is down, the message is lost.
    fn send(&mut self, to: NodeId, payload: &[u8]) {
        let mut frame = ((payload.len() + 4) as u32).to_le_bytes().to_vec();
        frame.extend(self.id.0.to_le_bytes());
        frame.extend(payload);
        if !self.streams.contains_key(&to) {
            let Some(addr) = self.addrs.get(to.0 as usize) else { return };
            let Ok(stream) = TcpStream::connect_timeout(addr, SEND_TIMEOUT) else { return };
            let _ = stream.set_write_timeout(Some(SEND_TIMEOUT));
            let _ = stream.set_nodelay(true);
            self.streams.insert(to, stream);
        }
        let stream = self.streams.get_mut(&to).expect("connected above");
        if stream.write_all(&frame).is_err() {
            self.streams.remove(&to); // reconnect next time
        }
    }
}

struct Timers<T> {
    due: BinaryHeap<Reverse<(Instant, u64)>>,
    pending: BTreeMap<u64, T>,
    next_id: u64,
    halted: bool,
}

impl<T> Default for Timers<T> {
    fn default() -> Self {
        Timers { due: BinaryHeap::new(), pending: BTreeMap::new(), next_id: 0, halted: false }
    }
}

impl<T> Timers<T> {
    fn pop_due(&mut self, now: Instant) -> Option<(TimerId, T)> {
        while let Some(&Reverse((at, id))) = self.due.peek() {
            if at > now {
                return None;
            }
            self.due.pop();
            // Cancelled timers stay in the heap and are skipped here, as in the simulator.
            if let Some(timer) = self.pending.remove(&id) {
                return Some((TimerId(id), timer));
            }
        }
        None
    }

    fn next_deadline(&self) -> Option<Instant> {
        self.due.peek().map(|Reverse((at, _))| *at)
    }
}

/// Files in the data directory, with real page-cache semantics: writes go to the OS, and only
/// `sync` (fsync) makes them durable.
struct Disk {
    dir: PathBuf,
    files: BTreeMap<String, File>,
    /// Files with a failed write since their last sync; the next sync reports it.
    failed: BTreeSet<String>,
}

impl Disk {
    fn file(&mut self, name: &str) -> io::Result<&mut File> {
        if !self.files.contains_key(name) {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(self.dir.join(name))?;
            self.files.insert(name.to_owned(), file);
        }
        Ok(self.files.get_mut(name).expect("opened above"))
    }

    fn try_write(&mut self, name: &str, offset: u64, data: &[u8]) -> io::Result<()> {
        let file = self.file(name)?;
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(data)
    }
}

impl Storage for Disk {
    fn read(&self, file: &str) -> Vec<u8> {
        std::fs::read(self.dir.join(file)).unwrap_or_default()
    }

    fn file_len(&self, file: &str) -> u64 {
        std::fs::metadata(self.dir.join(file)).map_or(0, |m| m.len())
    }

    fn write(&mut self, file: &str, offset: u64, data: &[u8]) {
        if self.try_write(file, offset, data).is_err() {
            self.failed.insert(file.to_owned());
        }
    }

    fn truncate(&mut self, file: &str, len: u64) {
        if self.file(file).and_then(|f| f.set_len(len)).is_err() {
            self.failed.insert(file.to_owned());
        }
    }

    fn sync(&mut self, file: &str) -> Result<(), SyncError> {
        let failed = self.failed.remove(file);
        match self.file(file).and_then(|f| f.sync_all()) {
            Ok(()) if !failed => Ok(()),
            _ => Err(SyncError),
        }
    }
}

/// xorshift64*: plenty for election timeouts and session ids, and needs no dependency.
struct Xorshift(u64);

impl Xorshift {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }
}

// Disk calls go through the context so that after a halt the rest of the callback can't touch the
// disk: above all, it must not keep writing after a failed fsync.
impl<M, T> Storage for RealContext<'_, M, T> {
    fn read(&self, file: &str) -> Vec<u8> {
        self.disk.read(file)
    }

    fn file_len(&self, file: &str) -> u64 {
        self.disk.file_len(file)
    }

    fn write(&mut self, file: &str, offset: u64, data: &[u8]) {
        if !self.timers.halted {
            self.disk.write(file, offset, data);
        }
    }

    fn truncate(&mut self, file: &str, len: u64) {
        if !self.timers.halted {
            self.disk.truncate(file, len);
        }
    }

    fn sync(&mut self, file: &str) -> Result<(), SyncError> {
        if self.timers.halted {
            return Err(SyncError);
        }
        self.disk.sync(file)
    }
}

/// The real [`Context`]: does each thing as it is asked.
struct RealContext<'a, M, T> {
    id: NodeId,
    ids: &'a [NodeId],
    now: Time,
    links: &'a mut Links,
    timers: &'a mut Timers<T>,
    disk: &'a mut Disk,
    rng: &'a mut Xorshift,
    encode: fn(&M) -> Vec<u8>,
}

impl<M, T> Context<M, T> for RealContext<'_, M, T> {
    fn id(&self) -> NodeId {
        self.id
    }

    fn nodes(&self) -> &[NodeId] {
        self.ids
    }

    fn now(&self) -> Time {
        self.now
    }

    fn send(&mut self, to: NodeId, msg: M) {
        if !self.timers.halted {
            self.links.send(to, &(self.encode)(&msg));
        }
    }

    fn set_timer(&mut self, after: Duration, timer: T) -> TimerId {
        let id = self.timers.next_id;
        self.timers.next_id += 1;
        self.timers.pending.insert(id, timer);
        self.timers.due.push(Reverse((Instant::now() + after, id)));
        TimerId(id)
    }

    fn cancel_timer(&mut self, id: TimerId) {
        self.timers.pending.remove(&id.0);
    }

    fn storage(&mut self) -> &mut dyn Storage {
        self
    }

    fn halt(&mut self) {
        // Nothing asked for after this may happen; the caller exits the process.
        self.timers.halted = true;
    }

    fn random_u64(&mut self) -> u64 {
        self.rng.next()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disk_writes_read_back_and_survive_reopening() {
        let dir = std::env::temp_dir().join(format!("dst-disk-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let mut disk = Disk { dir: dir.clone(), files: BTreeMap::new(), failed: BTreeSet::new() };
        disk.append("f", b"hello");
        disk.write("f", 1, b"EL");
        disk.sync("f").unwrap();
        disk.truncate("f", 4);
        disk.sync("f").unwrap();
        let reopened = Disk { dir: dir.clone(), files: BTreeMap::new(), failed: BTreeSet::new() };
        assert_eq!(reopened.read("f"), b"hELl");
        assert_eq!(reopened.read("missing"), b"");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn timers_fire_in_order_and_cancelled_ones_never() {
        let mut timers = Timers::default();
        let now = Instant::now();
        for (id, ms) in [(0u64, 30), (1, 10), (2, 20)] {
            timers.pending.insert(id, id);
            timers.due.push(Reverse((now + Duration::from_millis(ms), id)));
        }
        timers.pending.remove(&2);
        let later = now + Duration::from_secs(1);
        let fired: Vec<u64> =
            std::iter::from_fn(|| timers.pop_due(later)).map(|(_, t)| t).collect();
        assert_eq!(fired, [1, 0]);
        assert_eq!(timers.pop_due(later), None);
    }
}
