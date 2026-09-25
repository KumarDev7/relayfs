//! FUSE filesystem on the bridge machine with local backing cache and offline resilience.
//!
//! `mount_remote` mounts a remote directory into the local filesystem. Every
//! kernel operation interacts with both the remote agent and a local backing cache.
//!
//! 1. High Speed: FUSE writes land in the local cache and are acknowledged
//!    at disk speed; a background flusher streams dirty ranges to the target
//!    with a bounded window of concurrent RPCs (coalesced up to 4 MiB), so a
//!    transfer saturates the network instead of waiting one RTT per write.
//!    Reads that miss the cache fetch concurrently and prefetch the rest of
//!    the file, likewise saturating the link.
//! 2. Offline Resilience: If the target disconnects, the mounted folder remains
//!    completely usable for `ls`, `cp`, editors, etc., backed by the local cache.
//! 3. Clean Teardown: Proactive unmounting prevents orphaned "Transport endpoint
//!    is not connected" mount states.

use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use fuser::{
    spawn_mount, AccessFlags, Config, Errno, FileAttr, FileHandle, FileType, Filesystem,
    FopenFlags, Generation, INodeNo, KernelConfig, LockOwner, MountOption, OpenFlags, RenameFlags,
    ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen,
    ReplyStatfs, ReplyWrite, ReplyXattr, Request, SessionACL, TimeOrNow, WriteFlags,
};
use relayfs_protocol::{FileKind, RpcError};
use tokio::sync::{Mutex, Notify};

use crate::client::AgentClient;

const TTL: Duration = Duration::from_secs(1);
const FUSE_TIMEOUT: Duration = Duration::from_secs(15);

/// Maps inode numbers to remote paths for the lifetime of a mount.
struct InodeMap {
    next: u64,
    by_ino: HashMap<u64, PathBuf>,
    by_path: HashMap<PathBuf, u64>,
}

impl InodeMap {
    fn new(root: PathBuf) -> Self {
        let mut by_ino = HashMap::new();
        let mut by_path = HashMap::new();
        by_ino.insert(1, root.clone());
        by_path.insert(root, 1);
        Self {
            next: 2,
            by_ino,
            by_path,
        }
    }

    fn path(&self, ino: INodeNo) -> Option<PathBuf> {
        self.by_ino.get(&ino.0).cloned()
    }

    fn ino_for(&mut self, path: &Path) -> u64 {
        if let Some(&ino) = self.by_path.get(path) {
            return ino;
        }
        let ino = self.next;
        self.next += 1;
        self.by_path.insert(path.to_path_buf(), ino);
        self.by_ino.insert(ino, path.to_path_buf());
        ino
    }
}

/// Owns the FUSE worker runtime. A `tokio::runtime::Runtime` panics when
/// dropped inside an async context ("Cannot drop a runtime in a context where
/// blocking is not allowed") — which is exactly where mount failures unwind —
/// so shut it down in the background instead of blocking.
struct FsRuntime(Option<tokio::runtime::Runtime>);

impl FsRuntime {
    fn new() -> Self {
        Self(Some(tokio::runtime::Runtime::new().expect("fuse runtime")))
    }

    fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.0.as_ref().expect("fuse runtime").block_on(future)
    }

    fn spawn<F>(&self, future: F) -> tokio::task::JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.0.as_ref().expect("fuse runtime").spawn(future)
    }
}

impl Drop for FsRuntime {
    fn drop(&mut self) {
        if let Some(rt) = self.0.take() {
            rt.shutdown_background();
        }
    }
}

/// The FUSE filesystem backed by the remote agent and local disk cache.
pub struct RemoteFs {
    client: Arc<AgentClient>,
    rt: FsRuntime,
    read_only: bool,
    remote_dir: PathBuf,
    cache_dir: PathBuf,
    inodes: Mutex<InodeMap>,
    /// Write-behind upload queue drained by the background flusher.
    uploads: Arc<UploadQueue>,
    /// Paths with a read-prefetch task currently running.
    prefetching: Arc<std::sync::Mutex<HashSet<PathBuf>>>,
}

impl RemoteFs {
    pub fn new(
        client: Arc<AgentClient>,
        remote_dir: PathBuf,
        cache_dir: PathBuf,
        read_only: bool,
    ) -> Self {
        let inodes = Mutex::new(InodeMap::new(remote_dir.clone()));
        let _ = std::fs::create_dir_all(&cache_dir);
        let uploads = Arc::new(UploadQueue::new());
        let rt = FsRuntime::new();
        if !read_only {
            rt.spawn(flusher_loop(
                client.clone(),
                remote_dir.clone(),
                cache_dir.clone(),
                uploads.clone(),
            ));
        }
        Self {
            client,
            rt,
            read_only,
            remote_dir,
            cache_dir,
            inodes,
            uploads,
            prefetching: Arc::new(std::sync::Mutex::new(HashSet::new())),
        }
    }

    /// Return the corresponding local cache path for a remote path.
    fn cache_path(&self, remote_path: &Path) -> PathBuf {
        if let Ok(rel) = remote_path.strip_prefix(&self.remote_dir) {
            self.cache_dir.join(rel)
        } else {
            self.cache_dir
                .join(remote_path.file_name().unwrap_or_default())
        }
    }

    /// Resolve an inode to a remote path.
    fn path_for(&self, ino: INodeNo) -> Option<PathBuf> {
        self.rt
            .block_on(async { self.inodes.lock().await.path(ino) })
    }

    /// Resolve `parent/name` to a remote path, registering the inode.
    fn child_path(&self, parent: INodeNo, name: &OsStr) -> Option<PathBuf> {
        self.rt.block_on(async {
            let mut inodes = self.inodes.lock().await;
            let parent_path = inodes.path(parent)?;
            let path = parent_path.join(name);
            inodes.ino_for(&path);
            Some(path)
        })
    }

    /// RPC with a timeout; maps errors to Errno. Never logs params: file
    /// payloads are megabytes of base64 and logging them per RPC was a major
    /// throughput drag at the default `info` level.
    fn rpc(&self, method: &str, params: serde_json::Value) -> Result<serde_json::Value, Errno> {
        tracing::debug!("fuse {method}");
        self.rt
            .block_on(self.client.call_timeout(method, params, FUSE_TIMEOUT))
            .map_err(|e| errno_for(&e))
    }

    /// Queue bytes (already written to the local cache) for background upload
    /// and block only when the dirty backlog exceeds `QUEUE_CAP` (real
    /// backpressure when the network cannot keep up).
    fn enqueue_upload(&self, path: &Path, offset: u64, data_len: usize) {
        if data_len == 0 {
            return;
        }
        let queue = self.uploads.clone();
        let path = path.to_path_buf();
        let (start, end) = (offset, offset + data_len as u64);
        self.rt.block_on(async move {
            queue.enqueue(path, start, end).await;
            queue.wait_below(QUEUE_CAP).await;
        });
    }

    /// Block until every pending upload for `paths` has been acknowledged by
    /// the target. Returns `Errno::EIO` if a send fails or the wait times out;
    /// the flusher keeps retrying queued data in the background either way.
    fn drain_uploads(&self, paths: &[&Path]) -> Result<(), Errno> {
        if paths.is_empty() {
            return Ok(());
        }
        let owned: Vec<PathBuf> = paths.iter().map(|p| p.to_path_buf()).collect();
        let queue = self.uploads.clone();
        self.rt
            .block_on(async move { queue.wait_drain(&owned, FUSE_TIMEOUT).await })
    }

    /// Start (at most one) background prefetch that warms the local cache for
    /// the rest of a file after a sequential read miss.
    fn kick_prefetch(&self, path: &Path) {
        {
            let mut set = self.prefetching.lock().unwrap_or_else(|e| e.into_inner());
            if !set.insert(path.to_path_buf()) {
                return;
            }
        }
        let client = self.client.clone();
        let set = self.prefetching.clone();
        let key = path.to_path_buf();
        let remote = path.to_string_lossy().into_owned();
        let local = self.cache_path(path);
        self.rt.spawn(async move {
            prefetch_tail(client, remote, local).await;
            set.lock().unwrap_or_else(|e| e.into_inner()).remove(&key);
        });
    }

    /// Fetch remote attributes for a path.
    fn remote_attr(&self, path: &Path) -> Result<FileAttr, Errno> {
        let result = self.rpc(
            relayfs_protocol::method::STAT,
            serde_json::json!({ "path": path.to_string_lossy() }),
        )?;
        let stat: relayfs_protocol::StatResult =
            serde_json::from_value(result).map_err(|_| Errno::EIO)?;
        Ok(attr_from_stat(stat, self.ino_for_path(path)))
    }

    /// Fetch attributes: online uses remote stat (and mirrors locally); offline falls back to local cache.
    fn attr_for(&self, path: &Path) -> Result<FileAttr, Errno> {
        let local = self.cache_path(path);
        match self.remote_attr(path) {
            Ok(attr) => {
                if attr.kind == FileType::Directory {
                    let _ = std::fs::create_dir_all(&local);
                }
                Ok(attr)
            }
            Err(remote_err) => {
                // If remote is offline or unreachable, fall back seamlessly to local cache
                if let Ok(meta) = std::fs::symlink_metadata(&local) {
                    Ok(attr_from_metadata(&meta, self.ino_for_path(path)))
                } else {
                    Err(remote_err)
                }
            }
        }
    }

    fn ino_for_path(&self, path: &Path) -> u64 {
        self.rt
            .block_on(async { self.inodes.lock().await.ino_for(path) })
    }
}

fn attr_from_stat(stat: relayfs_protocol::StatResult, ino: u64) -> FileAttr {
    let kind = match stat.kind {
        FileKind::File => FileType::RegularFile,
        FileKind::Dir => FileType::Directory,
        FileKind::Symlink => FileType::Symlink,
        FileKind::Other => FileType::RegularFile,
    };
    let now = std::time::SystemTime::now();
    let modified = std::time::UNIX_EPOCH + Duration::from_secs(stat.modified);
    FileAttr {
        ino: INodeNo(ino),
        size: stat.size,
        blocks: stat.size / 512,
        atime: now,
        mtime: modified,
        ctime: modified,
        crtime: modified,
        kind,
        perm: (stat.mode & 0o7777) as u16,
        nlink: if kind == FileType::Directory { 2 } else { 1 },
        uid: stat.uid,
        gid: stat.gid,
        rdev: 0,
        blksize: 4096,
        flags: 0,
    }
}

fn attr_from_metadata(meta: &std::fs::Metadata, ino: u64) -> FileAttr {
    let kind = if meta.is_dir() {
        FileType::Directory
    } else if meta.file_type().is_symlink() {
        FileType::Symlink
    } else {
        FileType::RegularFile
    };
    let now = std::time::SystemTime::now();
    let modified = meta.modified().unwrap_or(now);
    #[cfg(unix)]
    let (mode, uid, gid) = {
        use std::os::unix::fs::MetadataExt;
        (meta.mode(), meta.uid(), meta.gid())
    };
    #[cfg(not(unix))]
    let (mode, uid, gid) = (0o755, 1000, 1000);

    FileAttr {
        ino: INodeNo(ino),
        size: meta.len(),
        blocks: meta.len() / 512,
        atime: now,
        mtime: modified,
        ctime: modified,
        crtime: modified,
        kind,
        perm: (mode & 0o7777) as u16,
        nlink: if kind == FileType::Directory { 2 } else { 1 },
        uid,
        gid,
        rdev: 0,
        blksize: 4096,
        flags: 0,
    }
}

fn errno_for(e: &RpcError) -> Errno {
    let msg = e.message.to_lowercase();
    if msg.contains("no such file") || msg.contains("not found") {
        Errno::ENOENT
    } else if msg.contains("permission") || msg.contains("denied") {
        Errno::EACCES
    } else if msg.contains("exists") {
        Errno::EEXIST
    } else if msg.contains("not a directory") {
        Errno::ENOTDIR
    } else if msg.contains("is a directory") {
        Errno::EISDIR
    } else {
        Errno::EIO
    }
}

/// Bytes of `WRITE_AT` payloads allowed in flight at once. Sized to cover a
/// bandwidth-delay product (e.g. 512 Mbit/s at 500 ms RTT) so the pipe never
/// runs dry during a large transfer.
const WRITE_WINDOW: u64 = 32 * 1024 * 1024;
/// Dirty bytes buffered locally before FUSE writers get backpressure.
const QUEUE_CAP: u64 = 96 * 1024 * 1024;
/// Maximum payload per `WRITE_AT` RPC (the target accepts up to 8 MiB).
const CHUNK_MAX: u64 = 4 * 1024 * 1024;
/// Concurrent chunk fetches while warming the cache during reads.
const PREFETCH_INFLIGHT: usize = 4;
const PREFETCH_CHUNK: u64 = 1024 * 1024;
/// Concurrent chunk fetches for the mount-time background sync.
const SYNC_INFLIGHT: usize = 8;

/// Half-open byte range [start, end) of pending data for one file.
type Range = (u64, u64);

/// Outcome of one background `WRITE_AT` send.
#[derive(Clone, Copy, PartialEq)]
enum SendOutcome {
    /// Acknowledged by the target.
    Sent,
    /// Transient failure (offline/timeout): requeue with backoff.
    Retry,
    /// Permanent target-side failure (file gone/unwritable): drop, surface
    /// the error to any waiting drain.
    Drop,
}

/// Permanent target-side failures must not be retried forever; connection
/// failures must be.
fn send_fatal(e: &RpcError) -> bool {
    if e.code == relayfs_protocol::code::METHOD_NOT_FOUND {
        return true;
    }
    let msg = e.message.to_lowercase();
    msg.contains("no such file")
        || msg.contains("not found")
        || msg.contains("permission")
        || msg.contains("denied")
}

/// Insert [start, end) into a sorted, disjoint, non-adjacent range list,
/// merging overlap and adjacency; returns the number of newly covered bytes.
fn merge_range(ranges: &mut Vec<Range>, start: u64, end: u64) -> u64 {
    if end <= start {
        return 0;
    }
    let covered: u64 = ranges
        .iter()
        .filter(|&&(s, e)| s < end && start < e)
        .map(|&(s, e)| e.min(end) - s.max(start))
        .sum();
    let mut new_start = start;
    let mut new_end = end;
    let mut i = 0;
    while i < ranges.len() {
        let (s, e) = ranges[i];
        if e < new_start {
            i += 1;
            continue;
        }
        if s > new_end {
            break;
        }
        new_start = new_start.min(s);
        new_end = new_end.max(e);
        ranges.remove(i);
    }
    ranges.insert(i, (new_start, new_end));
    (new_end - new_start) - covered
}

struct QueueState {
    /// Dirty ranges per remote path: sorted, disjoint, non-adjacent.
    dirty: HashMap<PathBuf, Vec<Range>>,
    /// Ranges currently inside a `WRITE_AT` RPC.
    inflight: HashMap<PathBuf, Vec<Range>>,
    /// Paths whose new dirty ranges overlap in-flight sends; their uploads
    /// pause until those sends land, preserving byte order for rewrites.
    blocked: HashSet<PathBuf>,
    dirty_bytes: u64,
    inflight_bytes: u64,
    /// Monotonic failed-send counter; drains snapshot it to detect errors.
    errors: u64,
    /// Flusher pauses until this instant after a failed send (offline backoff).
    retry_after: std::time::Instant,
    backoff_ms: u64,
}

/// Write-behind upload queue: FUSE writes land in the local cache and the
/// syscall returns at disk speed; a background flusher streams dirty ranges
/// to the target with a bounded window of concurrent RPCs.
struct UploadQueue {
    state: Mutex<QueueState>,
    /// Fires on send completion/requeue: wakes drains and backpressure waits.
    progress: Notify,
    /// Fires on enqueue: wakes the flusher.
    wake: Notify,
}

impl UploadQueue {
    fn new() -> Self {
        Self {
            state: Mutex::new(QueueState {
                dirty: HashMap::new(),
                inflight: HashMap::new(),
                blocked: HashSet::new(),
                dirty_bytes: 0,
                inflight_bytes: 0,
                errors: 0,
                retry_after: std::time::Instant::now(),
                backoff_ms: 0,
            }),
            progress: Notify::new(),
            wake: Notify::new(),
        }
    }

    /// Queue [start, end) for upload, ordering against overlapping in-flight
    /// sends, then wake the flusher.
    async fn enqueue(&self, path: PathBuf, start: u64, end: u64) {
        if end <= start {
            return;
        }
        let mut st = self.state.lock().await;
        if let Some(inf) = st.inflight.get(&path) {
            if inf.iter().any(|&(s, e)| start < e && s < end) {
                st.blocked.insert(path.clone());
            }
        }
        let ranges = st.dirty.entry(path).or_default();
        st.dirty_bytes += merge_range(ranges, start, end);
        drop(st);
        self.wake.notify_one();
    }

    /// Claim dirty ranges for sending: up to the in-flight window total and
    /// `CHUNK_MAX` per RPC. Empty while in post-failure backoff.
    async fn take_batch(&self) -> Vec<(PathBuf, Range)> {
        let mut st = self.state.lock().await;
        let mut out = Vec::new();
        if st.dirty.is_empty() || std::time::Instant::now() < st.retry_after {
            return out;
        }
        let mut window_left = WRITE_WINDOW.saturating_sub(st.inflight_bytes);
        if window_left == 0 {
            return out;
        }
        let paths: Vec<PathBuf> = st.dirty.keys().cloned().collect();
        for path in paths {
            if window_left == 0 {
                break;
            }
            if st.blocked.contains(&path) {
                continue;
            }
            loop {
                let front = st.dirty.get(&path).and_then(|r| r.first()).copied();
                let Some((s, e)) = front else {
                    st.dirty.remove(&path);
                    break;
                };
                let piece_end = e.min(s + CHUNK_MAX).min(s + window_left);
                if piece_end <= s {
                    break;
                }
                let piece = (s, piece_end);
                let ranges = st.dirty.get_mut(&path).expect("entry exists");
                if ranges[0].1 > piece_end {
                    ranges[0].0 = piece_end;
                } else {
                    ranges.remove(0);
                }
                if ranges.is_empty() {
                    st.dirty.remove(&path);
                }
                st.dirty_bytes = st.dirty_bytes.saturating_sub(piece_end - s);
                st.inflight_bytes += piece_end - s;
                st.inflight.entry(path.clone()).or_default().push(piece);
                window_left -= piece_end - s;
                out.push((path.clone(), piece));
                if window_left == 0 {
                    break;
                }
            }
        }
        out
    }

    /// Record a send outcome and release waiters.
    async fn finish(&self, path: &Path, range: Range, outcome: SendOutcome) {
        let mut st = self.state.lock().await;
        let (s, e) = range;
        st.inflight_bytes = st.inflight_bytes.saturating_sub(e - s);
        if let Some(list) = st.inflight.get_mut(path) {
            list.retain(|&r| r != range);
            if list.is_empty() {
                st.inflight.remove(path);
            }
        }
        match outcome {
            SendOutcome::Sent => {
                st.retry_after = std::time::Instant::now();
                st.backoff_ms = 0;
            }
            SendOutcome::Retry => {
                st.errors += 1;
                st.backoff_ms = (st.backoff_ms.max(100) * 2).min(2000);
                st.retry_after = std::time::Instant::now() + Duration::from_millis(st.backoff_ms);
                match st.dirty.get_mut(path) {
                    Some(ranges) => st.dirty_bytes += merge_range(ranges, s, e),
                    None => {
                        let mut ranges = Vec::new();
                        st.dirty_bytes += merge_range(&mut ranges, s, e);
                        st.dirty.insert(path.to_path_buf(), ranges);
                    }
                }
            }
            SendOutcome::Drop => {
                st.errors += 1;
            }
        }
        if !st.inflight.contains_key(path) {
            st.blocked.remove(path);
        }
        drop(st);
        self.progress.notify_waiters();
        self.wake.notify_one();
    }

    /// Wait until no dirty or in-flight data remains for `paths`, a send
    /// fails, or `timeout` elapses.
    async fn wait_drain(&self, paths: &[PathBuf], timeout: Duration) -> Result<(), Errno> {
        let deadline = tokio::time::Instant::now() + timeout;
        let start_errors = self.state.lock().await.errors;
        loop {
            let wait = self.progress.notified();
            tokio::pin!(wait);
            let st = self.state.lock().await;
            let pending = paths.iter().any(|p| {
                st.dirty.get(p).is_some_and(|v| !v.is_empty())
                    || st.inflight.get(p).is_some_and(|v| !v.is_empty())
            });
            if !pending {
                return Ok(());
            }
            if st.errors > start_errors {
                return Err(Errno::EIO);
            }
            drop(st);
            tokio::select! {
                _ = &mut wait => {}
                _ = tokio::time::sleep_until(deadline) => return Err(Errno::EIO),
                // Catches the rare notify that raced our state check.
                _ = tokio::time::sleep(Duration::from_millis(250)) => {}
            }
        }
    }

    /// Backpressure: block while the dirty backlog exceeds `cap`.
    async fn wait_below(&self, cap: u64) {
        loop {
            {
                let st = self.state.lock().await;
                if st.dirty_bytes <= cap {
                    return;
                }
            }
            tokio::select! {
                _ = self.progress.notified() => {}
                _ = tokio::time::sleep(Duration::from_millis(250)) => {}
            }
        }
    }
}

/// Streams dirty ranges to the target for the lifetime of the mount: claims
/// work from the queue, reads the exact bytes from the local cache, and fires
/// `WRITE_AT` RPCs with up to `WRITE_WINDOW` bytes in flight. Throughput is
/// bounded by the network, not by round-trip latency.
async fn flusher_loop(
    client: Arc<AgentClient>,
    remote_dir: PathBuf,
    cache_dir: PathBuf,
    queue: Arc<UploadQueue>,
) {
    loop {
        let batch = queue.take_batch().await;
        if batch.is_empty() {
            tokio::select! {
                _ = queue.wake.notified() => {}
                _ = tokio::time::sleep(Duration::from_millis(250)) => {}
            }
            continue;
        }
        for (path, range) in batch {
            let client = client.clone();
            let queue = queue.clone();
            let rel = path
                .strip_prefix(&remote_dir)
                .map(Path::to_path_buf)
                .unwrap_or_else(|_| path.clone());
            let local = cache_dir.join(rel);
            tokio::spawn(async move {
                let (start, end) = range;
                let len = (end - start) as usize;
                let data = tokio::task::spawn_blocking(move || -> Option<Vec<u8>> {
                    use std::io::{Read, Seek, SeekFrom};
                    let mut f = std::fs::File::open(&local).ok()?;
                    f.seek(SeekFrom::Start(start)).ok()?;
                    let mut buf = vec![0u8; len];
                    let mut got = 0;
                    while got < len {
                        match f.read(&mut buf[got..]) {
                            Ok(0) => break,
                            Ok(n) => got += n,
                            Err(_) => return None,
                        }
                    }
                    buf.truncate(got);
                    Some(buf)
                })
                .await
                .ok()
                .flatten();

                // A missing/short cache file means the local write failed or
                // a truncate raced us; retry so the drain reports the problem.
                let Some(bytes) = data.filter(|b| b.len() == len) else {
                    queue.finish(&path, range, SendOutcome::Retry).await;
                    return;
                };
                let outcome = match client
                    .call_timeout(
                        relayfs_protocol::method::WRITE_AT,
                        serde_json::json!({
                            "path": path.to_string_lossy(),
                            "offset": start,
                            "data": base64_encode(&bytes),
                        }),
                        FUSE_TIMEOUT,
                    )
                    .await
                {
                    Ok(_) => SendOutcome::Sent,
                    Err(e) if send_fatal(&e) => SendOutcome::Drop,
                    Err(_) => SendOutcome::Retry,
                };
                queue.finish(&path, range, outcome).await;
            });
        }
    }
}

/// Warm the local cache for the remainder of a file after a read miss, using
/// a small window of concurrent `READ_FILE` RPCs. Sequential readers then run
/// at network speed instead of one RTT per kernel read.
async fn prefetch_tail(client: Arc<AgentClient>, remote_path: String, local_path: PathBuf) {
    // Size comes from one STAT; failure (offline) just skips the prefetch.
    let size = match client
        .call_timeout(
            relayfs_protocol::method::STAT,
            serde_json::json!({ "path": remote_path }),
            FUSE_TIMEOUT,
        )
        .await
    {
        Ok(v) => serde_json::from_value::<relayfs_protocol::StatResult>(v)
            .map(|s| s.size)
            .unwrap_or(0),
        Err(_) => return,
    };
    let from = std::fs::metadata(&local_path).map(|m| m.len()).unwrap_or(0);
    if from >= size {
        return;
    }
    let mut offsets = Vec::new();
    let mut off = from;
    while off < size {
        offsets.push(off);
        off += PREFETCH_CHUNK;
    }
    use futures::StreamExt;
    // `buffered` fetches with PREFETCH_INFLIGHT concurrency but yields results
    // in OFFSET order, so cache writes stay hole-free: read() trusts the cache
    // file length as a valid prefix, and out-of-order writes would expose
    // zeros for ranges that were never fetched.
    let mut fetches = futures::stream::iter(offsets)
        .map(|start| {
            let client = client.clone();
            let path = remote_path.clone();
            let limit = (size - start).min(PREFETCH_CHUNK);
            async move {
                let v = client
                    .call_timeout(
                        relayfs_protocol::method::READ_FILE,
                        serde_json::json!({ "path": path, "offset": start, "limit": limit }),
                        FUSE_TIMEOUT,
                    )
                    .await
                    .ok()?;
                let read = serde_json::from_value::<relayfs_protocol::ReadFileResult>(v).ok()?;
                let bytes = base64_decode(&read.data).ok()?;
                if bytes.is_empty() {
                    None
                } else {
                    Some((start, bytes))
                }
            }
        })
        .buffered(PREFETCH_INFLIGHT);

    while let Some(item) = fetches.next().await {
        let Some((start, bytes)) = item else {
            break; // target went away; stop warming
        };
        let local = local_path.clone();
        let applied = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
            use std::io::{Seek, SeekFrom, Write};
            let mut f = std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(false)
                .open(&local)?;
            f.seek(SeekFrom::Start(start))?;
            f.write_all(&bytes)
        })
        .await
        .is_ok_and(|r| r.is_ok());
        if !applied {
            break;
        }
    }
}

impl Filesystem for RemoteFs {
    fn init(&mut self, _req: &Request, config: &mut KernelConfig) -> std::io::Result<()> {
        // Large readahead + deep background queue: the kernel can keep many
        // read requests in flight, which is what saturates a fast link.
        let _ = config.set_max_readahead(4 * 1024 * 1024);
        let _ = config.set_max_write(4 * 1024 * 1024);
        let _ = config.set_max_background(64);
        let _ = config.set_congestion_threshold(48);
        Ok(())
    }

    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let Some(path) = self.child_path(parent, name) else {
            reply.error(Errno::ENOENT);
            return;
        };
        match self.attr_for(&path) {
            Ok(attr) => reply.entry(&TTL, &attr, Generation(0)),
            Err(e) => reply.error(e),
        }
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        let Some(path) = self.path_for(ino) else {
            reply.error(Errno::ENOENT);
            return;
        };
        match self.attr_for(&path) {
            Ok(attr) => reply.attr(&TTL, &attr),
            Err(e) => reply.error(e),
        }
    }

    fn setattr(
        &self,
        _req: &Request,
        ino: INodeNo,
        mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<TimeOrNow>,
        _mtime: Option<TimeOrNow>,
        _ctime: Option<std::time::SystemTime>,
        _fh: Option<FileHandle>,
        _crtime: Option<std::time::SystemTime>,
        _chgtime: Option<std::time::SystemTime>,
        _bkuptime: Option<std::time::SystemTime>,
        _flags: Option<fuser::BsdFileFlags>,
        reply: ReplyAttr,
    ) {
        let Some(path) = self.path_for(ino) else {
            reply.error(Errno::ENOENT);
            return;
        };
        let local = self.cache_path(&path);
        if let Some(size) = size {
            // Truncate must not overtake queued writes for this file: that
            // would let stale bytes land after the shrink.
            if self.drain_uploads(&[&path]).is_err() {
                reply.error(Errno::EIO);
                return;
            }
            let _ = self.rpc(
                relayfs_protocol::method::TRUNCATE,
                serde_json::json!({ "path": path.to_string_lossy(), "size": size }),
            );
            if let Ok(file) = std::fs::OpenOptions::new().write(true).open(&local) {
                let _ = file.set_len(size);
            }
        }
        if let Some(mode) = mode {
            let _ = self.rpc(
                relayfs_protocol::method::CHMOD,
                serde_json::json!({ "path": path.to_string_lossy(), "mode": mode }),
            );
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let _ = std::fs::set_permissions(&local, std::fs::Permissions::from_mode(mode));
            }
        }
        match self.attr_for(&path) {
            Ok(attr) => reply.attr(&TTL, &attr),
            Err(e) => reply.error(e),
        }
    }

    fn readlink(&self, _req: &Request, ino: INodeNo, reply: ReplyData) {
        let Some(path) = self.path_for(ino) else {
            reply.error(Errno::ENOENT);
            return;
        };
        let local = self.cache_path(&path);
        if let Ok(target) = std::fs::read_link(&local) {
            reply.data(target.to_string_lossy().as_bytes());
            return;
        }
        match self.rpc(
            relayfs_protocol::method::STAT,
            serde_json::json!({ "path": path.to_string_lossy() }),
        ) {
            Ok(result) => {
                let stat: Result<relayfs_protocol::StatResult, Errno> =
                    serde_json::from_value(result).map_err(|_| Errno::EIO);
                match stat {
                    Ok(stat) => match stat.link_target {
                        Some(target) => reply.data(target.as_bytes()),
                        None => reply.error(Errno::EINVAL),
                    },
                    Err(e) => reply.error(e),
                }
            }
            Err(e) => reply.error(e),
        }
    }

    fn mknod(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        _rdev: u32,
        reply: ReplyEntry,
    ) {
        let Some(path) = self.child_path(parent, name) else {
            reply.error(Errno::ENOENT);
            return;
        };
        if self.read_only {
            reply.error(Errno::EROFS);
            return;
        }
        let local = self.cache_path(&path);
        if let Some(p) = local.parent() {
            let _ = std::fs::create_dir_all(p);
        }
        let _ = std::fs::File::create(&local);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&local, std::fs::Permissions::from_mode(mode));
        }
        if self.drain_uploads(&[&path]).is_err() {
            reply.error(Errno::EIO);
            return;
        }
        let _ = self.rpc(
            relayfs_protocol::method::WRITE_FILE,
            serde_json::json!({
                "path": path.to_string_lossy(),
                "data": "",
                "create_dirs": false,
            }),
        );
        match self.attr_for(&path) {
            Ok(attr) => reply.entry(&TTL, &attr, Generation(0)),
            Err(e) => reply.error(e),
        }
    }

    fn mkdir(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        let Some(path) = self.child_path(parent, name) else {
            reply.error(Errno::ENOENT);
            return;
        };
        if self.read_only {
            reply.error(Errno::EROFS);
            return;
        }
        let local = self.cache_path(&path);
        let _ = std::fs::create_dir_all(&local);
        let _ = self.rpc(
            relayfs_protocol::method::MKDIR,
            serde_json::json!({ "path": path.to_string_lossy(), "mode": mode }),
        );
        match self.attr_for(&path) {
            Ok(attr) => reply.entry(&TTL, &attr, Generation(0)),
            Err(e) => reply.error(e),
        }
    }

    fn unlink(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let Some(path) = self.child_path(parent, name) else {
            reply.error(Errno::ENOENT);
            return;
        };
        if self.read_only {
            reply.error(Errno::EROFS);
            return;
        }
        let local = self.cache_path(&path);
        // Deleting with queued writes would discard them; flush first.
        if self.drain_uploads(&[&path]).is_err() {
            reply.error(Errno::EIO);
            return;
        }
        let _ = std::fs::remove_file(&local);
        let _ = self.rpc(
            relayfs_protocol::method::REMOVE,
            serde_json::json!({ "path": path.to_string_lossy(), "recursive": false }),
        );
        reply.ok();
    }

    fn rmdir(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let Some(path) = self.child_path(parent, name) else {
            reply.error(Errno::ENOENT);
            return;
        };
        if self.read_only {
            reply.error(Errno::EROFS);
            return;
        }
        let local = self.cache_path(&path);
        if self.drain_uploads(&[&path]).is_err() {
            reply.error(Errno::EIO);
            return;
        }
        let _ = std::fs::remove_dir(&local);
        let _ = self.rpc(
            relayfs_protocol::method::REMOVE,
            serde_json::json!({ "path": path.to_string_lossy(), "recursive": false }),
        );
        reply.ok();
    }

    fn symlink(
        &self,
        _req: &Request,
        parent: INodeNo,
        link_name: &OsStr,
        target: &Path,
        reply: ReplyEntry,
    ) {
        let Some(path) = self.child_path(parent, link_name) else {
            reply.error(Errno::ENOENT);
            return;
        };
        if self.read_only {
            reply.error(Errno::EROFS);
            return;
        }
        let local = self.cache_path(&path);
        #[cfg(unix)]
        {
            let _ = std::os::unix::fs::symlink(target, &local);
        }
        if self.drain_uploads(&[&path]).is_err() {
            reply.error(Errno::EIO);
            return;
        }
        let _ = self.rpc(
            relayfs_protocol::method::SYMLINK,
            serde_json::json!({
                "link": path.to_string_lossy(),
                "target": target.to_string_lossy(),
            }),
        );
        match self.attr_for(&path) {
            Ok(attr) => reply.entry(&TTL, &attr, Generation(0)),
            Err(e) => reply.error(e),
        }
    }

    fn rename(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        newparent: INodeNo,
        newname: &OsStr,
        _flags: RenameFlags,
        reply: ReplyEmpty,
    ) {
        let (Some(from), Some(to)) = (
            self.child_path(parent, name),
            self.child_path(newparent, newname),
        ) else {
            reply.error(Errno::ENOENT);
            return;
        };
        if self.read_only {
            reply.error(Errno::EROFS);
            return;
        }
        let from_local = self.cache_path(&from);
        let to_local = self.cache_path(&to);
        // A rename must not overtake queued writes to either path.
        if self.drain_uploads(&[&from, &to]).is_err() {
            reply.error(Errno::EIO);
            return;
        }
        if let Some(p) = to_local.parent() {
            let _ = std::fs::create_dir_all(p);
        }
        let _ = std::fs::rename(&from_local, &to_local);
        let _ = self.rpc(
            relayfs_protocol::method::RENAME,
            serde_json::json!({
                "from": from.to_string_lossy(),
                "to": to.to_string_lossy(),
            }),
        );
        reply.ok();
    }

    fn link(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _newparent: INodeNo,
        _newname: &OsStr,
        reply: ReplyEntry,
    ) {
        reply.error(Errno::ENOSYS);
    }

    fn open(&self, _req: &Request, _ino: INodeNo, _flags: OpenFlags, reply: ReplyOpen) {
        reply.opened(FileHandle(0), FopenFlags::FOPEN_KEEP_CACHE);
    }

    fn read(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyData,
    ) {
        let Some(path) = self.path_for(ino) else {
            reply.error(Errno::ENOENT);
            return;
        };
        let local = self.cache_path(&path);

        // 1. If local cache file exists and has sufficient bytes, serve immediately at local SSD speed
        if let Ok(meta) = std::fs::metadata(&local) {
            let file_len = meta.len();
            if file_len > 0 && offset < file_len {
                if let Ok(mut file) = std::fs::File::open(&local) {
                    use std::io::{Read, Seek, SeekFrom};
                    if file.seek(SeekFrom::Start(offset)).is_ok() {
                        let to_read = (size as u64).min(file_len - offset) as usize;
                        let mut buf = vec![0u8; to_read];
                        if let Ok(n) = file.read(&mut buf) {
                            reply.data(&buf[..n]);
                            return;
                        }
                    }
                }
            } else if file_len == 0 && offset == 0 {
                reply.data(&[]);
                return;
            }
        }

        // 2. Fetch from remote in 1 MiB chunks for fast network streaming
        let chunk_size = (size as u64).max(1024 * 1024);
        match self.rpc(
            relayfs_protocol::method::READ_FILE,
            serde_json::json!({
                "path": path.to_string_lossy(),
                "offset": offset,
                "limit": chunk_size,
            }),
        ) {
            Ok(result) => {
                let read: Result<relayfs_protocol::ReadFileResult, Errno> =
                    serde_json::from_value(result).map_err(|_| Errno::EIO);
                match read {
                    Ok(read) => match base64_decode(&read.data) {
                        Ok(bytes) => {
                            if let Some(parent) = local.parent() {
                                let _ = std::fs::create_dir_all(parent);
                            }
                            // Cache the fetch only when it extends the cached
                            // prefix; writing at a far offset would punch a
                            // hole of zeros the cache-hit path could later
                            // serve as real file data.
                            let frontier = std::fs::metadata(&local).map(|m| m.len()).unwrap_or(0);
                            let mut warmed = false;
                            if offset <= frontier {
                                use std::io::{Seek, SeekFrom, Write};
                                if let Ok(mut f) = std::fs::OpenOptions::new()
                                    .create(true)
                                    .write(true)
                                    .truncate(false)
                                    .open(&local)
                                {
                                    if f.seek(SeekFrom::Start(offset)).is_ok()
                                        && f.write_all(&bytes).is_ok()
                                    {
                                        warmed = true;
                                    }
                                }
                            }
                            if warmed {
                                // Sequential read: warm the rest of the file
                                // with a window of concurrent RPCs so the
                                // reader saturates the network.
                                self.kick_prefetch(&path);
                            }
                            let return_len = (size as usize).min(bytes.len());
                            reply.data(&bytes[..return_len]);
                        }
                        Err(_) => reply.error(Errno::EIO),
                    },
                    Err(e) => reply.error(e),
                }
            }
            Err(e) => {
                // Offline fallback: serve whatever bytes exist locally
                if let Ok(mut file) = std::fs::File::open(&local) {
                    use std::io::{Read, Seek, SeekFrom};
                    if file.seek(SeekFrom::Start(offset)).is_ok() {
                        let mut buf = vec![0u8; size as usize];
                        if let Ok(n) = file.read(&mut buf) {
                            reply.data(&buf[..n]);
                            return;
                        }
                    }
                }
                reply.error(e);
            }
        }
    }

    fn write(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        data: &[u8],
        _write_flags: WriteFlags,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyWrite,
    ) {
        let Some(path) = self.path_for(ino) else {
            reply.error(Errno::ENOENT);
            return;
        };
        if self.read_only {
            reply.error(Errno::EROFS);
            return;
        }
        let local = self.cache_path(&path);
        if let Some(p) = local.parent() {
            let _ = std::fs::create_dir_all(p);
        }
        use std::io::{Seek, SeekFrom, Write};
        let mut cached = false;
        if let Ok(mut file) = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&local)
        {
            if file.seek(SeekFrom::Start(offset)).is_ok() && file.write_all(data).is_ok() {
                cached = true;
            }
        }
        if cached {
            // Write-behind: acknowledge at disk speed; the flusher streams
            // the bytes to the target with a window of concurrent RPCs.
            // Backpressure applies once the dirty backlog hits QUEUE_CAP.
            self.enqueue_upload(&path, offset, data.len());
            reply.written(data.len() as u32);
        } else {
            // Local cache unusable (disk full/permissions): fall back to a
            // direct synchronous write so the data still lands remotely.
            let _ = self.rpc(
                relayfs_protocol::method::WRITE_AT,
                serde_json::json!({
                    "path": path.to_string_lossy(),
                    "offset": offset,
                    "data": base64_encode(data),
                }),
            );
            reply.written(data.len() as u32);
        }
    }

    fn flush(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        _lock_owner: LockOwner,
        reply: ReplyEmpty,
    ) {
        // close() must not return before this file's data reached the target.
        match self.path_for(ino) {
            Some(path) => match self.drain_uploads(&[&path]) {
                Ok(()) => reply.ok(),
                Err(e) => reply.error(e),
            },
            None => reply.ok(),
        }
    }

    fn release(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        // Normally already drained by flush; drains anything left (e.g. a
        // close without flush) but never fails the release itself.
        if let Some(path) = self.path_for(ino) {
            let _ = self.drain_uploads(&[&path]);
        }
        reply.ok();
    }

    fn fsync(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        match self.path_for(ino) {
            Some(path) => match self.drain_uploads(&[&path]) {
                Ok(()) => reply.ok(),
                Err(e) => reply.error(e),
            },
            None => reply.ok(),
        }
    }

    fn opendir(&self, _req: &Request, _ino: INodeNo, _flags: OpenFlags, reply: ReplyOpen) {
        reply.opened(FileHandle(0), FopenFlags::empty());
    }

    fn readdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        let Some(path) = self.path_for(ino) else {
            reply.error(Errno::ENOENT);
            return;
        };
        let local_dir = self.cache_path(&path);
        let _ = std::fs::create_dir_all(&local_dir);

        let entries_result = match self.rpc(
            relayfs_protocol::method::LIST_DIR,
            serde_json::json!({ "path": path.to_string_lossy() }),
        ) {
            Ok(result) => {
                let list: Result<relayfs_protocol::ListDirResult, _> =
                    serde_json::from_value(result);
                list.map(|l| {
                    l.entries
                        .into_iter()
                        .map(|e| {
                            let kind = match e.kind {
                                FileKind::File => FileType::RegularFile,
                                FileKind::Dir => FileType::Directory,
                                FileKind::Symlink => FileType::Symlink,
                                FileKind::Other => FileType::RegularFile,
                            };
                            let child_local = local_dir.join(&e.name);
                            if kind == FileType::Directory {
                                let _ = std::fs::create_dir_all(&child_local);
                            }
                            (kind, e.name)
                        })
                        .collect::<Vec<_>>()
                })
                .map_err(|_| Errno::EIO)
            }
            Err(remote_err) => {
                // Offline fallback: list entries from local cache directory
                if let Ok(rd) = std::fs::read_dir(&local_dir) {
                    let mut local_entries = Vec::new();
                    for entry in rd.flatten() {
                        let name = entry.file_name().to_string_lossy().into_owned();
                        let kind = if let Ok(ft) = entry.file_type() {
                            if ft.is_dir() {
                                FileType::Directory
                            } else if ft.is_symlink() {
                                FileType::Symlink
                            } else {
                                FileType::RegularFile
                            }
                        } else {
                            FileType::RegularFile
                        };
                        local_entries.push((kind, name));
                    }
                    Ok(local_entries)
                } else {
                    Err(remote_err)
                }
            }
        };

        match entries_result {
            Ok(entries) => {
                let mut all: Vec<(u64, FileType, String)> = vec![
                    (ino.0, FileType::Directory, ".".into()),
                    (1, FileType::Directory, "..".into()),
                ];
                for (kind, name) in entries {
                    let child_path = path.join(&name);
                    let child_ino = self.ino_for_path(&child_path);
                    all.push((child_ino, kind, name));
                }
                for (i, (child_ino, kind, name)) in all.iter().enumerate() {
                    if (i as u64) < offset {
                        continue;
                    }
                    if reply.add(INodeNo(*child_ino), (i + 1) as u64, *kind, name) {
                        break;
                    }
                }
                reply.ok();
            }
            Err(e) => reply.error(e),
        }
    }

    fn releasedir(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _fh: FileHandle,
        _flags: OpenFlags,
        reply: ReplyEmpty,
    ) {
        reply.ok();
    }

    fn fsyncdir(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _fh: FileHandle,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        reply.ok();
    }

    fn statfs(&self, _req: &Request, _ino: INodeNo, reply: ReplyStatfs) {
        reply.statfs(
            1 << 30, // blocks (512B units) = 512 GiB
            1 << 29, // bfree
            1 << 29, // bavail
            1 << 20, // files
            1 << 20, // ffree
            4096,    // bsize
            255,     // namelen
            4096,    // frsize
        );
    }

    fn setxattr(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _name: &OsStr,
        _value: &[u8],
        _flags: i32,
        _position: u32,
        reply: ReplyEmpty,
    ) {
        reply.error(Errno::ENOSYS);
    }

    fn getxattr(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _name: &OsStr,
        _size: u32,
        reply: ReplyXattr,
    ) {
        reply.error(Errno::ENOSYS);
    }

    fn listxattr(&self, _req: &Request, _ino: INodeNo, size: u32, reply: ReplyXattr) {
        if size == 0 {
            reply.size(0);
        } else {
            reply.data(&[]);
        }
    }

    fn removexattr(&self, _req: &Request, _ino: INodeNo, _name: &OsStr, reply: ReplyEmpty) {
        reply.error(Errno::ENOSYS);
    }

    fn access(&self, _req: &Request, _ino: INodeNo, _mask: AccessFlags, reply: ReplyEmpty) {
        reply.ok();
    }

    fn create(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        _flags: i32,
        reply: ReplyCreate,
    ) {
        let Some(path) = self.child_path(parent, name) else {
            reply.error(Errno::ENOENT);
            return;
        };
        if self.read_only {
            reply.error(Errno::EROFS);
            return;
        }
        let local = self.cache_path(&path);
        if let Some(p) = local.parent() {
            let _ = std::fs::create_dir_all(p);
        }
        let _ = std::fs::File::create(&local);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&local, std::fs::Permissions::from_mode(mode));
        }
        // O_TRUNC/create must not overtake queued writes for this path.
        if self.drain_uploads(&[&path]).is_err() {
            reply.error(Errno::EIO);
            return;
        }
        let _ = self.rpc(
            relayfs_protocol::method::WRITE_FILE,
            serde_json::json!({
                "path": path.to_string_lossy(),
                "data": "",
                "create_dirs": false,
            }),
        );
        let _ = self.rpc(
            relayfs_protocol::method::CHMOD,
            serde_json::json!({ "path": path.to_string_lossy(), "mode": mode }),
        );
        match self.attr_for(&path) {
            Ok(attr) => reply.created(
                &TTL,
                &attr,
                Generation(0),
                FileHandle(0),
                FopenFlags::FOPEN_KEEP_CACHE,
            ),
            Err(e) => reply.error(e),
        }
    }
}

/// Computes the persistent local backing cache directory for a given mount.
fn get_cache_dir(remote_dir: &Path, mount_point: &Path) -> PathBuf {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    std::hash::Hash::hash(&remote_dir.to_string_lossy(), &mut hasher);
    std::hash::Hash::hash(&mount_point.to_string_lossy(), &mut hasher);
    let hash = std::hash::Hasher::finish(&hasher);
    let base = std::env::var("RELAYFS_CACHE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            std::env::var("HOME")
                .map(|h| {
                    PathBuf::from(h)
                        .join(".cache")
                        .join("relayfs")
                        .join("mounts")
                })
                .unwrap_or_else(|_| std::env::temp_dir().join("relayfs-mounts"))
        });
    base.join(format!("{:016x}", hash))
}

/// Asynchronously streams remote folder contents into the local cache in 1 MiB chunks.
async fn background_sync_folder(client: Arc<AgentClient>, remote_dir: String, cache_dir: PathBuf) {
    let mut queue = vec![(remote_dir, cache_dir)];
    while let Some((rem, loc)) = queue.pop() {
        let _ = tokio::fs::create_dir_all(&loc).await;
        let res = client
            .call(
                relayfs_protocol::method::LIST_DIR,
                serde_json::json!({ "path": rem }),
            )
            .await;
        let Ok(val) = res else { break };
        let Ok(list) = serde_json::from_value::<relayfs_protocol::ListDirResult>(val) else {
            continue;
        };
        for entry in list.entries {
            let child_rem = format!("{}/{}", rem.trim_end_matches('/'), entry.name);
            let child_loc = loc.join(&entry.name);
            match entry.kind {
                FileKind::Dir => {
                    queue.push((child_rem, child_loc));
                }
                FileKind::File => {
                    if let Ok(m) = tokio::fs::metadata(&child_loc).await {
                        if m.len() == entry.size {
                            continue;
                        }
                    }
                    let _ = download_to_cache(&client, &child_rem, &child_loc, entry.size).await;
                }
                _ => {}
            }
        }
    }
}

async fn download_to_cache(
    client: &Arc<AgentClient>,
    remote_path: &str,
    local_path: &Path,
    total_size: u64,
) -> anyhow::Result<()> {
    if total_size == 0 {
        let _ = tokio::fs::File::create(local_path).await;
        return Ok(());
    }
    if let Some(p) = local_path.parent() {
        let _ = tokio::fs::create_dir_all(p).await;
    }
    let temp_path = local_path.with_extension(format!("tmp-{:08x}", rand::random::<u32>()));
    let _ = tokio::fs::File::create(&temp_path).await;

    // Fetch chunks with a bounded window of concurrent RPCs: throughput is
    // limited by the network, not by one round trip per chunk.
    let mut tasks = tokio::task::JoinSet::new();
    let mut offset = 0u64;
    let mut failed = false;
    while offset < total_size {
        while tasks.len() >= SYNC_INFLIGHT {
            match tasks.join_next().await {
                Some(Ok(Ok(true))) => {}
                _ => {
                    failed = true;
                    break;
                }
            }
        }
        if failed {
            break;
        }
        let limit = (total_size - offset).min(1024 * 1024);
        let client = client.clone();
        let path = remote_path.to_string();
        let temp = temp_path.clone();
        let start = offset;
        tasks.spawn(async move {
            let res = client
                .call_timeout(
                    relayfs_protocol::method::READ_FILE,
                    serde_json::json!({ "path": path, "offset": start, "limit": limit }),
                    FUSE_TIMEOUT,
                )
                .await
                .map_err(|e| anyhow::anyhow!("{e:?}"))?;
            let read: relayfs_protocol::ReadFileResult = serde_json::from_value(res)?;
            let bytes = base64_decode(&read.data).map_err(|e| anyhow::anyhow!("{e}"))?;
            if bytes.is_empty() {
                return Ok(false);
            }
            tokio::task::spawn_blocking(move || -> std::io::Result<()> {
                use std::io::{Seek, SeekFrom, Write};
                let mut f = std::fs::OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(false)
                    .open(&temp)?;
                f.seek(SeekFrom::Start(start))?;
                f.write_all(&bytes)
            })
            .await??;
            Ok::<bool, anyhow::Error>(true)
        });
        offset += limit;
    }
    while let Some(res) = tasks.join_next().await {
        if !matches!(res, Ok(Ok(true))) {
            failed = true;
        }
    }
    if failed {
        let _ = tokio::fs::remove_file(&temp_path).await;
        anyhow::bail!("download of {remote_path} interrupted");
    }
    tokio::fs::rename(&temp_path, local_path).await?;
    Ok(())
}

/// Manages active mounts: mount point -> background session.
pub struct MountManager {
    client: Arc<AgentClient>,
    mounts: Mutex<HashMap<String, fuser::BackgroundSession>>,
    active_paths: std::sync::Mutex<HashSet<String>>,
}

impl MountManager {
    pub fn new(client: Arc<AgentClient>) -> Self {
        Self {
            client,
            mounts: Mutex::new(HashMap::new()),
            active_paths: std::sync::Mutex::new(HashSet::new()),
        }
    }

    /// Mount `remote_dir` at local `mount_point`.
    pub async fn mount(
        &self,
        remote_dir: &str,
        mount_point: &str,
        read_only: bool,
    ) -> Result<(), String> {
        // Retire any previous session for this mount point BEFORE mounting
        // again: dropping a stale BackgroundSession after the new mount is
        // live would unmount the NEW filesystem (its drop unmounts by path),
        // leaving a plain directory that serves ENOENT for every open.
        if let Some(old) = self.mounts.lock().await.remove(mount_point) {
            let _ = old.umount_and_join();
        }
        // Clean any leftover or broken mount point from previous crashed runs
        let _ = std::process::Command::new("fusermount3")
            .arg("-u")
            .arg("-z")
            .arg(mount_point)
            .output();

        // Verify the remote directory exists.
        let stat = self
            .client
            .call(
                relayfs_protocol::method::STAT,
                serde_json::json!({ "path": remote_dir }),
            )
            .await
            .map_err(|e| e.message.to_string())?;
        let stat: relayfs_protocol::StatResult = serde_json::from_value(stat.clone())
            .map_err(|e| format!("bad stat result {e}: {stat}"))?;
        if stat.kind != FileKind::Dir {
            return Err(format!("{remote_dir} is not a directory"));
        }

        // Create the mount point.
        let mount_path = PathBuf::from(mount_point);
        std::fs::create_dir_all(&mount_path)
            .map_err(|e| format!("create mount point {}: {e}", mount_path.display()))?;

        let cache_dir = get_cache_dir(&PathBuf::from(remote_dir), &mount_path);
        let _ = std::fs::create_dir_all(&cache_dir);

        let fs = RemoteFs::new(
            self.client.clone(),
            PathBuf::from(remote_dir),
            cache_dir.clone(),
            read_only,
        );

        let mut config = Config::default();
        config.mount_options = vec![
            MountOption::FSName("relayfs".into()),
            MountOption::Subtype("relayfs".into()),
            // NOTE: no `auto_cache` here — it is a libfuse-internal option
            // that fusermount3 rejects with "unknown option". Attribute
            // freshness comes from the 1s entry TTL instead.
        ];
        config.acl = SessionACL::Owner;
        config.n_threads = Some(16);
        config.clone_fd = true;

        let session =
            spawn_mount(fs, &mount_path, &config).map_err(|e| format!("mount failed: {e}"))?;

        self.mounts
            .lock()
            .await
            .insert(mount_point.to_string(), session);
        if let Ok(mut paths) = self.active_paths.lock() {
            paths.insert(mount_point.to_string());
        }

        // Background mirror sync
        let client_clone = self.client.clone();
        let remote_dir_str = remote_dir.to_string();
        let cache_dir_clone = cache_dir.clone();
        tokio::spawn(async move {
            background_sync_folder(client_clone, remote_dir_str, cache_dir_clone).await;
        });

        Ok(())
    }

    /// Unmount and join the session.
    pub async fn unmount(&self, mount_point: &str) -> Result<(), String> {
        let mut mounts = self.mounts.lock().await;
        let session = mounts
            .remove(mount_point)
            .ok_or_else(|| format!("no mount at {mount_point}"))?;
        if let Ok(mut paths) = self.active_paths.lock() {
            paths.remove(mount_point);
        }
        session
            .umount_and_join()
            .map_err(|e| format!("unmount failed: {e}"))?;
        let _ = std::process::Command::new("fusermount3")
            .arg("-u")
            .arg("-z")
            .arg(mount_point)
            .output();
        Ok(())
    }

    pub async fn list(&self) -> Vec<String> {
        self.mounts.lock().await.keys().cloned().collect()
    }
}

impl Drop for MountManager {
    fn drop(&mut self) {
        if let Ok(paths) = self.active_paths.lock() {
            for mp in paths.iter() {
                let _ = std::process::Command::new("fusermount3")
                    .arg("-u")
                    .arg("-z")
                    .arg(mp)
                    .output();
            }
        }
    }
}

fn base64_encode(data: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(data)
}

fn base64_decode(data: &str) -> Result<Vec<u8>, String> {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD
        .decode(data)
        .map_err(|e| format!("invalid base64: {e}"))
}
