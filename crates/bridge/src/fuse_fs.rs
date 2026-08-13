//! FUSE filesystem on the bridge machine.
//!
//! `mount_remote` mounts a remote directory into the local filesystem. Every
//! kernel operation is translated into an RPC to the agent. The mount is a
//! *true* kernel mount (via `/dev/fuse`), so local tools (`ls`, editors, build
//! tools) see the remote directory as a normal local path.

use std::collections::HashMap;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use fuser::{
    spawn_mount, AccessFlags, Config, Errno, FileAttr, FileHandle, FileType, Filesystem,
    FopenFlags, Generation, INodeNo, LockOwner, MountOption, OpenFlags, RenameFlags, ReplyAttr,
    ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyStatfs,
    ReplyWrite, ReplyXattr, Request, SessionACL, TimeOrNow, WriteFlags,
};
use relayfs_protocol::{FileKind, RpcError};
use tokio::sync::Mutex;

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

/// The FUSE filesystem backed by the remote agent.
pub struct RemoteFs {
    client: Arc<AgentClient>,
    rt: tokio::runtime::Runtime,
    read_only: bool,
    inodes: Mutex<InodeMap>,
}

impl RemoteFs {
    pub fn new(client: Arc<AgentClient>, remote_dir: PathBuf, read_only: bool) -> Self {
        let inodes = Mutex::new(InodeMap::new(remote_dir));
        Self {
            client,
            rt: tokio::runtime::Runtime::new().expect("fuse runtime"),
            read_only,
            inodes,
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

    /// RPC with a timeout; maps errors to Errno.
    fn rpc(&self, method: &str, params: serde_json::Value) -> Result<serde_json::Value, Errno> {
        // Transparency: every kernel operation on the mount becomes an RPC to
        // the target — log it so mount activity is visible in bridge logs.
        tracing::info!("fuse {method}: {}", params);
        self.rt
            .block_on(self.client.call_timeout(method, params, FUSE_TIMEOUT))
            .map_err(|e| errno_for(&e))
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
        // Timeouts, offline agent, and anything unrecognized surface as a
        // generic filesystem error so editors show a save/read failure.
        Errno::EIO
    }
}

impl Filesystem for RemoteFs {
    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let Some(path) = self.child_path(parent, name) else {
            reply.error(Errno::ENOENT);
            return;
        };
        match self.remote_attr(&path) {
            Ok(attr) => reply.entry(&TTL, &attr, Generation(0)),
            Err(e) => reply.error(e),
        }
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        let Some(path) = self.path_for(ino) else {
            reply.error(Errno::ENOENT);
            return;
        };
        match self.remote_attr(&path) {
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
        if let Some(size) = size {
            if let Err(e) = self.rpc(
                relayfs_protocol::method::TRUNCATE,
                serde_json::json!({ "path": path.to_string_lossy(), "size": size }),
            ) {
                reply.error(e);
                return;
            }
        }
        if let Some(mode) = mode {
            if let Err(e) = self.rpc(
                relayfs_protocol::method::CHMOD,
                serde_json::json!({ "path": path.to_string_lossy(), "mode": mode }),
            ) {
                reply.error(e);
                return;
            }
        }
        match self.remote_attr(&path) {
            Ok(attr) => reply.attr(&TTL, &attr),
            Err(e) => reply.error(e),
        }
    }

    fn readlink(&self, _req: &Request, ino: INodeNo, reply: ReplyData) {
        let Some(path) = self.path_for(ino) else {
            reply.error(Errno::ENOENT);
            return;
        };
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
        // Create an empty regular file.
        if let Err(e) = self.rpc(
            relayfs_protocol::method::WRITE_FILE,
            serde_json::json!({
                "path": path.to_string_lossy(),
                "data": "",
                "create_dirs": false,
            }),
        ) {
            reply.error(e);
            return;
        }
        if mode & 0o170000 == 0o040000 {
            // FIFO etc. — treat as regular file; mode fixup below.
        }
        let _ = mode;
        match self.remote_attr(&path) {
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
        if let Err(e) = self.rpc(
            relayfs_protocol::method::MKDIR,
            serde_json::json!({ "path": path.to_string_lossy(), "mode": mode }),
        ) {
            reply.error(e);
            return;
        }
        match self.remote_attr(&path) {
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
        match self.rpc(
            relayfs_protocol::method::REMOVE,
            serde_json::json!({ "path": path.to_string_lossy(), "recursive": false }),
        ) {
            Ok(_) => reply.ok(),
            Err(e) => reply.error(e),
        }
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
        match self.rpc(
            relayfs_protocol::method::REMOVE,
            serde_json::json!({ "path": path.to_string_lossy(), "recursive": false }),
        ) {
            Ok(_) => reply.ok(),
            Err(e) => reply.error(e),
        }
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
        if let Err(e) = self.rpc(
            relayfs_protocol::method::SYMLINK,
            serde_json::json!({
                "link": path.to_string_lossy(),
                "target": target.to_string_lossy(),
            }),
        ) {
            reply.error(e);
            return;
        }
        match self.remote_attr(&path) {
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
        match self.rpc(
            relayfs_protocol::method::RENAME,
            serde_json::json!({
                "from": from.to_string_lossy(),
                "to": to.to_string_lossy(),
            }),
        ) {
            Ok(_) => reply.ok(),
            Err(e) => reply.error(e),
        }
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
        reply.opened(FileHandle(0), FopenFlags::empty());
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
        match self.rpc(
            relayfs_protocol::method::READ_FILE,
            serde_json::json!({
                "path": path.to_string_lossy(),
                "offset": offset,
                "limit": size,
            }),
        ) {
            Ok(result) => {
                let read: Result<relayfs_protocol::ReadFileResult, Errno> =
                    serde_json::from_value(result).map_err(|_| Errno::EIO);
                match read {
                    Ok(read) => match base64_decode(&read.data) {
                        Ok(bytes) => reply.data(&bytes),
                        Err(_) => reply.error(Errno::EIO),
                    },
                    Err(e) => reply.error(e),
                }
            }
            Err(e) => reply.error(e),
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
        match self.rpc(
            relayfs_protocol::method::WRITE_AT,
            serde_json::json!({
                "path": path.to_string_lossy(),
                "offset": offset,
                "data": base64_encode(data),
            }),
        ) {
            Ok(_) => reply.written(data.len() as u32),
            Err(e) => reply.error(e),
        }
    }

    fn flush(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _fh: FileHandle,
        _lock_owner: LockOwner,
        reply: ReplyEmpty,
    ) {
        reply.ok();
    }

    fn release(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _fh: FileHandle,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        reply.ok();
    }

    fn fsync(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _fh: FileHandle,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        reply.ok();
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
        match self.rpc(
            relayfs_protocol::method::LIST_DIR,
            serde_json::json!({ "path": path.to_string_lossy() }),
        ) {
            Ok(result) => {
                let list: Result<relayfs_protocol::ListDirResult, Errno> =
                    serde_json::from_value(result).map_err(|_| Errno::EIO);
                match list {
                    Ok(list) => {
                        let mut entries: Vec<(u64, FileType, String)> = Vec::new();
                        for entry in &list.entries {
                            let kind = match entry.kind {
                                FileKind::File => FileType::RegularFile,
                                FileKind::Dir => FileType::Directory,
                                FileKind::Symlink => FileType::Symlink,
                                FileKind::Other => FileType::RegularFile,
                            };
                            entries.push((0, kind, entry.name.clone()));
                        }
                        // Register inodes for all children.
                        let mut inodes = self.rt.block_on(async { self.inodes.lock().await });
                        for (_, _, name) in &entries {
                            let child = path.join(name);
                            inodes.ino_for(&child);
                        }
                        drop(inodes);

                        // "." and ".." first.
                        let mut all: Vec<(u64, FileType, String)> = vec![
                            (ino.0, FileType::Directory, ".".into()),
                            (1, FileType::Directory, "..".into()),
                        ];
                        for (_, kind, name) in entries {
                            let child = path.join(&name);
                            let child_ino = self
                                .rt
                                .block_on(async { self.inodes.lock().await.ino_for(&child) });
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
        // Static generous values; the remote filesystem is not queried.
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
        if let Err(e) = self.rpc(
            relayfs_protocol::method::WRITE_FILE,
            serde_json::json!({
                "path": path.to_string_lossy(),
                "data": "",
                "create_dirs": false,
            }),
        ) {
            reply.error(e);
            return;
        }
        if let Err(e) = self.rpc(
            relayfs_protocol::method::CHMOD,
            serde_json::json!({ "path": path.to_string_lossy(), "mode": mode }),
        ) {
            reply.error(e);
            return;
        }
        match self.remote_attr(&path) {
            Ok(attr) => reply.created(
                &TTL,
                &attr,
                Generation(0),
                FileHandle(0),
                FopenFlags::empty(),
            ),
            Err(e) => reply.error(e),
        }
    }
}

/// Manages active mounts: mount point -> background session.
pub struct MountManager {
    client: Arc<AgentClient>,
    mounts: Mutex<HashMap<String, fuser::BackgroundSession>>,
}

impl MountManager {
    pub fn new(client: Arc<AgentClient>) -> Self {
        Self {
            client,
            mounts: Mutex::new(HashMap::new()),
        }
    }

    /// Mount `remote_dir` at local `mount_point`.
    pub async fn mount(
        &self,
        remote_dir: &str,
        mount_point: &str,
        read_only: bool,
    ) -> Result<(), String> {
        // Verify the remote directory exists.
        let stat = self
            .client
            .call(
                relayfs_protocol::method::STAT,
                serde_json::json!({ "path": remote_dir }),
            )
            .await
            .map_err(|e| e.message.to_string())?;
        let stat: relayfs_protocol::StatResult =
            serde_json::from_value(stat).map_err(|e| format!("bad stat result: {e}"))?;
        if stat.kind != FileKind::Dir {
            return Err(format!("{remote_dir} is not a directory"));
        }

        // Create the mount point.
        let mount_path = PathBuf::from(mount_point);
        std::fs::create_dir_all(&mount_path)
            .map_err(|e| format!("create mount point {}: {e}", mount_path.display()))?;

        let fs = RemoteFs::new(self.client.clone(), PathBuf::from(remote_dir), read_only);

        let mut config = Config::default();
        config.mount_options = vec![
            MountOption::FSName("relayfs".into()),
            MountOption::Subtype("relayfs".into()),
        ];
        config.acl = SessionACL::Owner;
        config.n_threads = Some(4);
        config.clone_fd = true;

        let session =
            spawn_mount(fs, &mount_path, &config).map_err(|e| format!("mount failed: {e}"))?;

        self.mounts
            .lock()
            .await
            .insert(mount_point.to_string(), session);
        Ok(())
    }

    /// Unmount and join the session.
    pub async fn unmount(&self, mount_point: &str) -> Result<(), String> {
        let mut mounts = self.mounts.lock().await;
        let session = mounts
            .remove(mount_point)
            .ok_or_else(|| format!("no mount at {mount_point}"))?;
        session
            .umount_and_join()
            .map_err(|e| format!("unmount failed: {e}"))?;
        Ok(())
    }

    pub async fn list(&self) -> Vec<String> {
        self.mounts.lock().await.keys().cloned().collect()
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
