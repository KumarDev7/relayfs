//! File system operations served by the agent.

use std::path::{Path, PathBuf};

use relayfs_protocol::{
    CopyParams, DirEntry, FileKind, ListDirParams, ListDirResult, MkdirParams, ReadFileParams,
    ReadFileResult, RemoveParams, RenameParams, StatParams, StatResult, WriteFileParams,
    WriteFileResult,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::conn::{WsStream, send_notification};

pub async fn read_file(params: serde_json::Value) -> anyhow::Result<serde_json::Value> {
    let params: ReadFileParams = serde_json::from_value(params)?;
    let path = PathBuf::from(&params.path);
    let mut file = tokio::fs::File::open(&path).await.map_err(|e| {
        anyhow::anyhow!("open {}: {e}", path.display())
    })?;

    let offset = params.offset.unwrap_or(0);
    let limit = params.limit.unwrap_or(1024 * 1024);
    use tokio::io::AsyncSeekExt;
    if offset > 0 {
        file.seek(std::io::SeekFrom::Start(offset)).await?;
    }

    let mut buf = vec![0u8; limit as usize];
    let n = file.read(&mut buf).await?;
    buf.truncate(n);

    Ok(serde_json::to_value(ReadFileResult {
        data: base64_encode(&buf),
        eof: (n as u64) < limit,
    })?)
}

pub async fn write_file(params: serde_json::Value) -> anyhow::Result<serde_json::Value> {
    let params: WriteFileParams = serde_json::from_value(params)?;
    let path = PathBuf::from(&params.path);

    if params.create_dirs.unwrap_or(false) {
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(|e| {
                anyhow::anyhow!("create dirs {}: {e}", parent.display())
            })?;
        }
    }

    let data = base64_decode(&params.data)?;
    let mut file = tokio::fs::File::create(&path).await.map_err(|e| {
        anyhow::anyhow!("create {}: {e}", path.display())
    })?;
    file.write_all(&data).await?;
    file.flush().await?;

    Ok(serde_json::to_value(WriteFileResult {
        bytes_written: data.len() as u64,
    })?)
}

pub async fn list_dir(params: serde_json::Value) -> anyhow::Result<serde_json::Value> {
    let params: ListDirParams = serde_json::from_value(params)?;
    let path = PathBuf::from(&params.path);

    let mut entries = Vec::new();
    let mut rd = tokio::fs::read_dir(&path).await.map_err(|e| {
        anyhow::anyhow!("read_dir {}: {e}", path.display())
    })?;
    while let Some(entry) = rd.next_entry().await? {
        let name = entry.file_name().to_string_lossy().into_owned();
        let meta = match entry.metadata().await {
            Ok(m) => m,
            Err(_) => continue,
        };
        let kind = if meta.is_dir() {
            FileKind::Dir
        } else if meta.is_file() {
            FileKind::File
        } else if meta.file_type().is_symlink() {
            FileKind::Symlink
        } else {
            FileKind::Other
        };
        let modified = meta
            .modified()
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_secs())
            .unwrap_or(0);
        entries.push(DirEntry {
            name,
            kind,
            size: meta.len(),
            modified,
        });
    }
    entries.sort_by(|a, b| a.name.cmp(&b.name));

    Ok(serde_json::to_value(ListDirResult { entries })?)
}

pub async fn stat(params: serde_json::Value) -> anyhow::Result<serde_json::Value> {
    let params: StatParams = serde_json::from_value(params)?;
    let path = PathBuf::from(&params.path);

    let meta = tokio::fs::symlink_metadata(&path).await.map_err(|e| {
        anyhow::anyhow!("stat {}: {e}", path.display())
    })?;

    let kind = if meta.is_dir() {
        FileKind::Dir
    } else if meta.is_file() {
        FileKind::File
    } else if meta.file_type().is_symlink() {
        FileKind::Symlink
    } else {
        FileKind::Other
    };

    let link_target = if kind == FileKind::Symlink {
        tokio::fs::read_link(&path).await.ok().map(|p| p.to_string_lossy().into_owned())
    } else {
        None
    };

    #[cfg(unix)]
    let (mode, uid, gid) = {
        use std::os::unix::fs::MetadataExt;
        (meta.mode(), meta.uid(), meta.gid())
    };
    #[cfg(not(unix))]
    let (mode, uid, gid) = (0, 0, 0);

    let modified = meta
        .modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_secs())
        .unwrap_or(0);

    Ok(serde_json::to_value(StatResult {
        kind,
        size: meta.len(),
        modified,
        mode,
        uid,
        gid,
        link_target,
    })?)
}

pub async fn mkdir(params: serde_json::Value) -> anyhow::Result<serde_json::Value> {
    let params: MkdirParams = serde_json::from_value(params)?;
    let path = PathBuf::from(&params.path);
    let mode = params.mode.unwrap_or(0o755);

    #[cfg(unix)]
    {
        let mut builder = tokio::fs::DirBuilder::new();
        builder.mode(mode);
        builder.create(&path).await.map_err(|e| {
            anyhow::anyhow!("mkdir {}: {e}", path.display())
        })?;
    }
    #[cfg(not(unix))]
    {
        tokio::fs::create_dir(&path).await.map_err(|e| {
            anyhow::anyhow!("mkdir {}: {e}", path.display())
        })?;
    }
    Ok(serde_json::Value::Null)
}

pub async fn remove(params: serde_json::Value) -> anyhow::Result<serde_json::Value> {
    let params: RemoveParams = serde_json::from_value(params)?;
    let path = PathBuf::from(&params.path);

    if params.recursive.unwrap_or(false) {
        tokio::fs::remove_dir_all(&path).await.map_err(|e| {
            anyhow::anyhow!("remove {}: {e}", path.display())
        })?;
    } else {
        let meta = tokio::fs::symlink_metadata(&path).await.map_err(|e| {
            anyhow::anyhow!("remove {}: {e}", path.display())
        })?;
        if meta.is_dir() {
            tokio::fs::remove_dir(&path).await.map_err(|e| {
                anyhow::anyhow!("remove {}: {e}", path.display())
            })?;
        } else {
            tokio::fs::remove_file(&path).await.map_err(|e| {
                anyhow::anyhow!("remove {}: {e}", path.display())
            })?;
        }
    }
    Ok(serde_json::Value::Null)
}

pub async fn rename(params: serde_json::Value) -> anyhow::Result<serde_json::Value> {
    let params: RenameParams = serde_json::from_value(params)?;
    tokio::fs::rename(&params.from, &params.to).await.map_err(|e| {
        anyhow::anyhow!("rename {} -> {}: {e}", params.from, params.to)
    })?;
    Ok(serde_json::Value::Null)
}

pub async fn write_at(params: serde_json::Value) -> anyhow::Result<serde_json::Value> {
    let params: relayfs_protocol::WriteAtParams = serde_json::from_value(params)?;
    let path = PathBuf::from(&params.path);
    let data = base64_decode(&params.data)?;

    use tokio::io::{AsyncSeekExt, AsyncWriteExt};
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .await
        .map_err(|e| anyhow::anyhow!("open {}: {e}", path.display()))?;
    file.seek(std::io::SeekFrom::Start(params.offset)).await?;
    file.write_all(&data).await?;
    file.flush().await?;
    Ok(serde_json::json!({ "bytes_written": data.len() }))
}

pub async fn truncate(params: serde_json::Value) -> anyhow::Result<serde_json::Value> {
    let params: relayfs_protocol::TruncateParams = serde_json::from_value(params)?;
    let path = PathBuf::from(&params.path);
    tokio::fs::File::options()
        .write(true)
        .open(&path)
        .await
        .map_err(|e| anyhow::anyhow!("open {}: {e}", path.display()))?
        .set_len(params.size)
        .await
        .map_err(|e| anyhow::anyhow!("truncate {}: {e}", path.display()))?;
    Ok(serde_json::Value::Null)
}

pub async fn symlink(params: serde_json::Value) -> anyhow::Result<serde_json::Value> {
    let params: relayfs_protocol::SymlinkParams = serde_json::from_value(params)?;
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(&params.target, &params.link).map_err(|e| {
            anyhow::anyhow!("symlink {} -> {}: {e}", params.link, params.target)
        })?;
    }
    #[cfg(not(unix))]
    {
        return Err(anyhow::anyhow!("symlinks not supported on this platform"));
    }
    Ok(serde_json::Value::Null)
}

pub async fn chmod(params: serde_json::Value) -> anyhow::Result<serde_json::Value> {
    let params: relayfs_protocol::ChmodParams = serde_json::from_value(params)?;
    let path = PathBuf::from(&params.path);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        tokio::fs::set_permissions(&path, std::fs::Permissions::from_mode(params.mode))
            .await
            .map_err(|e| anyhow::anyhow!("chmod {}: {e}", path.display()))?;
    }
    #[cfg(not(unix))]
    {
        return Err(anyhow::anyhow!("chmod not supported on this platform"));
    }
    Ok(serde_json::Value::Null)
}

pub async fn copy(params: serde_json::Value) -> anyhow::Result<serde_json::Value> {
    let params: CopyParams = serde_json::from_value(params)?;
    let from = PathBuf::from(&params.from);
    let to = PathBuf::from(&params.to);

    let meta = tokio::fs::symlink_metadata(&from).await.map_err(|e| {
        anyhow::anyhow!("copy {}: {e}", from.display())
    })?;

    if meta.is_dir() {
        if !params.recursive.unwrap_or(false) {
            return Err(anyhow::anyhow!(
                "{} is a directory; pass recursive=true to copy it",
                from.display()
            ));
        }
        copy_dir_recursive(&from, &to)?;
    } else {
        if let Some(parent) = to.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::copy(&from, &to).map_err(|e| {
            anyhow::anyhow!("copy {} -> {}: {e}", from.display(), to.display())
        })?;
    }
    Ok(serde_json::Value::Null)
}

fn copy_dir_recursive(from: &Path, to: &Path) -> anyhow::Result<()> {
    std::fs::create_dir_all(to)?;
    for entry in std::fs::read_dir(from)? {
        let entry = entry?;
        let src = entry.path();
        let dst = to.join(entry.file_name());
        let meta = entry.metadata()?;
        if meta.is_dir() {
            copy_dir_recursive(&src, &dst)?;
        } else {
            std::fs::copy(&src, &dst)?;
        }
    }
    Ok(())
}

/// Stream a file's contents in chunks as `stream_chunk` notifications.
pub async fn stream_file(
    ws: &mut WsStream,
    id: u64,
    params: serde_json::Value,
) -> anyhow::Result<serde_json::Value> {
    let params: ReadFileParams = serde_json::from_value(params)?;
    let path = PathBuf::from(&params.path);
    let mut file = tokio::fs::File::open(&path).await.map_err(|e| {
        anyhow::anyhow!("open {}: {e}", path.display())
    })?;

    use tokio::io::AsyncReadExt;
    let mut buf = vec![0u8; 256 * 1024];
    let mut total = 0u64;
    loop {
        let n = file.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        total += n as u64;
        send_notification(
            ws,
            "stream_chunk",
            serde_json::json!({
                "id": id,
                "data": base64_encode(&buf[..n]),
            }),
        )
        .await?;
    }
    send_notification(
        ws,
        "stream_end",
        serde_json::json!({ "id": id, "total": total }),
    )
    .await?;
    Ok(serde_json::Value::Null)
}

fn base64_encode(data: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(data)
}

fn base64_decode(data: &str) -> anyhow::Result<Vec<u8>> {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD
        .decode(data)
        .map_err(|e| anyhow::anyhow!("invalid base64: {e}"))
}
