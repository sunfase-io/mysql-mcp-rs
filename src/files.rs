use std::{
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(1);

pub struct AtomicTarget {
    final_path: PathBuf,
    temp_path: PathBuf,
    overwrite: bool,
    committed: bool,
}

impl AtomicTarget {
    pub async fn create_async(
        path: &str,
        overwrite: bool,
    ) -> anyhow::Result<(Self, tokio::fs::File)> {
        let final_path = PathBuf::from(path);
        anyhow::ensure!(final_path.is_absolute(), "输出路径必须是绝对路径: {path}");
        anyhow::ensure!(
            overwrite || !tokio::fs::try_exists(&final_path).await?,
            "目标文件已存在；如需替换请显式传 overwrite=true: {path}"
        );
        let parent = final_path
            .parent()
            .ok_or_else(|| anyhow::anyhow!("输出路径没有父目录: {path}"))?;
        let metadata = tokio::fs::metadata(parent).await?;
        anyhow::ensure!(metadata.is_dir(), "输出目录不存在: {}", parent.display());
        let name = final_path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| anyhow::anyhow!("输出文件名不是合法 UTF-8"))?;
        let temp_path = parent.join(format!(
            ".{name}.mysql-mcp-{}-{}.tmp",
            std::process::id(),
            TEMP_COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        let file = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp_path)
            .await?;
        Ok((
            Self {
                final_path,
                temp_path,
                overwrite,
                committed: false,
            },
            file,
        ))
    }

    pub async fn commit_async(mut self, file: tokio::fs::File) -> anyhow::Result<PathBuf> {
        file.sync_all().await?;
        drop(file);
        if self.overwrite {
            #[cfg(not(windows))]
            tokio::fs::rename(&self.temp_path, &self.final_path).await?;

            #[cfg(windows)]
            {
                let source = self.temp_path.clone();
                let target = self.final_path.clone();
                tokio::task::spawn_blocking(move || atomic_replace(&source, &target)).await??;
            }
        } else {
            tokio::fs::hard_link(&self.temp_path, &self.final_path).await?;
            tokio::fs::remove_file(&self.temp_path).await?;
        }
        self.committed = true;
        Ok(self.final_path.clone())
    }
}

#[cfg(windows)]
fn atomic_replace(source: &Path, target: &Path) -> std::io::Result<()> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::{
        MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW,
    };

    let source: Vec<u16> = source.as_os_str().encode_wide().chain(Some(0)).collect();
    let target: Vec<u16> = target.as_os_str().encode_wide().chain(Some(0)).collect();
    // SAFETY: 两个 UTF-16 缓冲区均以 NUL 结尾，并在调用期间保持有效。
    let succeeded = unsafe {
        MoveFileExW(
            source.as_ptr(),
            target.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    };
    if succeeded == 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(())
    }
}

impl Drop for AtomicTarget {
    fn drop(&mut self) {
        if !self.committed {
            let _ = std::fs::remove_file(&self.temp_path);
        }
    }
}

pub fn validate_export_paths(paths: &[PathBuf]) -> anyhow::Result<()> {
    let mut seen = std::collections::HashSet::new();
    for path in paths {
        let key = path.to_string_lossy().to_lowercase();
        anyhow::ensure!(
            seen.insert(key),
            "多个对象映射到同一导出路径: {}",
            path.display()
        );
    }
    Ok(())
}

pub fn export_path(
    root: &Path,
    database: &str,
    object_type: &str,
    name: &str,
) -> anyhow::Result<PathBuf> {
    let database = safe_component("database", database)?;
    let name = safe_component("name", name)?;
    let extension = match object_type.to_ascii_uppercase().as_str() {
        "TABLE" => "table.sql",
        "VIEW" => "view.sql",
        "PROCEDURE" => "procedure.sql",
        "FUNCTION" => "function.sql",
        "TRIGGER" => "trigger.sql",
        "EVENT" => "event.sql",
        other => anyhow::bail!("不支持的对象类型: {other}"),
    };
    Ok(root
        .join(&database)
        .join(object_type.to_ascii_lowercase())
        .join(format!("{name}.{extension}")))
}

fn safe_component(label: &str, value: &str) -> anyhow::Result<String> {
    anyhow::ensure!(!value.is_empty(), "{label} 不能为空");
    anyhow::ensure!(!value.contains('\0'), "{label} 不能包含 NUL");
    let mut encoded = String::with_capacity(value.len());
    for byte in value.as_bytes() {
        if byte.is_ascii_alphanumeric() || matches!(*byte, b'_' | b'$' | b'-') {
            encoded.push(char::from(*byte));
        } else {
            use std::fmt::Write as _;
            write!(encoded, "%{byte:02X}").expect("写入 String 不会失败");
        }
    }
    Ok(encoded)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    #[tokio::test]
    async fn atomic_target_does_not_overwrite_by_default() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("data.jsonl");
        tokio::fs::write(&path, "old").await.unwrap();
        assert!(
            AtomicTarget::create_async(path.to_str().unwrap(), false)
                .await
                .is_err()
        );
        assert_eq!(tokio::fs::read_to_string(path).await.unwrap(), "old");
    }

    #[tokio::test]
    async fn atomic_target_publishes_complete_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("data.jsonl");
        let (target, mut file) = AtomicTarget::create_async(path.to_str().unwrap(), false)
            .await
            .unwrap();
        file.write_all(b"{\"id\":1}\n").await.unwrap();
        target.commit_async(file).await.unwrap();
        assert_eq!(tokio::fs::read(path).await.unwrap(), b"{\"id\":1}\n");
    }

    #[tokio::test]
    async fn atomic_target_replaces_only_when_explicitly_allowed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("data.csv");
        tokio::fs::write(&path, b"old").await.unwrap();
        let (target, mut file) = AtomicTarget::create_async(path.to_str().unwrap(), true)
            .await
            .unwrap();
        file.write_all(b"new").await.unwrap();
        target.commit_async(file).await.unwrap();
        assert_eq!(tokio::fs::read(path).await.unwrap(), b"new");
    }

    #[test]
    fn detects_case_insensitive_object_path_conflicts() {
        let paths = vec![
            PathBuf::from("db/table/A.sql"),
            PathBuf::from("DB/table/a.sql"),
        ];
        assert!(validate_export_paths(&paths).is_err());
    }

    #[test]
    fn encodes_object_path_traversal_and_unicode() {
        let path = export_path(Path::new("C:/tmp"), "数据库", "TABLE", "../订单").unwrap();
        let relative = path.strip_prefix("C:/tmp").unwrap().to_string_lossy();
        assert!(!relative.contains(".."));
        assert!(relative.contains("%"));
    }
}
