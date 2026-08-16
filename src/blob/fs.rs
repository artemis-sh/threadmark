//! Filesystem object storage for single-node deployments.
//!
//! Objects live at `$BLOB_DIR/<storage_key>`. Writes go to a temporary file in
//! the destination directory and are then renamed into place, so a partially
//! written object is never visible under its final name.
//!
//! This backend does not support direct browser upload or presigned download.
//! Both are advertised as unavailable rather than emulated; see the capability
//! matrix in the local-storage-backend RFC.

use std::path::{Path, PathBuf};

use anyhow::Context;
use tokio::io::AsyncWriteExt;

/// Filesystem-backed object storage rooted at a single directory.
#[derive(Clone)]
pub struct FsStore {
    root: PathBuf,
}

impl FsStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// Verify the root exists and is writable.
    pub async fn ensure_ready(&self) -> anyhow::Result<()> {
        tokio::fs::create_dir_all(&self.root)
            .await
            .with_context(|| format!("create blob directory {}", self.root.display()))?;
        let probe = self.root.join(".threadmark-write-probe");
        tokio::fs::write(&probe, b"")
            .await
            .with_context(|| format!("blob directory {} is not writable", self.root.display()))?;
        tokio::fs::remove_file(&probe).await.ok();
        Ok(())
    }

    pub async fn ping(&self) -> anyhow::Result<()> {
        tokio::fs::metadata(&self.root)
            .await
            .with_context(|| format!("stat blob directory {}", self.root.display()))?;
        Ok(())
    }

    /// Resolve a storage key to an absolute path under the root.
    ///
    /// Storage keys are built from the tenant and principal, which in
    /// trusted-header mode come straight from request headers. They are treated
    /// as hostile input: each component is checked against an allow-list, so a
    /// key can neither escape the root nor address a device or dotfile.
    fn path(&self, key: &str) -> anyhow::Result<PathBuf> {
        let mut path = self.root.clone();
        let mut components = 0;
        for component in key.split('/') {
            anyhow::ensure!(!component.is_empty(), "storage key has an empty component");
            anyhow::ensure!(
                component != "." && component != "..",
                "storage key has a relative component"
            );
            anyhow::ensure!(
                !component.starts_with('.'),
                "storage key component starts with a dot"
            );
            anyhow::ensure!(
                component
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | ':')),
                "storage key component has unsupported characters"
            );
            path.push(component);
            components += 1;
        }
        anyhow::ensure!(components > 0, "storage key is empty");
        Ok(path)
    }

    pub async fn put(&self, key: &str, bytes: Vec<u8>, _content_type: &str) -> anyhow::Result<()> {
        let path = self.path(key)?;
        let parent = path
            .parent()
            .context("storage key has no parent directory")?
            .to_owned();
        tokio::fs::create_dir_all(&parent)
            .await
            .with_context(|| format!("create {}", parent.display()))?;

        // Write to a sibling temporary file and rename, so a reader never sees a
        // partially written object under the final name.
        let temporary = parent.join(format!(".tmp-{}", crate::ids::new_id("blob")));
        let mut file = tokio::fs::File::create(&temporary)
            .await
            .with_context(|| format!("create {}", temporary.display()))?;
        if let Err(error) = file.write_all(&bytes).await {
            drop(file);
            tokio::fs::remove_file(&temporary).await.ok();
            return Err(error).context("write object bytes");
        }
        // Flush the bytes before the rename publishes the name, so a crash
        // cannot leave a visible but empty object.
        file.sync_all().await.context("sync object bytes")?;
        drop(file);
        tokio::fs::rename(&temporary, &path)
            .await
            .with_context(|| format!("publish {}", path.display()))?;
        Ok(())
    }

    pub async fn get(&self, key: &str) -> anyhow::Result<Vec<u8>> {
        let path = self.path(key)?;
        tokio::fs::read(&path)
            .await
            .with_context(|| format!("read {}", path.display()))
    }

    pub async fn open(&self, key: &str) -> anyhow::Result<tokio::fs::File> {
        let path = self.path(key)?;
        tokio::fs::File::open(&path)
            .await
            .with_context(|| format!("open {}", path.display()))
    }

    pub async fn delete(&self, key: &str) -> anyhow::Result<()> {
        let path = self.path(key)?;
        match tokio::fs::remove_file(&path).await {
            Ok(()) => {}
            // Deletion is retried from a durable outbox, so an already-removed
            // object is success rather than an error.
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error).context("delete object"),
        }
        prune_empty_parents(&self.root, &path).await;
        Ok(())
    }

    pub async fn size(&self, key: &str) -> anyhow::Result<Option<u64>> {
        let path = self.path(key)?;
        match tokio::fs::metadata(&path).await {
            Ok(metadata) => Ok(Some(metadata.len())),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error).context("stat object"),
        }
    }
}

/// Remove directories left empty by a deletion, stopping at the root.
///
/// Without this, per-tenant and per-owner directories accumulate indefinitely.
async fn prune_empty_parents(root: &Path, path: &Path) {
    let mut current = path.parent();
    while let Some(directory) = current {
        if directory == root || !directory.starts_with(root) {
            return;
        }
        if tokio::fs::remove_dir(directory).await.is_err() {
            return;
        }
        current = directory.parent();
    }
}

#[cfg(test)]
mod tests {
    use super::FsStore;

    fn store() -> (tempdir::TempDir, FsStore) {
        let dir = tempdir::TempDir::new("threadmark-blob").unwrap();
        let store = FsStore::new(dir.path());
        (dir, store)
    }

    #[tokio::test]
    async fn round_trips_an_object() {
        let (_dir, store) = store();
        store.ensure_ready().await.unwrap();
        store
            .put("tenant/owner/file_1", b"hello".to_vec(), "text/plain")
            .await
            .unwrap();
        assert_eq!(store.get("tenant/owner/file_1").await.unwrap(), b"hello");
        assert_eq!(store.size("tenant/owner/file_1").await.unwrap(), Some(5));
    }

    #[tokio::test]
    async fn rejects_keys_that_escape_the_root() {
        let (_dir, store) = store();
        for key in [
            "../escape",
            "tenant/../../escape",
            "tenant//file",
            "/absolute",
            "tenant/./file",
            "",
        ] {
            assert!(
                store.path(key).is_err(),
                "key {key:?} should have been rejected"
            );
        }
    }

    #[tokio::test]
    async fn rejects_hidden_and_exotic_components() {
        let (_dir, store) = store();
        for key in [
            ".hidden/file",
            "tenant/.ssh",
            "tenant/file name",
            "tenant/file\0",
            "tenant/../file",
        ] {
            assert!(
                store.path(key).is_err(),
                "key {key:?} should have been rejected"
            );
        }
    }

    #[tokio::test]
    async fn accepts_the_keys_threadmark_generates() {
        let (_dir, store) = store();
        // Mirrors `format!("{tenant}/{principal}/{id}")` from files.rs, including
        // principals that look like `user:123` from a JWT subject.
        for key in [
            "tenant-a/user_1/file_01m05n002v8mqa68qc107hrdmx",
            "tenant.a/user:123/file_x",
            "t/u/uploads-staging.file_1",
        ] {
            assert!(store.path(key).is_ok(), "key {key:?} should be accepted");
        }
    }

    #[tokio::test]
    async fn deleting_is_idempotent_and_prunes_directories() {
        let (dir, store) = store();
        store.ensure_ready().await.unwrap();
        store
            .put("tenant/owner/file_1", b"x".to_vec(), "text/plain")
            .await
            .unwrap();
        store.delete("tenant/owner/file_1").await.unwrap();
        // Second delete succeeds: the outbox retries and must not stall.
        store.delete("tenant/owner/file_1").await.unwrap();
        assert_eq!(store.size("tenant/owner/file_1").await.unwrap(), None);
        assert!(
            !dir.path().join("tenant").exists(),
            "emptied directories should be pruned"
        );
        assert!(dir.path().exists(), "the root itself must survive");
    }

    #[tokio::test]
    async fn overwriting_replaces_bytes_atomically() {
        let (_dir, store) = store();
        store.ensure_ready().await.unwrap();
        store
            .put("t/u/f", b"first".to_vec(), "text/plain")
            .await
            .unwrap();
        store
            .put("t/u/f", b"second-and-longer".to_vec(), "text/plain")
            .await
            .unwrap();
        assert_eq!(store.get("t/u/f").await.unwrap(), b"second-and-longer");
    }

    #[tokio::test]
    async fn leaves_no_temporary_files_behind() {
        let (dir, store) = store();
        store.ensure_ready().await.unwrap();
        store
            .put("t/u/f", b"bytes".to_vec(), "text/plain")
            .await
            .unwrap();
        let mut entries = tokio::fs::read_dir(dir.path().join("t/u")).await.unwrap();
        let mut names = Vec::new();
        while let Some(entry) = entries.next_entry().await.unwrap() {
            names.push(entry.file_name().to_string_lossy().into_owned());
        }
        assert_eq!(names, vec!["f".to_string()]);
    }
}
