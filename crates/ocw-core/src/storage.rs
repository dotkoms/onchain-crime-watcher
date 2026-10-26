use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::Path,
};

pub fn private_dir(path: &Path) -> io::Result<()> {
    if !path.exists() {
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(path)?;
    }
    let meta = fs::symlink_metadata(path)?;
    if !meta.is_dir() || meta.file_type().is_symlink() || meta.permissions().mode() & 0o077 != 0 {
        return Err(io::Error::other(
            "data directory must be a private, real directory (0700)",
        ));
    }
    Ok(())
}

pub fn new_private(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
}

pub fn atomic_write(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| io::Error::other("missing parent"))?;
    let tmp = path.with_extension(format!("tmp-{}", std::process::id()));
    let mut file = new_private(&tmp)?;
    let result = (|| {
        file.write_all(bytes)?;
        file.sync_all()?;
        fs::rename(&tmp, path)?;
        File::open(parent)?.sync_all()
    })();
    if result.is_err() {
        let _ = fs::remove_file(&tmp);
    }
    result
}

pub struct ProcessLock(File);
impl ProcessLock {
    pub fn acquire(data_dir: &Path) -> io::Result<Self> {
        private_dir(data_dir)?;
        let path = data_dir.join("collector.lock");
        if path.is_symlink() {
            return Err(io::Error::other("lock cannot be a symlink"));
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(path)?;
        fs2::FileExt::try_lock_exclusive(&file)
            .map_err(|_| io::Error::other("another collector owns this data directory"))?;
        Ok(Self(file))
    }
}
impl Drop for ProcessLock {
    fn drop(&mut self) {
        let _ = fs2::FileExt::unlock(&self.0);
    }
}

/// Restored delivery/spend state can be older than real external side effects.
pub fn restore_review_required(data: &Path) -> bool {
    fs::symlink_metadata(data.join("restore-review-required.json")).is_ok()
}
