import fcntl
import json
import hashlib
import io
import tarfile
from pathlib import Path
import tempfile
import unittest
import storage_snapshot as snapshot


class SnapshotTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.data = self.root / "data"
        self.data.mkdir(mode=0o700)
        (self.data / "collector.lock").touch(mode=0o600)
        (self.data / "history").mkdir(mode=0o700)
        (self.data / "history" / "actual-record-bytes").write_bytes(b"isolated-test-storage-boundary")
        self.archive = self.root / "snapshot.tar.gz"

    def tearDown(self):
        self.temp.cleanup()

    def test_roundtrip_preserves_bytes_and_blocks_live_until_external_reconciliation(self):
        snapshot.backup(self.data, self.archive)
        manifest = snapshot.inspect(self.archive)
        self.assertEqual(len(manifest["files"]), 1)
        restored = self.root / "restored"
        snapshot.restore(self.archive, restored)
        self.assertEqual((restored / "history" / "actual-record-bytes").read_bytes(), b"isolated-test-storage-boundary")
        self.assertEqual(restored.stat().st_mode & 0o777, 0o700)
        self.assertTrue(json.loads((restored / "restore-review-required.json").read_text())["reason"])
        self.assertFalse((restored / "collector.lock").exists())
        with self.assertRaises(ValueError):
            snapshot.restore(self.archive, restored)

    def test_live_lock_symlinks_and_corruption_are_rejected(self):
        with (self.data / "collector.lock").open("r+") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaises(OSError):
                snapshot.backup(self.data, self.archive)
        (self.data / "link").symlink_to(self.root / "elsewhere")
        with self.assertRaises(ValueError):
            snapshot.backup(self.data, self.archive)
        (self.data / "link").unlink()
        snapshot.backup(self.data, self.archive)
        with self.archive.open("ab") as output:
            output.write(b"corrupt")
        with self.assertRaises(ValueError):
            snapshot.inspect(self.archive)

    def test_valid_archive_checksum_cannot_authorize_path_traversal(self):
        with tarfile.open(self.archive, "w:gz") as tape:
            item = tarfile.TarInfo("files/../outside")
            item.size = 1
            tape.addfile(item, io.BytesIO(b"x"))
        Path(str(self.archive) + ".sha256").write_text(hashlib.sha256(self.archive.read_bytes()).hexdigest())
        with self.assertRaises(ValueError):
            snapshot.restore(self.archive, self.root / "restored")
        self.assertFalse((self.root / "outside").exists())
        self.assertFalse((self.root / "restored").exists())

    def test_paths_cannot_escape_restore_or_replace_the_collectors_lock(self):
        for path in ["../outside", "/outside", "a/../outside", "collector.lock", "history/collector.lock", "a//b", "a\\b"]:
            with self.assertRaises(ValueError):
                snapshot.safe_name(path)


if __name__ == "__main__":
    unittest.main()
