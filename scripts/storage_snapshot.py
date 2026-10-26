#!/usr/bin/env python3
"""Offline, checksummed data snapshots. No network, process starts, or secret config copies."""
import argparse
import contextlib
import fcntl
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import stat
import tarfile
import time
import uuid

MAX_FILES = 10_000
MAX_BYTES = 16 * 1024**3
MAX_MANIFEST = 8 * 1024**2


def digest(path):
    result = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024**2), b""):
            result.update(chunk)
    return result.hexdigest()


def private_file(path):
    return os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600), "wb")


def sync_dir(path):
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def safe_name(name):
    parts = PurePosixPath(name)
    if (not name or len(name) > 1024 or "\x00" in name or "\\" in name or parts.is_absolute()
            or any(part in ("..", ".", "collector.lock") for part in parts.parts)
            or parts.as_posix() != name):
        raise ValueError("invalid snapshot member")
    return parts


@contextlib.contextmanager
def stopped_lock(data):
    metadata = data.lstat()
    if not stat.S_ISDIR(metadata.st_mode) or metadata.st_mode & 0o077:
        raise ValueError("source must be a private real directory")
    descriptor = os.open(data / "collector.lock", os.O_RDWR | os.O_NOFOLLOW)
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise ValueError("invalid source lock")
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield
    finally:
        os.close(descriptor)


class HashReader:
    def __init__(self, source):
        self.source = source
        self.hash = hashlib.sha256()

    def read(self, size):
        chunk = self.source.read(size)
        self.hash.update(chunk)
        return chunk


def backup(data, archive):
    data, archive = Path(data).absolute(), Path(archive).absolute()
    checksum = Path(str(archive) + ".sha256")
    if archive.resolve().is_relative_to(data.resolve()) or archive.exists() or checksum.exists():
        raise ValueError("snapshot must be a new path outside the source")
    created = False
    checksum_created = False
    with stopped_lock(data):
        files, total = [], 0
        for root, dirs, names in os.walk(data, followlinks=False):
            for name in dirs:
                if not stat.S_ISDIR((Path(root) / name).lstat().st_mode):
                    raise ValueError("source contains a link or special directory")
            for name in names:
                if name == "collector.lock":
                    continue
                path = Path(root) / name
                relative = path.relative_to(data).as_posix()
                safe_name(relative)
                metadata = path.lstat()
                if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
                    raise ValueError("source contains a link or special file")
                total += metadata.st_size
                files.append((relative, metadata))
                if len(files) > MAX_FILES or total > MAX_BYTES:
                    raise ValueError("snapshot capacity exceeded")
        if shutil.disk_usage(archive.parent).free < total + 16 * 1024**2:
            raise ValueError("insufficient snapshot disk space")
        try:
            with private_file(archive) as output:
                created = True
                entries = []
                with tarfile.open(fileobj=output, mode="w:gz") as tape:
                    for relative, expected in sorted(files):
                        descriptor = os.open(data / relative, os.O_RDONLY | os.O_NOFOLLOW)
                        with os.fdopen(descriptor, "rb") as source:
                            actual = os.fstat(source.fileno())
                            if (actual.st_dev, actual.st_ino, actual.st_size, actual.st_mtime_ns) != (expected.st_dev, expected.st_ino, expected.st_size, expected.st_mtime_ns):
                                raise ValueError("source changed during snapshot")
                            reader = HashReader(source)
                            item = tarfile.TarInfo("files/" + relative)
                            item.size, item.mode = actual.st_size, 0o600
                            tape.addfile(item, reader)
                            after = os.fstat(source.fileno())
                            if (after.st_size, after.st_mtime_ns) != (actual.st_size, actual.st_mtime_ns):
                                raise ValueError("source changed during snapshot")
                            entries.append({"path": relative, "bytes": item.size, "sha256": reader.hash.hexdigest()})
                    payload = json.dumps({"schema": 1, "created_ms": int(time.time() * 1000), "files": entries}, sort_keys=True).encode()
                    if len(payload) > MAX_MANIFEST:
                        raise ValueError("manifest capacity exceeded")
                    item = tarfile.TarInfo("manifest.json")
                    item.size, item.mode = len(payload), 0o600
                    tape.addfile(item, io.BytesIO(payload))
                output.flush()
                os.fsync(output.fileno())
            with private_file(checksum) as output:
                checksum_created = True
                output.write((digest(archive) + "\n").encode())
                output.flush()
                os.fsync(output.fileno())
            sync_dir(archive.parent)
        except BaseException:
            if created:
                archive.unlink(missing_ok=True)
                if checksum_created:
                    checksum.unlink(missing_ok=True)
            raise


def inspect(archive):
    archive = Path(archive)
    if archive.is_symlink() or not archive.is_file():
        raise ValueError("snapshot must be a regular file")
    checksum = Path(str(archive) + ".sha256")
    if checksum.is_symlink() or checksum.stat().st_size > 128:
        raise ValueError("invalid snapshot checksum")
    expected = checksum.read_text().strip()
    if len(expected) != 64 or digest(archive) != expected:
        raise ValueError("snapshot checksum mismatch")
    members, total, manifest = {}, 0, None
    with tarfile.open(archive, "r:gz") as tape:
        for item in tape:
            if len(members) >= MAX_FILES + 1 or not item.isfile() or item.sparse:
                raise ValueError("invalid snapshot member type or count")
            if item.name in members or item.size < 0:
                raise ValueError("duplicate or invalid snapshot member")
            members[item.name] = item.size
            if item.name == "manifest.json":
                if item.size > MAX_MANIFEST:
                    raise ValueError("manifest capacity exceeded")
                manifest = json.load(tape.extractfile(item))
            else:
                if not item.name.startswith("files/"):
                    raise ValueError("unknown snapshot member")
                safe_name(item.name[6:])
                total += item.size
                if total > MAX_BYTES:
                    raise ValueError("snapshot capacity exceeded")
    if not isinstance(manifest, dict) or set(manifest) != {"schema", "created_ms", "files"} or manifest["schema"] != 1 or not isinstance(manifest["created_ms"], int):
        raise ValueError("invalid snapshot manifest")
    entries = manifest["files"]
    if not isinstance(entries, list) or len(entries) > MAX_FILES:
        raise ValueError("invalid snapshot file list")
    expected_members = {"manifest.json": members.get("manifest.json")}
    for entry in entries:
        if not isinstance(entry, dict) or set(entry) != {"path", "bytes", "sha256"}:
            raise ValueError("invalid snapshot entry")
        safe_name(entry["path"])
        if not isinstance(entry["bytes"], int) or not 0 <= entry["bytes"] <= MAX_BYTES or not isinstance(entry["sha256"], str) or len(entry["sha256"]) != 64:
            raise ValueError("invalid snapshot entry values")
        name = "files/" + entry["path"]
        if name in expected_members:
            raise ValueError("duplicate manifest entry")
        expected_members[name] = entry["bytes"]
    if expected_members != members:
        raise ValueError("manifest/member mismatch")
    return manifest


def restore(archive, target):
    archive, target = Path(archive), Path(target).absolute()
    manifest = inspect(archive)
    if target.exists() or target.is_symlink():
        raise ValueError("restore target must not exist")
    total = sum(entry["bytes"] for entry in manifest["files"])
    if shutil.disk_usage(target.parent).free < total + 16 * 1024**2:
        raise ValueError("insufficient restore disk space")
    staging = target.with_name("." + target.name + ".restore-" + uuid.uuid4().hex)
    staging.mkdir(mode=0o700)
    try:
        with tarfile.open(archive, "r:gz") as tape:
            for entry in manifest["files"]:
                path = staging.joinpath(*safe_name(entry["path"]).parts)
                path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
                with tape.extractfile("files/" + entry["path"]) as source, private_file(path) as output:
                    hashed = hashlib.sha256()
                    remaining = entry["bytes"]
                    while remaining:
                        chunk = source.read(min(1024**2, remaining))
                        if not chunk:
                            raise ValueError("short snapshot file")
                        output.write(chunk)
                        hashed.update(chunk)
                        remaining -= len(chunk)
                    if hashed.hexdigest() != entry["sha256"]:
                        raise ValueError("snapshot file checksum mismatch")
                    output.flush()
                    os.fsync(output.fileno())
        marker = staging / "restore-review-required.json"
        # Always replace an older restore marker with the current recovery boundary.
        marker.unlink(missing_ok=True)
        with private_file(marker) as output:
            output.write(json.dumps({"schema": 1, "snapshot_ms": manifest["created_ms"], "reason": "reconcile external spend and delivery outcomes before live startup"}).encode())
            output.flush()
            os.fsync(output.fileno())
        for root, _, _ in os.walk(staging, topdown=False):
            os.chmod(root, 0o700)
            sync_dir(root)
        if target.exists() or target.is_symlink():
            raise ValueError("restore target appeared during extraction")
        staging.rename(target)
        sync_dir(target.parent)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=["backup", "verify", "restore"])
    parser.add_argument("source")
    parser.add_argument("destination", nargs="?")
    args = parser.parse_args()
    try:
        if args.operation == "backup" and args.destination:
            backup(args.source, args.destination)
        elif args.operation == "restore" and args.destination:
            restore(args.source, args.destination)
        elif args.operation == "verify" and not args.destination:
            inspect(args.source)
        else:
            parser.error("backup/restore require a destination; verify does not")
    except (OSError, ValueError, tarfile.TarError, TypeError, KeyError, EOFError):
        parser.exit(2, "snapshot operation failed; storage, lock, capacity or integrity check rejected it (paths redacted)\n")
    print(json.dumps({"operation": args.operation, "status": "complete", "live_start": "blocked_pending_reconciliation" if args.operation == "restore" else "not_requested"}))


if __name__ == "__main__":
    main()
