"""Inventory a confirmed Render disk mount without changing or deleting files.

Run on Render: python scripts/inventory_persistent_disk.py --root MOUNT --output /tmp/disk-manifest.json
Every regular file gets size and SHA-256. Symlinks and unreadable files block a
complete inventory and require inspection; they are never silently followed.
"""
import argparse
import hashlib
import json
from pathlib import Path
import os


def inventory(root):
    root = root.resolve(strict=True)
    files, problems = [], []
    def walk_error(error):
        problems.append({"path": str(error.filename), "error": type(error).__name__})
    for directory, dirs, names in os.walk(root, followlinks=False, onerror=walk_error):
        for name in list(dirs):
            path = Path(directory) / name
            if path.is_symlink():
                problems.append({"path": path.relative_to(root).as_posix(), "error": "symlink_directory"})
                dirs.remove(name)
        for name in sorted(names):
            path = Path(directory) / name
            relative = path.relative_to(root).as_posix()
            try:
                if path.is_symlink() or not path.is_file():
                    problems.append({"path": relative, "error": "non_regular_file"})
                    continue
                before = path.stat()
                digest = hashlib.sha256()
                with path.open("rb") as source:
                    for chunk in iter(lambda: source.read(1024 * 1024), b""):
                        digest.update(chunk)
                after = path.stat()
                if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
                    problems.append({"path": relative, "error": "changed_during_inventory"})
                    continue
                files.append({"path": relative, "bytes": after.st_size, "sha256": digest.hexdigest()})
            except OSError as exc:
                problems.append({"path": relative, "error": type(exc).__name__})
    return {"root": str(root), "complete": not problems, "files": files,
            "total_bytes": sum(f["bytes"] for f in files), "problems": problems}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    root = args.root.resolve(strict=True)
    if not root.is_dir():
        parser.error("--root must be a directory")
    if args.output.resolve().is_relative_to(root):
        parser.error("Write the manifest outside the source disk")
    report = inventory(root)
    args.output.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps({k: v for k, v in report.items() if k not in ("files", "problems")}))
    print("files:", len(report["files"]), "problems:", len(report["problems"]))
    return 0 if report["complete"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
