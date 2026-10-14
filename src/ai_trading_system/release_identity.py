"""Read and verify the code identity of a git-free daily release (OPS-086)."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path

from ai_trading_system import paths

RELEASE_FILE = "RELEASE.json"
SCHEMA_VERSION = "aits_release.v1"


class ReleaseManifestError(ValueError):
    """The release manifest cannot be read or violates the release protocol."""


@dataclass(frozen=True)
class ReleaseFile:
    path: str
    sha256: str
    size: int


@dataclass(frozen=True)
class ReleaseManifest:
    release_id: str
    source_commit: str
    source_tree: str
    origin_main_commit: str
    built_at: str
    files: tuple[ReleaseFile, ...]


def is_release_root(root: Path) -> bool:
    root = Path(root)
    return paths.join_root(root, RELEASE_FILE).is_file() and not (root / ".git").exists()


def find_release_root(path: Path) -> Path | None:
    path = Path(path)
    directory = path.parent if path.is_file() else path
    for root in (directory, *directory.parents):
        if is_release_root(root):
            return root
    return None


def files_digest(files: Iterable[ReleaseFile]) -> str:
    digest = hashlib.sha256()
    for item in files:
        digest.update(f"{item.sha256}  {item.path}\n".encode())
    return digest.hexdigest()


def read_release_manifest(root: Path) -> ReleaseManifest:
    try:
        payload = json.loads(paths.join_root(Path(root), RELEASE_FILE).read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ReleaseManifestError(f"{RELEASE_FILE}: {exc}") from exc
    if not isinstance(payload, dict):
        raise ReleaseManifestError(f"{RELEASE_FILE}: expected JSON object")
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise ReleaseManifestError(f"schema_version: expected {SCHEMA_VERSION}")
    for key in ("source_commit", "source_tree", "origin_main_commit"):
        value = payload.get(key)
        if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{40}", value) is None:
            raise ReleaseManifestError(f"{key}: expected 40 lowercase hex characters")
    release_id = payload.get("release_id")
    if (
        not isinstance(release_id, str)
        or re.fullmatch(r"[0-9]{8}-[0-9a-f]{9}", release_id) is None
        or release_id[9:] != payload["source_commit"][:9]
    ):
        raise ReleaseManifestError("release_id: expected YYYYMMDD-<source_commit prefix>")
    built_at = payload.get("built_at")
    if not isinstance(built_at, str):
        raise ReleaseManifestError("built_at: expected string")
    entries = payload.get("files")
    if not isinstance(entries, list) or not entries:
        raise ReleaseManifestError("files: expected nonempty list")
    files = []
    for index, entry in enumerate(entries):
        label = f"files[{index}]"
        if not isinstance(entry, dict):
            raise ReleaseManifestError(f"{label}: expected object")
        path = entry.get("path")
        if (
            not isinstance(path, str)
            or not path
            or "\\" in path
            or ":" in path
            or "\x00" in path
            or any(part in ("", ".", "..") for part in path.split("/"))
            or path == RELEASE_FILE
            or path.split("/")[0] in paths.STATE_DIRS
        ):
            raise ReleaseManifestError(f"{label}.path: unsafe relative POSIX path {path!r}")
        sha256 = entry.get("sha256")
        if not isinstance(sha256, str) or re.fullmatch(r"[0-9a-f]{64}", sha256) is None:
            raise ReleaseManifestError(f"{label}.sha256: expected 64 lowercase hex characters")
        size = entry.get("size")
        if type(size) is not int or size < 0:
            raise ReleaseManifestError(f"{label}.size: expected nonnegative integer")
        files.append(ReleaseFile(path, sha256, size))
    names = [item.path for item in files]
    if names != sorted(names) or len(set(names)) != len(names):
        raise ReleaseManifestError("files: paths must be sorted and unique")
    if payload.get("files_sha256") != files_digest(files):
        raise ReleaseManifestError("files_sha256: files digest mismatch")
    return ReleaseManifest(
        release_id=release_id,
        source_commit=payload["source_commit"],
        source_tree=payload["source_tree"],
        origin_main_commit=payload["origin_main_commit"],
        built_at=built_at,
        files=tuple(files),
    )


def release_mismatches(
    root: Path,
    manifest: ReleaseManifest | None = None,
    *,
    prefixes: Sequence[str] = (),
) -> tuple[str, ...]:
    manifest = manifest if manifest is not None else read_release_manifest(root)
    mismatches = []
    for item in manifest.files:
        if prefixes and item.path.split("/")[0] not in prefixes:
            continue
        path = paths.join_root(Path(root), item.path)
        if not path.is_file():
            mismatches.append(f"missing {item.path}")
            continue
        digest = hashlib.sha256()
        try:
            with path.open("rb") as handle:
                for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                    digest.update(chunk)
        except OSError as exc:
            raise ReleaseManifestError(f"files: cannot read {item.path}: {exc}") from exc
        actual = digest.hexdigest()
        if actual != item.sha256:
            mismatches.append(f"changed {item.path} {actual}")
    return tuple(sorted(mismatches))


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    verify = commands.add_parser("verify")
    verify.add_argument("--root", type=Path, default=paths.CODE_ROOT)
    args = parser.parse_args(argv)
    try:
        if (args.root / ".git").exists():
            raise ReleaseManifestError(".git: root is a git checkout, not a release")
        manifest = read_release_manifest(args.root)
        mismatches = release_mismatches(args.root, manifest)
    except ReleaseManifestError as exc:
        print(f"error={exc}", file=sys.stderr)
        return 2
    print(f"release_id={manifest.release_id}")
    print(f"source_commit={manifest.source_commit}")
    print(f"mismatches={len(mismatches)}")
    for mismatch in mismatches[:20]:
        print(mismatch)
    return 1 if mismatches else 0


if __name__ == "__main__":
    sys.exit(main())
