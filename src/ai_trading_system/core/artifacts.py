from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class ArtifactRef:
    path: Path
    exists: bool
    artifact_type: str
    sha256: str | None
    size_bytes: int | None
    file_count: int | None

    @classmethod
    def from_path(cls, path: Path) -> ArtifactRef:
        exists = path.exists()
        is_file = exists and path.is_file()
        is_dir = exists and path.is_dir()
        return cls(
            path=path,
            exists=exists,
            artifact_type=_artifact_type(path),
            sha256=_sha256_file(path) if is_file else None,
            size_bytes=path.stat().st_size if is_file else None,
            file_count=_file_count(path) if is_dir else None,
        )

    def to_manifest_record(self) -> dict[str, Any]:
        return {
            "path": str(self.path),
            "exists": self.exists,
            "artifact_type": self.artifact_type,
            "sha256": self.sha256,
            "size_bytes": self.size_bytes,
            "file_count": self.file_count,
        }


def _artifact_type(path: Path) -> str:
    if path.is_dir():
        return "directory"
    suffix = path.suffix.lower().lstrip(".")
    return suffix or "file"


def _file_count(path: Path) -> int:
    return sum(1 for child in path.rglob("*") if child.is_file())


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
