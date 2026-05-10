from __future__ import annotations

import hashlib
import json
import re
import shutil
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path

SCHEMA_VERSION = 1
LEGACY_OUTPUT_MODES = {"mirror", "off"}


@dataclass(frozen=True)
class RunArtifactPaths:
    as_of: date
    run_id: str
    safe_run_id: str
    output_root: Path
    run_root: Path
    reports_dir: Path
    traces_dir: Path
    metadata_dir: Path
    manifest_path: Path


def default_daily_run_id(as_of: date, generated_at: datetime | None = None) -> str:
    timestamp = (generated_at or datetime.now(tz=UTC)).astimezone(UTC)
    return f"daily_ops_run:{as_of.isoformat()}:{timestamp.strftime('%Y%m%dT%H%M%SZ')}"


def safe_run_id(run_id: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._=-]+", "_", run_id.strip())
    cleaned = cleaned.strip("._-")
    return cleaned or "run"


def build_run_artifact_paths(
    *,
    as_of: date,
    run_id: str,
    output_root: Path,
) -> RunArtifactPaths:
    safe_id = safe_run_id(run_id)
    run_root = output_root / as_of.isoformat() / safe_id
    return RunArtifactPaths(
        as_of=as_of,
        run_id=run_id,
        safe_run_id=safe_id,
        output_root=output_root,
        run_root=run_root,
        reports_dir=run_root / "reports",
        traces_dir=run_root / "traces",
        metadata_dir=run_root / "metadata",
        manifest_path=run_root / "manifest.json",
    )


def validate_legacy_output_mode(mode: str) -> str:
    normalized = mode.strip().lower()
    if normalized not in LEGACY_OUTPUT_MODES:
        raise ValueError("legacy_output_mode must be mirror or off")
    return normalized


def prepare_run_directories(paths: RunArtifactPaths) -> RunArtifactPaths:
    for directory in (
        paths.run_root,
        paths.reports_dir,
        paths.traces_dir,
        paths.metadata_dir,
    ):
        directory.mkdir(parents=True, exist_ok=True)
    return paths


def mirror_file(source: Path, destination: Path) -> Path | None:
    if not source.exists() or not source.is_file():
        return None
    destination.parent.mkdir(parents=True, exist_ok=True)
    if source.resolve() == destination.resolve():
        return destination
    shutil.copy2(source, destination)
    return destination


def mirror_canonical_daily_ops_outputs_to_legacy(
    *,
    paths: RunArtifactPaths,
    legacy_reports_dir: Path,
) -> tuple[Path, ...]:
    as_of_text = paths.as_of.isoformat()
    pairs = (
        (
            paths.reports_dir / f"daily_ops_plan_{as_of_text}.md",
            legacy_reports_dir / f"daily_ops_plan_{as_of_text}.md",
        ),
        (
            paths.reports_dir / f"daily_ops_run_{as_of_text}.md",
            legacy_reports_dir / f"daily_ops_run_{as_of_text}.md",
        ),
        (
            paths.metadata_dir / f"daily_ops_run_metadata_{as_of_text}.json",
            legacy_reports_dir / f"daily_ops_run_metadata_{as_of_text}.json",
        ),
    )
    mirrored: list[Path] = []
    for source, destination in pairs:
        if copied := mirror_file(source, destination):
            mirrored.append(copied)
    return tuple(mirrored)


def mirror_legacy_reports_to_run(
    *,
    as_of: date,
    legacy_reports_dir: Path,
    paths: RunArtifactPaths,
    min_modified_at: datetime | None = None,
) -> tuple[Path, ...]:
    if not legacy_reports_dir.exists():
        return ()
    min_modified_timestamp = None if min_modified_at is None else min_modified_at.timestamp()
    as_of_text = as_of.isoformat()
    mirrored: list[Path] = []
    for source in sorted(legacy_reports_dir.rglob("*")):
        if not source.is_file() or as_of_text not in source.name:
            continue
        if min_modified_timestamp is not None:
            try:
                if source.stat().st_mtime < min_modified_timestamp:
                    continue
            except OSError:
                continue
        if source.name.startswith(
            (
                "daily_ops_plan_",
                "daily_ops_run_",
                "daily_ops_run_metadata_",
            )
        ):
            continue
        relative = source.relative_to(legacy_reports_dir)
        if relative.parts and relative.parts[0] == "evidence":
            destination = paths.traces_dir / source.name
        elif source.name.startswith("daily_ops_run_metadata_"):
            destination = paths.metadata_dir / source.name
        else:
            destination = paths.reports_dir / relative.name
        if copied := mirror_file(source, destination):
            mirrored.append(copied)
    return tuple(dict.fromkeys(mirrored))


def write_run_manifest(
    *,
    paths: RunArtifactPaths,
    project_root: Path,
    status: str,
    visibility_cutoff: datetime,
    visibility_cutoff_source: str,
    legacy_output_mode: str,
    input_artifacts: Iterable[Path],
    canonical_output_artifacts: Iterable[Path],
    legacy_output_artifacts: Iterable[Path],
    generated_at: datetime | None = None,
) -> Path:
    manifest = build_run_manifest(
        paths=paths,
        project_root=project_root,
        status=status,
        visibility_cutoff=visibility_cutoff,
        visibility_cutoff_source=visibility_cutoff_source,
        legacy_output_mode=legacy_output_mode,
        input_artifacts=input_artifacts,
        canonical_output_artifacts=canonical_output_artifacts,
        legacy_output_artifacts=legacy_output_artifacts,
        generated_at=generated_at,
    )
    paths.manifest_path.parent.mkdir(parents=True, exist_ok=True)
    paths.manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return paths.manifest_path


def build_run_manifest(
    *,
    paths: RunArtifactPaths,
    project_root: Path,
    status: str,
    visibility_cutoff: datetime,
    visibility_cutoff_source: str,
    legacy_output_mode: str,
    input_artifacts: Iterable[Path],
    canonical_output_artifacts: Iterable[Path],
    legacy_output_artifacts: Iterable[Path],
    generated_at: datetime | None = None,
) -> Mapping[str, object]:
    return {
        "schema_version": SCHEMA_VERSION,
        "run_id": paths.run_id,
        "safe_run_id": paths.safe_run_id,
        "as_of": paths.as_of.isoformat(),
        "generated_at": (generated_at or datetime.now(tz=UTC)).isoformat(),
        "project_root": str(project_root),
        "run_root": str(paths.run_root),
        "status": status,
        "visibility_cutoff": visibility_cutoff.isoformat(),
        "visibility_cutoff_source": visibility_cutoff_source,
        "legacy_output_mode": legacy_output_mode,
        "input_artifacts": [_artifact_record(path) for path in dict.fromkeys(input_artifacts)],
        "output_artifacts": [
            _artifact_record(path) for path in dict.fromkeys(canonical_output_artifacts)
        ],
        "legacy_output_artifacts": [
            _artifact_record(path) for path in dict.fromkeys(legacy_output_artifacts)
        ],
    }


def collect_run_files(paths: RunArtifactPaths) -> tuple[Path, ...]:
    if not paths.run_root.exists():
        return ()
    return tuple(
        path
        for path in sorted(paths.run_root.rglob("*"))
        if path.is_file() and path != paths.manifest_path
    )


def _artifact_record(path: Path) -> Mapping[str, object]:
    return {
        "path": str(path),
        "exists": path.exists(),
        "artifact_type": _artifact_type(path),
        "sha256": _sha256_file(path) if path.exists() and path.is_file() else None,
        "size_bytes": path.stat().st_size if path.exists() and path.is_file() else None,
        "file_count": _file_count(path) if path.exists() and path.is_dir() else None,
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
