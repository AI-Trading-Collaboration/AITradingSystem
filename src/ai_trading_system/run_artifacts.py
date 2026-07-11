from __future__ import annotations

import platform
import re
import shutil
import subprocess
import sys
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.core.artifacts import ArtifactRef
from ai_trading_system.platform.artifacts import write_json_atomic

SCHEMA_VERSION = 1
LEGACY_OUTPUT_MODES = {"mirror", "off"}


@dataclass(frozen=True)
class RunArtifactPaths:
    as_of: date
    run_id: str
    safe_run_id: str
    execution_timestamp_utc: str
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
    generated_at: datetime | None = None,
) -> RunArtifactPaths:
    safe_id = safe_run_id(run_id)
    timestamp = (generated_at or datetime.now(tz=UTC)).astimezone(UTC)
    execution_timestamp = timestamp.strftime("%Y%m%dT%H%M%SZ")
    run_root = output_root / "daily" / execution_timestamp / f"as_of_{as_of.isoformat()}__{safe_id}"
    return RunArtifactPaths(
        as_of=as_of,
        run_id=run_id,
        safe_run_id=safe_id,
        execution_timestamp_utc=execution_timestamp,
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
        (
            paths.reports_dir / f"daily_task_dashboard_{as_of_text}.html",
            legacy_reports_dir / f"daily_task_dashboard_{as_of_text}.html",
        ),
        (
            paths.reports_dir / f"daily_task_dashboard_{as_of_text}.json",
            legacy_reports_dir / f"daily_task_dashboard_{as_of_text}.json",
        ),
        (
            paths.reports_dir / f"daily_decision_summary_{as_of_text}.json",
            legacy_reports_dir / f"daily_decision_summary_{as_of_text}.json",
        ),
        (
            paths.reports_dir / f"order_intent_candidates_{as_of_text}.json",
            legacy_reports_dir / f"order_intent_candidates_{as_of_text}.json",
        ),
        (
            paths.reports_dir / f"reader_brief_{as_of_text}.html",
            legacy_reports_dir / f"reader_brief_{as_of_text}.html",
        ),
        (
            paths.reports_dir / f"reader_brief_{as_of_text}.json",
            legacy_reports_dir / f"reader_brief_{as_of_text}.json",
        ),
        (
            paths.reports_dir / f"owner_daily_brief_{as_of_text}.html",
            legacy_reports_dir / f"owner_daily_brief_{as_of_text}.html",
        ),
        (
            paths.reports_dir / f"owner_daily_brief_{as_of_text}.json",
            legacy_reports_dir / f"owner_daily_brief_{as_of_text}.json",
        ),
        (
            paths.reports_dir / f"reader_brief_quality_{as_of_text}.json",
            legacy_reports_dir / f"reader_brief_quality_{as_of_text}.json",
        ),
        (
            paths.reports_dir / f"reader_brief_quality_{as_of_text}.md",
            legacy_reports_dir / f"reader_brief_quality_{as_of_text}.md",
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
    command: Iterable[str] | str | None = None,
    resolved_config: Mapping[str, Path | str] | None = None,
    schema_versions: Mapping[str, int | str] | None = None,
    random_seed: int | str | None = None,
    elapsed_seconds: float | None = None,
    warnings: Iterable[str] | None = None,
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
        command=command,
        resolved_config=resolved_config,
        schema_versions=schema_versions,
        random_seed=random_seed,
        elapsed_seconds=elapsed_seconds,
        warnings=warnings,
        generated_at=generated_at,
    )
    write_json_atomic(
        paths.manifest_path,
        manifest,
        sort_keys=False,
        trailing_newline=False,
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
    command: Iterable[str] | str | None = None,
    resolved_config: Mapping[str, Path | str] | None = None,
    schema_versions: Mapping[str, int | str] | None = None,
    random_seed: int | str | None = None,
    elapsed_seconds: float | None = None,
    warnings: Iterable[str] | None = None,
    generated_at: datetime | None = None,
) -> Mapping[str, object]:
    input_paths = tuple(dict.fromkeys(input_artifacts))
    output_paths = tuple(dict.fromkeys(canonical_output_artifacts))
    legacy_output_paths = tuple(dict.fromkeys(legacy_output_artifacts))
    input_records = [_artifact_record(path) for path in input_paths]
    output_records = [_artifact_record(path) for path in output_paths]
    legacy_output_records = [_artifact_record(path) for path in legacy_output_paths]
    resolved_config_records = _resolved_config_records(resolved_config or {})
    schema_version_records = {"run_manifest": str(SCHEMA_VERSION)}
    schema_version_records.update(
        {str(key): str(value) for key, value in (schema_versions or {}).items()}
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "daily_run_manifest",
        "production_effect": "none",
        "run_id": paths.run_id,
        "safe_run_id": paths.safe_run_id,
        "execution_timestamp_utc": paths.execution_timestamp_utc,
        "as_of": paths.as_of.isoformat(),
        "generated_at": (generated_at or datetime.now(tz=UTC)).isoformat(),
        "git_commit": _git_commit(project_root) or "unknown",
        "command": _command_list(command),
        "resolved_config": resolved_config_records,
        "input_checksums": _artifact_checksums(input_records),
        "schema_versions": schema_version_records,
        "random_seed": "not_applicable" if random_seed in (None, "") else random_seed,
        "environment_summary": _environment_summary(),
        "elapsed_seconds": 0.0 if elapsed_seconds is None else round(float(elapsed_seconds), 6),
        "warnings": [str(item) for item in (warnings or ())],
        "project_root": str(project_root),
        "run_root": str(paths.run_root),
        "status": status,
        "visibility_cutoff": visibility_cutoff.isoformat(),
        "visibility_cutoff_source": visibility_cutoff_source,
        "legacy_output_mode": legacy_output_mode,
        "input_artifacts": input_records,
        "output_artifacts": output_records,
        "legacy_output_artifacts": legacy_output_records,
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
    return ArtifactRef.from_path(path).to_manifest_record()


def _artifact_checksums(records: Iterable[Mapping[str, Any]]) -> dict[str, str]:
    checksums: dict[str, str] = {}
    for record in records:
        path = str(record.get("path") or "")
        if not path:
            continue
        checksums[path] = str(record.get("sha256") or "")
    return checksums


def _command_list(command: Iterable[str] | str | None) -> list[str]:
    if command is None:
        return ["unknown"]
    if isinstance(command, str):
        return [command]
    values = [str(item) for item in command if str(item)]
    return values or ["unknown"]


def _environment_summary() -> dict[str, str]:
    return {
        "python_version": sys.version.split()[0],
        "platform": platform.platform(),
        "working_directory": str(Path.cwd()),
    }


def _git_commit(project_root: Path) -> str | None:
    try:
        completed = subprocess.run(
            ("git", "rev-parse", "HEAD"),
            cwd=project_root,
            text=True,
            capture_output=True,
            check=False,
            timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if completed.returncode != 0:
        return None
    return completed.stdout.strip() or None


def _resolved_config_records(config: Mapping[str, Path | str]) -> dict[str, Mapping[str, object]]:
    records: dict[str, Mapping[str, object]] = {}
    for key, value in config.items():
        records[str(key)] = _artifact_record(Path(value))
    return records
