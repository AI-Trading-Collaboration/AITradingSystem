from __future__ import annotations

import argparse
import base64
import binascii
import hashlib
import json
import re
import subprocess
from collections.abc import Mapping, Sequence
from pathlib import Path, PurePosixPath
from typing import Any, TypedDict

from ai_trading_system.platform.architecture.bootstrap_handoff import (
    REQUIRED_VALIDATION_TIERS,
    BootstrapHandoffError,
    validate_bootstrap_handoff,
)
from ai_trading_system.yaml_loader import (
    StrictYamlError,
    StrictYamlOptions,
    load_strict_yaml_text,
)

PORTABLE_VALIDATION_REPORT_SCHEMA_VERSION = "portable_bootstrap_validation_report.v1"
BOOTSTRAP_VALIDATION_BUNDLE_SCHEMA_VERSION = "arch_005_bootstrap_validation_bundle.v1"
G2_5_POLICY_SCHEMA_VERSION = "arch_004_g2_5_readiness_policy.v1"
DEFAULT_POLICY_PATH = Path("config/architecture/arch_004_g2_5_readiness.yaml")
DEFAULT_PROJECT_ROOT = Path(__file__).resolve().parents[4]

_COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_BUNDLE_FIELDS = {
    "schema_version",
    "source_handoff_path",
    "source_handoff_sha256",
    "source_handoff_checksum",
    "artifact_count",
    "artifacts",
    "production_effect",
    "broker_action",
}
_BUNDLE_ARTIFACT_FIELDS = {"tier", "original_path", "sha256", "content_base64"}
_RUNTIME_TIER_BY_BUNDLE_TIER = {
    "focused": "fast-unit",
    "architecture_fitness": "architecture-fitness",
    "contract_validation": "contract-validation",
    "full_validation": "full",
}
_STRICT_YAML_OPTIONS = StrictYamlOptions(
    key_policy="HASHABLE",
    flatten_mapping=True,
    reject_non_finite=False,
)


class PortableValidationBundleError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


class _ValidatedArtifact(TypedDict):
    bundle_tier: str
    runtime_tier: str
    original_path: str
    sha256: str
    size_bytes: int
    status: str
    exit_code: int
    content: bytes


class _ValidatedBundle(TypedDict):
    bundle_sha256: str
    artifacts: list[_ValidatedArtifact]


def load_validation_bundle_bytes(
    *,
    path: Path,
    expected_sha256: str,
    handoff: Mapping[str, Any],
    handoff_path: Path,
    project_root: Path,
) -> dict[str, bytes]:
    """Load exact validation bytes and verify all portable artifact facts."""

    validated = _load_and_validate_bundle(
        path=path,
        expected_sha256=expected_sha256,
        handoff=handoff,
        handoff_path=handoff_path,
        project_root=project_root,
    )
    return {
        str(row["original_path"]): bytes(row["content"])
        for row in validated["artifacts"]
    }


def validate_portable_bootstrap_bundle(
    *,
    project_root: Path,
    bundle_path: Path,
    expected_bundle_sha256: str,
    handoff_path: Path,
    source_base_commit: str,
    git_project_root: Path | None = None,
) -> dict[str, object]:
    """Validate a tracked bootstrap bundle without reading historical outputs."""

    root = project_root.resolve()
    git_root = (git_project_root or root).resolve()
    resolved_bundle = _repo_file(root, bundle_path, "bundle_path")
    resolved_handoff = _repo_file(root, handoff_path, "handoff_path")
    handoff = _mapping(_load_yaml_path(resolved_handoff, "handoff"), "handoff")
    source_base = _resolve_commit(git_root, source_base_commit, "source_base")
    handoff_base = _commit(handoff.get("base_commit"), "handoff.base_commit")
    handoff_head = _commit(handoff.get("head_commit"), "handoff.head_commit")
    for label, commit in (
        ("handoff_base", handoff_base),
        ("handoff_head", handoff_head),
        ("source_base", source_base),
    ):
        _require_git_commit(git_root, commit, label)
    _require_ancestor(
        git_root,
        handoff_base,
        handoff_head,
        "HANDOFF_BASE_HEAD_LINEAGE",
    )
    _require_ancestor(
        git_root,
        handoff_head,
        source_base,
        "HANDOFF_HEAD_SOURCE_BASE_LINEAGE",
    )

    validated = _load_and_validate_bundle(
        path=resolved_bundle,
        expected_sha256=expected_bundle_sha256,
        handoff=handoff,
        handoff_path=resolved_handoff,
        project_root=root,
    )
    frozen_validation_artifacts = {
        str(row["original_path"]): bytes(row["content"])
        for row in validated["artifacts"]
    }
    frozen_paths = _handoff_frozen_paths(handoff)
    frozen_tracked_files = {
        path: _git_blob(git_root, handoff_head, path) for path in frozen_paths
    }
    try:
        validate_bootstrap_handoff(
            handoff,
            project_root=root,
            expected_head_commit=handoff_head,
            expected_branch=_text(handoff.get("branch"), "handoff.branch"),
            frozen_tracked_files=frozen_tracked_files,
            frozen_validation_artifacts=frozen_validation_artifacts,
        )
    except BootstrapHandoffError as exc:
        raise PortableValidationBundleError(
            exc.code if exc.code.startswith("HANDOFF_") else f"HANDOFF_{exc.code}",
            exc.message,
        ) from exc

    artifact_summaries = [
        {
            "bundle_tier": row["bundle_tier"],
            "runtime_tier": row["runtime_tier"],
            "original_path": row["original_path"],
            "sha256": row["sha256"],
            "size_bytes": row["size_bytes"],
            "status": row["status"],
            "exit_code": row["exit_code"],
        }
        for row in validated["artifacts"]
    ]
    return {
        "schema_version": PORTABLE_VALIDATION_REPORT_SCHEMA_VERSION,
        "status": "PASS",
        "bundle": {
            "path": _relative(resolved_bundle, root),
            "schema_version": BOOTSTRAP_VALIDATION_BUNDLE_SCHEMA_VERSION,
            "sha256": validated["bundle_sha256"],
            "size_bytes": resolved_bundle.stat().st_size,
            "artifact_count": len(artifact_summaries),
        },
        "handoff": {
            "path": _relative(resolved_handoff, root),
            "sha256": _sha256_path(resolved_handoff),
            "checksum": handoff["handoff_checksum"],
            "base_commit": handoff_base,
            "head_commit": handoff_head,
            "canonical_validation": "PASS",
            "frozen_tracked_file_count": len(frozen_tracked_files),
        },
        "git_lineage": {
            "status": "PASS",
            "handoff_base_commit": handoff_base,
            "handoff_head_commit": handoff_head,
            "source_base_commit": source_base,
            "handoff_base_is_ancestor_of_handoff_head": True,
            "handoff_head_is_ancestor_of_source_base": True,
        },
        "artifacts": artifact_summaries,
        "untracked_outputs_read": False,
        "source_of_truth": "TRACKED_BUNDLE_AND_CANONICAL_HANDOFF",
        "production_effect": "none",
        "broker_action": "none",
    }


def validate_from_policy(
    *,
    project_root: Path,
    policy_path: Path = DEFAULT_POLICY_PATH,
    source_base_commit: str = "HEAD",
    git_project_root: Path | None = None,
) -> dict[str, object]:
    root = project_root.resolve()
    resolved_policy = _repo_file(root, policy_path, "policy_path")
    policy = _mapping(_load_yaml_path(resolved_policy, "policy"), "policy")
    if policy.get("schema_version") != G2_5_POLICY_SCHEMA_VERSION:
        raise PortableValidationBundleError(
            "POLICY_SCHEMA",
            str(policy.get("schema_version")),
        )
    return validate_portable_bootstrap_bundle(
        project_root=root,
        bundle_path=Path(
            _portable(
                policy.get("source_validation_bundle_path"),
                "source_validation_bundle_path",
            )
        ),
        expected_bundle_sha256=_sha256_text(
            policy.get("source_validation_bundle_sha256"),
            "source_validation_bundle_sha256",
        ),
        handoff_path=Path(
            _portable(policy.get("source_handoff_path"), "source_handoff_path")
        ),
        source_base_commit=source_base_commit,
        git_project_root=git_project_root,
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="验证 tracked bootstrap validation bundle 与 Git lineage。",
    )
    parser.add_argument("--repository", default=str(DEFAULT_PROJECT_ROOT))
    parser.add_argument("--policy", default=str(DEFAULT_POLICY_PATH))
    parser.add_argument("--source-base", default="HEAD")
    args = parser.parse_args(argv)
    try:
        report = validate_from_policy(
            project_root=Path(args.repository),
            policy_path=Path(args.policy),
            source_base_commit=args.source_base,
        )
    except PortableValidationBundleError as exc:
        print(
            json.dumps(
                {
                    "schema_version": PORTABLE_VALIDATION_REPORT_SCHEMA_VERSION,
                    "status": "FAIL",
                    "error_code": exc.code,
                    "error": exc.message,
                    "production_effect": "none",
                    "broker_action": "none",
                },
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
        )
        return 1
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


def _load_and_validate_bundle(
    *,
    path: Path,
    expected_sha256: str,
    handoff: Mapping[str, Any],
    handoff_path: Path,
    project_root: Path,
) -> _ValidatedBundle:
    root = project_root.resolve()
    resolved_bundle = path.resolve()
    expected_bundle_sha = _sha256_text(expected_sha256, "expected_bundle_sha256")
    actual_bundle_sha = _sha256_path(resolved_bundle)
    if actual_bundle_sha != expected_bundle_sha:
        raise PortableValidationBundleError(
            "VALIDATION_BUNDLE_FILE_HASH_DRIFT",
            f"expected={expected_bundle_sha} actual={actual_bundle_sha}",
        )
    payload = _mapping(_load_json_path(resolved_bundle, "validation_bundle"), "bundle")
    _exact(payload, _BUNDLE_FIELDS, "VALIDATION_BUNDLE_FIELDS")
    if payload["schema_version"] != BOOTSTRAP_VALIDATION_BUNDLE_SCHEMA_VERSION:
        raise PortableValidationBundleError(
            "VALIDATION_BUNDLE_SCHEMA",
            str(payload["schema_version"]),
        )
    expected_handoff_path = _relative(handoff_path.resolve(), root)
    if (
        payload["source_handoff_path"] != expected_handoff_path
        or payload["source_handoff_sha256"] != _sha256_path(handoff_path)
        or payload["source_handoff_checksum"] != handoff.get("handoff_checksum")
    ):
        raise PortableValidationBundleError(
            "VALIDATION_BUNDLE_HANDOFF_DRIFT",
            expected_handoff_path,
        )
    if payload["production_effect"] != "none" or payload["broker_action"] != "none":
        raise PortableValidationBundleError(
            "VALIDATION_BUNDLE_UNSAFE_EFFECT",
            str(path),
        )
    rows = _maps(payload["artifacts"], "bundle.artifacts")
    if payload["artifact_count"] != len(rows):
        raise PortableValidationBundleError(
            "VALIDATION_BUNDLE_COUNT_DRIFT",
            str(payload["artifact_count"]),
        )
    handoff_tiers = _mapping(
        handoff.get("validation_artifacts"),
        "handoff.validation_artifacts",
    )
    if set(handoff_tiers) != set(REQUIRED_VALIDATION_TIERS):
        raise PortableValidationBundleError(
            "VALIDATION_BUNDLE_HANDOFF_TIER_SET",
            str(sorted(handoff_tiers)),
        )

    artifacts: list[_ValidatedArtifact] = []
    seen_tiers: set[str] = set()
    seen_paths: set[str] = set()
    for row in rows:
        _exact(row, _BUNDLE_ARTIFACT_FIELDS, "VALIDATION_BUNDLE_ARTIFACT_FIELDS")
        bundle_tier = _text(row["tier"], "bundle.tier")
        if bundle_tier in seen_tiers:
            raise PortableValidationBundleError(
                "VALIDATION_BUNDLE_DUPLICATE_TIER",
                bundle_tier,
            )
        seen_tiers.add(bundle_tier)
        if bundle_tier not in REQUIRED_VALIDATION_TIERS:
            raise PortableValidationBundleError(
                "VALIDATION_BUNDLE_UNKNOWN_TIER",
                bundle_tier,
            )
        source = _mapping(
            handoff_tiers[bundle_tier],
            f"handoff.validation_artifacts.{bundle_tier}",
        )
        original_path = _portable(row["original_path"], "bundle.original_path")
        if original_path in seen_paths:
            raise PortableValidationBundleError(
                "VALIDATION_BUNDLE_DUPLICATE_PATH",
                original_path,
            )
        seen_paths.add(original_path)
        if original_path != source.get("artifact_path"):
            raise PortableValidationBundleError(
                "VALIDATION_BUNDLE_PATH_DRIFT",
                bundle_tier,
            )
        expected_content_sha = _sha256_text(row["sha256"], "bundle.sha256")
        if expected_content_sha != source.get("artifact_sha256"):
            raise PortableValidationBundleError(
                "VALIDATION_BUNDLE_HANDOFF_HASH_DRIFT",
                bundle_tier,
            )
        try:
            content = base64.b64decode(
                _text(row["content_base64"], "bundle.content_base64"),
                validate=True,
            )
        except (binascii.Error, ValueError) as exc:
            raise PortableValidationBundleError(
                "VALIDATION_BUNDLE_BASE64",
                bundle_tier,
            ) from exc
        actual_content_sha = hashlib.sha256(content).hexdigest()
        if actual_content_sha != expected_content_sha:
            raise PortableValidationBundleError(
                "VALIDATION_BUNDLE_CONTENT_HASH_DRIFT",
                f"{bundle_tier}:{expected_content_sha}->{actual_content_sha}",
            )
        summary = _mapping(
            _load_json_bytes(content, f"artifact.{bundle_tier}"),
            f"artifact.{bundle_tier}",
        )
        if summary.get("status") != "PASS":
            raise PortableValidationBundleError(
                "VALIDATION_BUNDLE_ARTIFACT_STATUS",
                f"{bundle_tier}:{summary.get('status')}",
            )
        exit_code = summary.get("exit_code")
        if isinstance(exit_code, bool) or not isinstance(exit_code, int) or exit_code != 0:
            raise PortableValidationBundleError(
                "VALIDATION_BUNDLE_ARTIFACT_EXIT_CODE",
                f"{bundle_tier}:{exit_code}",
            )
        runtime_tier = _RUNTIME_TIER_BY_BUNDLE_TIER[bundle_tier]
        observed_tiers = {
            str(summary.get(field) or "")
            for field in ("tier", "requested_tier", "resolved_tier")
        }
        if observed_tiers != {runtime_tier}:
            raise PortableValidationBundleError(
                "VALIDATION_BUNDLE_ARTIFACT_TIER",
                f"{bundle_tier}:expected={runtime_tier} actual={sorted(observed_tiers)}",
            )
        artifacts.append(
            {
                "bundle_tier": bundle_tier,
                "runtime_tier": runtime_tier,
                "original_path": original_path,
                "sha256": actual_content_sha,
                "size_bytes": len(content),
                "status": "PASS",
                "exit_code": 0,
                "content": content,
            }
        )
    if seen_tiers != set(REQUIRED_VALIDATION_TIERS):
        raise PortableValidationBundleError(
            "VALIDATION_BUNDLE_TIER_SET",
            f"expected={list(REQUIRED_VALIDATION_TIERS)} actual={sorted(seen_tiers)}",
        )
    artifacts.sort(key=lambda row: REQUIRED_VALIDATION_TIERS.index(str(row["bundle_tier"])))
    return {
        "bundle_sha256": actual_bundle_sha,
        "artifacts": artifacts,
    }


def _handoff_frozen_paths(handoff: Mapping[str, Any]) -> tuple[str, ...]:
    matrix = _mapping(handoff.get("migration_matrix"), "handoff.migration_matrix")
    architecture_state = _mapping(
        handoff.get("architecture_state"),
        "handoff.architecture_state",
    )
    attribution = _mapping(
        handoff.get("worktree_attribution"),
        "handoff.worktree_attribution",
    )
    frozen_paths = {
        _portable(matrix.get("path"), "handoff.migration_matrix.path"),
        _portable(attribution.get("attribution_path"), "handoff.attribution_path"),
    }
    for raw in architecture_state.values():
        row = _mapping(raw, "handoff.architecture_state.record")
        frozen_paths.add(_portable(row.get("path"), "handoff.architecture_state.path"))
    if len(frozen_paths) != 6:
        raise PortableValidationBundleError(
            "HANDOFF_FROZEN_SOURCE_SET",
            str(sorted(frozen_paths)),
        )
    return tuple(sorted(frozen_paths))


def _load_json_path(path: Path, label: str) -> object:
    try:
        return _load_json_bytes(path.read_bytes(), label)
    except OSError as exc:
        raise PortableValidationBundleError(
            "FILE_READ_ERROR",
            f"{label}:{path}:{exc}",
        ) from exc


def _load_json_bytes(content: bytes, label: str) -> object:
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise PortableValidationBundleError(
            "JSON_UTF8",
            f"{label}:{exc}",
        ) from exc

    def no_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise PortableValidationBundleError(
                    "JSON_DUPLICATE_KEY",
                    f"{label}:{key}",
                )
            result[key] = value
        return result

    try:
        return json.loads(text, object_pairs_hook=no_duplicates)
    except PortableValidationBundleError:
        raise
    except (json.JSONDecodeError, ValueError) as exc:
        raise PortableValidationBundleError(
            "JSON_INVALID",
            f"{label}:{exc}",
        ) from exc


def _load_yaml_path(path: Path, label: str) -> object:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise PortableValidationBundleError(
            "FILE_READ_ERROR",
            f"{label}:{path}:{exc}",
        ) from exc
    except UnicodeDecodeError as exc:
        raise PortableValidationBundleError(
            "YAML_UTF8",
            f"{label}:{exc}",
        ) from exc
    try:
        return load_strict_yaml_text(text, options=_STRICT_YAML_OPTIONS)
    except StrictYamlError as exc:
        raise PortableValidationBundleError(
            f"YAML_{exc.code}",
            f"{label}:{exc}",
        ) from exc


def _repo_file(root: Path, path: Path, label: str) -> Path:
    candidate = path if path.is_absolute() else root / path
    resolved = candidate.resolve()
    if not resolved.is_relative_to(root):
        raise PortableValidationBundleError("PATH_OUTSIDE_PROJECT", f"{label}:{path}")
    if not resolved.is_file():
        raise PortableValidationBundleError("FILE_MISSING", f"{label}:{path}")
    return resolved


def _relative(path: Path, root: Path) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError as exc:
        raise PortableValidationBundleError("PATH_OUTSIDE_PROJECT", str(path)) from exc


def _portable(value: object, label: str) -> str:
    text = _text(value, label)
    path = PurePosixPath(text)
    if (
        path.is_absolute()
        or "\\" in text
        or ":" in text
        or not path.parts
        or any(part in {"", ".", ".."} for part in path.parts)
    ):
        raise PortableValidationBundleError("PATH_NOT_PORTABLE", f"{label}:{text}")
    return path.as_posix()


def _mapping(value: object, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise PortableValidationBundleError("EXPECTED_MAPPING", label)
    return value


def _maps(value: object, label: str) -> tuple[Mapping[str, Any], ...]:
    if not isinstance(value, list):
        raise PortableValidationBundleError("EXPECTED_LIST", label)
    return tuple(_mapping(row, f"{label}[{index}]") for index, row in enumerate(value))


def _exact(value: Mapping[str, Any], expected: set[str], code: str) -> None:
    if set(value) != expected:
        raise PortableValidationBundleError(
            code,
            f"missing={sorted(expected - set(value))} extra={sorted(set(value) - expected)}",
        )


def _text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise PortableValidationBundleError("EXPECTED_TEXT", label)
    return value.strip()


def _sha256_text(value: object, label: str) -> str:
    text = _text(value, label)
    if not _SHA256_RE.fullmatch(text):
        raise PortableValidationBundleError("SHA256_INVALID", f"{label}:{text}")
    return text


def _commit(value: object, label: str) -> str:
    text = _text(value, label)
    if not _COMMIT_RE.fullmatch(text):
        raise PortableValidationBundleError("COMMIT_INVALID", f"{label}:{text}")
    return text


def _sha256_path(path: Path) -> str:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError as exc:
        raise PortableValidationBundleError(
            "FILE_READ_ERROR",
            f"{path}:{exc}",
        ) from exc


def _git_process(root: Path, *args: str) -> subprocess.CompletedProcess[bytes]:
    try:
        return subprocess.run(
            ["git", *args],
            cwd=root,
            check=False,
            capture_output=True,
            shell=False,
        )
    except OSError as exc:
        raise PortableValidationBundleError(
            "GIT_HISTORY_UNAVAILABLE",
            str(exc),
        ) from exc


def _git_error(result: subprocess.CompletedProcess[bytes]) -> str:
    return result.stderr.decode("utf-8", errors="replace").strip() or "git command failed"


def _resolve_commit(root: Path, ref: str, label: str) -> str:
    result = _git_process(root, "rev-parse", "--verify", f"{ref}^{{commit}}")
    if result.returncode != 0:
        raise PortableValidationBundleError(
            "GIT_COMMIT_UNAVAILABLE",
            f"{label}:{ref}:{_git_error(result)}",
        )
    return _commit(result.stdout.decode("ascii").strip(), label)


def _require_git_commit(root: Path, commit: str, label: str) -> None:
    result = _git_process(root, "cat-file", "-e", f"{commit}^{{commit}}")
    if result.returncode != 0:
        raise PortableValidationBundleError(
            "GIT_COMMIT_UNAVAILABLE",
            f"{label}:{commit}:{_git_error(result)}",
        )


def _require_ancestor(root: Path, ancestor: str, descendant: str, code: str) -> None:
    result = _git_process(root, "merge-base", "--is-ancestor", ancestor, descendant)
    if result.returncode == 1:
        raise PortableValidationBundleError(
            code,
            f"ancestor={ancestor} descendant={descendant}",
        )
    if result.returncode != 0:
        raise PortableValidationBundleError(
            "GIT_HISTORY_UNAVAILABLE",
            _git_error(result),
        )


def _git_blob(root: Path, commit: str, path: str) -> bytes:
    result = _git_process(root, "show", f"{commit}:{_portable(path, 'git_blob.path')}")
    if result.returncode != 0:
        raise PortableValidationBundleError(
            "GIT_BLOB_UNAVAILABLE",
            f"{path}:{_git_error(result)}",
        )
    return result.stdout


if __name__ == "__main__":
    raise SystemExit(main())
