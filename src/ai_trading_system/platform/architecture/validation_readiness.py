"""Read-only admission of known Full dependencies (TRADING-2564 S1).

This is an early diagnostic, not research replay or a replacement for pytest.
The finite adapters below read existing commitments; they never search historical
outputs, hydrate evidence, execute DQ, render Atlas, or generate authority.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import subprocess
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path, PurePosixPath
from time import perf_counter
from typing import Any

from ai_trading_system.yaml_loader import load_strict_yaml_text

# These are dependency adapters for the exact retained-evidence failures recorded
# in TRADING-2563, not a discovery rule or a second source of expected hashes.
RESULT_ADMISSIONS = (
    "config/research/first_layer_composer_v2_foundational_falsification_result_admission_v1.yaml",
    "config/research/first_layer_composer_v2_foundational_falsification_failure_fix_result_admission_v1.yaml",
    "config/research/first_layer_composer_v2_matched_placebo_result_admission_v1.yaml",
    "config/research/first_layer_composer_v2_temporal_influence_failure_fix_result_admission_v1.yaml",
)
O1_POLICY = "config/research/o1_relative_opportunity_capability_audit_v1.yaml"
SIGNAL_POLICY = (
    "config/research/qc_qqq_options_exact_signal_implementation_backtest_execution_v1.yaml"
)
CHECKER_IDS = (
    "candidate_identity",
    "retained_evidence",
    "canonical_tasks",
    "atlas_final_binding",
    "architecture_generated",
    "report_flow_authority",
    "compatibility_authority",
)
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_COMMIT = re.compile(r"[0-9a-f]{40}\Z")
# TRADING-2564 invariant: the inspector and its two entry points must exist in
# the candidate, and their implementation tree must match that candidate. This
# finite literal allowlist never inventories or reads excluded user documents.
_INSPECTION_REQUIRED_FILES = (
    "src/ai_trading_system/platform/architecture/validation_readiness.py",
    "scripts/run_validation_tier.py",
    "scripts/validation_readiness.py",
)
_INSPECTION_CODE_PATHS = (
    ":(literal)src",
    ":(literal)scripts/run_validation_tier.py",
    ":(literal)scripts/validation_readiness.py",
)


class ValidationReadinessError(ValueError):
    def __init__(self, code: str, detail: str) -> None:
        self.code = code
        self.detail = detail
        super().__init__(f"{code}: {detail}")


def _mapping(value: object, label: str) -> Mapping[str, Any]:
    if not isinstance(value, dict):
        raise ValidationReadinessError("READINESS_BINDING_SCHEMA", label)
    return value


def _rows(value: object, label: str) -> list[Mapping[str, Any]]:
    if not isinstance(value, list) or not value:
        raise ValidationReadinessError("READINESS_BINDING_SCHEMA", label)
    return [_mapping(row, label) for row in value]


def _portable(value: object) -> str:
    if not isinstance(value, str) or not value or "\\" in value or ":" in value:
        raise ValidationReadinessError("READINESS_PATH_INVALID", str(value))
    path = PurePosixPath(value)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in value.split("/")):
        raise ValidationReadinessError("READINESS_PATH_INVALID", value)
    # This finite admission never opens user documents or market caches.
    if path.parts[0] not in {"config", "inputs", "outputs"}:
        raise ValidationReadinessError("READINESS_PATH_OUT_OF_SCOPE", value)
    return path.as_posix()


def _regular_file(root: Path, portable: object) -> Path:
    relative = _portable(portable)
    path = root
    root_stat = root.lstat()
    if stat.S_ISLNK(root_stat.st_mode) or (
        getattr(root_stat, "st_file_attributes", 0) & stat.FILE_ATTRIBUTE_REPARSE_POINT
    ):
        raise ValidationReadinessError("READINESS_REPARSE_PATH", str(root))
    for part in PurePosixPath(relative).parts:
        path = path / part
        try:
            metadata = path.lstat()
        except FileNotFoundError as exc:
            raise ValidationReadinessError("READINESS_DEPENDENCY_MISSING", relative) from exc
        if stat.S_ISLNK(metadata.st_mode) or (
            getattr(metadata, "st_file_attributes", 0) & stat.FILE_ATTRIBUTE_REPARSE_POINT
        ):
            raise ValidationReadinessError("READINESS_REPARSE_PATH", relative)
    if not path.is_file() or not path.resolve().is_relative_to(root):
        raise ValidationReadinessError("READINESS_PATH_NOT_REGULAR", relative)
    return path


def _git(root: Path, *arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", "-c", f"safe.directory={root.as_posix()}", *arguments],
        cwd=root,
        env={**os.environ, "GIT_OPTIONAL_LOCKS": "0"},
        capture_output=True,
        text=True,
        check=False,
    )


def _candidate_identity(root: Path, candidate: str) -> dict[str, Any]:
    _validate_candidate_sha(candidate)
    observed = _git(root, "rev-parse", "HEAD")
    if observed.returncode != 0 or observed.stdout.strip() != candidate:
        raise ValidationReadinessError("READINESS_CANDIDATE_MISMATCH", candidate)
    return {"status": "PASS", "candidate_sha": candidate}


def _validate_candidate_sha(candidate: str) -> None:
    if not isinstance(candidate, str) or not _COMMIT.fullmatch(candidate):
        raise ValidationReadinessError("READINESS_CANDIDATE_INVALID", str(candidate))


def _inspection_code_identity(root: Path, candidate: str) -> None:
    """Fail before canonical validators when live implementation is not committed."""
    _candidate_identity(root, candidate)
    for relative in _INSPECTION_REQUIRED_FILES:
        present = _git(root, "cat-file", "-e", f"{candidate}:{relative}")
        if present.returncode != 0:
            raise ValidationReadinessError("READINESS_INSPECTION_CODE_NOT_COMMITTED", relative)
    for flags, label in (((), "working-tree"), (("--cached",), "index")):
        clean = _git(
            root,
            "diff",
            *flags,
            "--quiet",
            "--no-ext-diff",
            "--no-textconv",
            candidate,
            "--",
            *_INSPECTION_CODE_PATHS,
        )
        if clean.returncode == 1:
            raise ValidationReadinessError("READINESS_INSPECTION_CODE_DIRTY", label)
        if clean.returncode != 0:
            raise ValidationReadinessError("READINESS_INSPECTION_CODE_CHECK_FAILED", label)
    untracked = _git(
        root, "ls-files", "--others", "--exclude-standard", "-z", "--", *_INSPECTION_CODE_PATHS
    )
    if untracked.returncode != 0:
        raise ValidationReadinessError("READINESS_INSPECTION_CODE_CHECK_FAILED", "untracked names")
    if untracked.stdout:
        raise ValidationReadinessError(
            "READINESS_INSPECTION_CODE_UNTRACKED", untracked.stdout.replace("\0", ";")
        )


def _committed_yaml(root: Path, candidate: str, relative: str) -> Mapping[str, Any]:
    _validate_candidate_sha(candidate)
    path = _regular_file(root, relative)
    present = _git(root, "cat-file", "-e", f"{candidate}:{relative}")
    clean = _git(
        root, "diff", "--quiet", "--no-ext-diff", "--no-textconv", candidate, "--", relative
    )
    if present.returncode != 0 or clean.returncode != 0:
        raise ValidationReadinessError("READINESS_AUTHORITY_NOT_COMMITTED", relative)
    return _mapping(
        load_strict_yaml_text(path.read_text(encoding="utf-8"), label=relative), relative
    )


def _binding_bytes(
    root: Path, binding: Mapping[str, Any], *, path_key: str = "path", sha_key: str = "sha256"
) -> bytes:
    relative = _portable(binding.get(path_key))
    expected = binding.get(sha_key)
    if not isinstance(expected, str) or not _SHA256.fullmatch(expected):
        raise ValidationReadinessError("READINESS_DIGEST_INVALID", relative)
    path = _regular_file(root, relative)
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != expected:
        raise ValidationReadinessError("READINESS_DEPENDENCY_HASH_MISMATCH", relative)
    expected_size = binding.get("size_bytes", binding.get("byte_count"))
    if expected_size is not None and (
        type(expected_size) is not int or expected_size < 0 or len(raw) != expected_size
    ):
        raise ValidationReadinessError("READINESS_DEPENDENCY_SIZE_MISMATCH", relative)
    return raw


def _json_mapping(raw: bytes, label: str) -> Mapping[str, Any]:
    def unique_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValidationReadinessError("READINESS_JSON_DUPLICATE_KEY", label)
            result[key] = value
        return result

    def invalid_constant(value: str) -> Any:
        raise ValidationReadinessError("READINESS_JSON_NONFINITE", label)

    return _mapping(
        json.loads(raw, object_pairs_hook=unique_pairs, parse_constant=invalid_constant), label
    )


def _blocker(checker: str, exc: Exception) -> dict[str, str]:
    return {
        "checker_id": checker,
        "code": str(getattr(exc, "code", "READINESS_CHECK_FAILED")),
        "detail": str(exc),
    }


class _EvidenceCheck:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.checked: set[str] = set()
        self.blockers: list[dict[str, str]] = []

    def bindings(self, rows: Sequence[Mapping[str, Any]], *, sha_key: str = "sha256") -> None:
        seen: set[str] = set()
        for row in rows:
            try:
                relative = _portable(row.get("path"))
                if relative in seen:
                    raise ValidationReadinessError("READINESS_DUPLICATE_DEPENDENCY", relative)
                seen.add(relative)
                _binding_bytes(self.root, row, sha_key=sha_key)
                self.checked.add(relative)
            except (ValueError, OSError) as exc:
                self.blockers.append(_blocker("retained_evidence", exc))


def _signal_dependencies(evidence: _EvidenceCheck, root: Path, candidate: str) -> None:
    policy = _committed_yaml(root, candidate, SIGNAL_POLICY)
    evidence.bindings(_rows(policy.get("authority_bindings"), SIGNAL_POLICY), sha_key="file_sha256")
    package = _mapping(policy.get("signal_package"), "signal_package")
    package_root = _portable(package.get("root"))
    package_bindings = [
        {"path": f"{package_root}/{name}.json", "sha256": package.get(f"{name}_sha256")}
        for name in ("package_receipt", "signal_index", "run_manifest")
    ]
    evidence.bindings(package_bindings)
    # Only a receipt already bound by the committed execution policy may supply
    # daily-file identities. No recursive search or invocation of package replay.
    raw = _binding_bytes(root, package_bindings[0])
    receipt = _json_mapping(raw, "signal_package_receipt")
    daily = _rows(receipt.get("daily_signal_artifacts"), "daily_signal_artifacts")
    daily_bindings = []
    for row in daily:
        relative = row.get("relative_path")
        if not isinstance(relative, str):
            raise ValidationReadinessError("READINESS_BINDING_SCHEMA", "daily relative_path")
        daily_bindings.append({**row, "path": f"{package_root}/{relative}"})
    evidence.bindings(daily_bindings)
    source = _mapping(receipt.get("source_artifact"), "source_artifact")
    evidence.bindings([{**source, "path": source.get("locator")}])


def _retained_evidence(root: Path, candidate: str) -> dict[str, Any]:
    evidence = _EvidenceCheck(root)
    for relative in RESULT_ADMISSIONS:
        try:
            payload = _committed_yaml(root, candidate, relative)
            evidence.bindings(
                _rows(payload.get("evidence_bindings"), relative), sha_key="file_sha256"
            )
        except (ValueError, OSError, subprocess.SubprocessError) as exc:
            evidence.blockers.append(_blocker("retained_evidence", exc))
    try:
        o1 = _committed_yaml(root, candidate, O1_POLICY)
        isolated = _mapping(o1.get("isolated_dq_evidence"), "isolated_dq_evidence")
        evidence.bindings([_mapping(isolated.get("gate"), "isolated_dq_evidence.gate")])
    except (ValueError, OSError, subprocess.SubprocessError) as exc:
        evidence.blockers.append(_blocker("retained_evidence", exc))
    try:
        _signal_dependencies(evidence, root, candidate)
    except (ValueError, OSError, subprocess.SubprocessError) as exc:
        evidence.blockers.append(_blocker("retained_evidence", exc))
    return {
        "status": "BLOCKED" if evidence.blockers else "PASS",
        "verified_dependency_count": len(evidence.checked),
        "inventory_scope": "EXPLICIT_CURRENT_ADMISSION_BINDINGS_ONLY",
        "blockers": evidence.blockers,
    }


def _canonical_tasks(root: Path, candidate: str) -> dict[str, Any]:
    from ai_trading_system.platform.architecture.task_registry_canonical import (
        validate_canonical_registry,
    )

    registry = validate_canonical_registry(project_root=root)
    if registry.index.get("status") != "PASS":
        raise ValidationReadinessError("READINESS_TASK_REGISTRY_INVALID", "canonical task index")
    return {"status": "PASS", "task_count": registry.index["task_count"]}


def _atlas_final_binding(root: Path, candidate: str) -> dict[str, Any]:
    from ai_trading_system.atlas.page_effectiveness import (
        load_page_effectiveness_policy,
        validate_page_effectiveness_manifest,
    )
    from ai_trading_system.contracts.strategy_research_page_effectiveness import (
        PageFreshnessStatus,
        StrategyResearchPageEffectivenessManifest,
    )

    policy = load_page_effectiveness_policy(repository_root=root)
    raw = _regular_file(root, policy.manifest_path).read_bytes()
    manifest = StrategyResearchPageEffectivenessManifest.from_json_bytes(raw)
    for identity in (*manifest.source_artifacts, *manifest.rendered_artifacts):
        # Source documents are checked by the canonical Atlas validator. The
        # runtime payloads themselves must not escape through reparse points.
        if identity.locator.startswith("outputs/"):
            _regular_file(root, identity.locator)
    validation = validate_page_effectiveness_manifest(
        repository_root=root, manifest=manifest, current_repository_commit=candidate
    )
    errors = list(validation.errors)
    if manifest.repository_commit != candidate or manifest.source_snapshot_commit != candidate:
        errors.append("READINESS_ATLAS_CANDIDATE_MISMATCH")
    if (
        validation.freshness_status is not PageFreshnessStatus.CURRENT
        or manifest.freshness_status is not PageFreshnessStatus.CURRENT
    ):
        errors.append("READINESS_ATLAS_NOT_CURRENT")
    if validation.status != "PASS" or errors:
        raise ValidationReadinessError("READINESS_ATLAS_INVALID", ";".join(errors))
    return {
        "status": "PASS",
        "manifest_sha256": manifest.content_sha256,
        "freshness_status": "CURRENT",
    }


def _architecture_generated(root: Path, candidate: str) -> dict[str, Any]:
    from ai_trading_system.platform.architecture.devex import build_architecture_fitness

    result = build_architecture_fitness(
        project_root=root,
        policy_path=root / "config/architecture/devex_ownership_policy.yaml",
        module_manifest_path=root / "inputs/architecture/arch_004e_module_manifest.yaml",
        test_manifest_path=root / "inputs/architecture/arch_004e_test_manifest.yaml",
        aggregate_index_path=root / "inputs/architecture/arch_004e_aggregate_shadow_index.yaml",
        dependency_policy_path=root / "config/architecture/arch_004c_dependency_policy.yaml",
        direct_writer_baseline_path=root
        / "inputs/architecture/arch_004c_direct_writer_baseline.yaml",
    )
    if result.get("status") != "PASS":
        raise ValidationReadinessError("READINESS_ARCHITECTURE_STALE", json.dumps(result))
    return {"status": "PASS"}


def _report_flow(root: Path, candidate: str) -> dict[str, Any]:
    from ai_trading_system.platform.architecture.report_catalog_flow_authority import (
        validate_repository_authority,
    )

    result = validate_repository_authority(root)
    if result.get("status") != "PASS":
        raise ValidationReadinessError("READINESS_REPORT_FLOW_INVALID", "report-flow authority")
    return {"status": "PASS"}


def _compatibility(root: Path, candidate: str) -> dict[str, Any]:
    from ai_trading_system.platform.architecture.compatibility_authority import (
        validate_repository_authority,
    )

    result = validate_repository_authority(root)
    if result.get("status") != "PASS":
        raise ValidationReadinessError("READINESS_COMPATIBILITY_INVALID", "compatibility authority")
    return {"status": "PASS"}


def _checkers() -> Mapping[str, Callable[[Path, str], dict[str, Any]]]:
    return dict(
        zip(
            CHECKER_IDS,
            (
                _candidate_identity,
                _retained_evidence,
                _canonical_tasks,
                _atlas_final_binding,
                _architecture_generated,
                _report_flow,
                _compatibility,
            ),
            strict=True,
        )
    )


def _inspection_code_root() -> Path:
    return Path(__file__).resolve().parents[4]


def check_full_readiness(project_root: Path, candidate_sha: str) -> dict[str, Any]:
    """Aggregate read-only checks without consuming a publication or run claim."""
    started = perf_counter()
    inspection_code_root = _inspection_code_root()
    root = project_root.absolute()
    checks: list[dict[str, Any]] = []
    blockers: list[dict[str, str]] = []
    try:
        # Validate before any candidate reaches Git argv or any checker runs.
        _validate_candidate_sha(candidate_sha)
        for ancestor in (*root.parents, root):
            metadata = ancestor.lstat()
            if stat.S_ISLNK(metadata.st_mode) or (
                getattr(metadata, "st_file_attributes", 0) & stat.FILE_ATTRIBUTE_REPARSE_POINT
            ):
                raise ValidationReadinessError("READINESS_REPARSE_PATH", str(ancestor))
        if not root.is_dir():
            raise ValidationReadinessError("READINESS_ROOT_NOT_DIRECTORY", str(root))
        root = root.resolve()
        if root != inspection_code_root:
            raise ValidationReadinessError(
                "READINESS_INSPECTION_ROOT_MISMATCH",
                f"inspection={inspection_code_root};target={root}",
            )
        # Not a registry adapter: missing/replaced checker inventory must never
        # bypass the live-source/candidate boundary in this standalone entry.
        _inspection_code_identity(root, candidate_sha)
    except (ValueError, OSError, subprocess.SubprocessError) as exc:
        return _result(
            candidate_sha,
            checks,
            [_blocker("candidate_identity", exc)],
            elapsed_seconds=perf_counter() - started,
            inspection_code_root=inspection_code_root,
            target_root=root,
        )
    checkers = _checkers()
    if tuple(checkers) != CHECKER_IDS:
        blockers.append(
            {
                "checker_id": "checker_inventory",
                "code": "READINESS_CHECKER_INVENTORY_INVALID",
                "detail": "必需检查器缺失、重复顺序或存在未知检查器",
            }
        )
    for checker_id in CHECKER_IDS:
        checker = checkers.get(checker_id)
        if checker is None:
            continue
        checker_started = perf_counter()
        try:
            details = checker(root, candidate_sha)
            if not isinstance(details, dict):
                raise ValidationReadinessError("READINESS_CHECK_RESULT_INVALID", checker_id)
            failures = details.pop("blockers", [])
            if not isinstance(failures, list) or any(
                not isinstance(row, dict)
                or set(row) != {"checker_id", "code", "detail"}
                or any(not isinstance(value, str) for value in row.values())
                for row in failures
            ):
                raise ValidationReadinessError("READINESS_CHECK_RESULT_INVALID", checker_id)
            expected_status = "BLOCKED" if failures else "PASS"
            if details.get("status") != expected_status:
                raise ValidationReadinessError("READINESS_CHECK_RESULT_INVALID", checker_id)
            blockers.extend(failures)
            checks.append(
                {"checker_id": checker_id, "status": "BLOCKED" if failures else "PASS", **details}
            )
        except Exception as exc:
            blockers.append(_blocker(checker_id, exc))
            checks.append({"checker_id": checker_id, "status": "BLOCKED"})
        finally:
            checks[-1]["elapsed_seconds"] = round(perf_counter() - checker_started, 6)
    return _result(
        candidate_sha,
        checks,
        blockers,
        elapsed_seconds=perf_counter() - started,
        inspection_code_root=inspection_code_root,
        target_root=root,
    )


def _result(
    candidate_sha: str,
    checks: list[dict[str, Any]],
    blockers: list[dict[str, str]],
    *,
    elapsed_seconds: float,
    inspection_code_root: Path,
    target_root: Path,
) -> dict[str, Any]:
    return {
        "schema_version": "full_validation_readiness.v1",
        "status": "BLOCKED" if blockers else "PASS",
        "candidate_sha": candidate_sha,
        "inspection_code_root": inspection_code_root.as_posix(),
        "target_root": target_root.as_posix(),
        "elapsed_seconds": round(elapsed_seconds, 6),
        "full_dispatch_ready": not blockers,
        "checks": checks,
        "blockers": blockers,
        "dispatch_performed": False,
        "research_dispatch_allowed": False,
        "dq_validation_executed": False,
        "artifacts_written": False,
        "production_effect": "none",
        "broker_action": "none",
        "limitations": [
            "仅核查明确列出的当前证据依赖，不声称覆盖所有历史输出。",
            "固定数量的独立测试断言仍须聚焦测试，PASS不替代正式Full或研究DQ/PIT。",
        ],
    }
