from __future__ import annotations

import base64
import copy
import hashlib
import json
import shutil
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path

import pytest

from ai_trading_system.platform.architecture.bootstrap_handoff import (
    bootstrap_handoff_checksum,
)
from ai_trading_system.platform.architecture.portable_validation_bundle import (
    BOOTSTRAP_VALIDATION_BUNDLE_SCHEMA_VERSION,
    DEFAULT_POLICY_PATH,
    PORTABLE_VALIDATION_REPORT_SCHEMA_VERSION,
    PortableValidationBundleError,
    validate_from_policy,
    validate_portable_bootstrap_bundle,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
BUNDLE_PATH = Path("inputs/architecture/arch_005_bootstrap_validation_bundle.json")
HANDOFF_PATH = Path("inputs/architecture/arch_005_bootstrap_handoff.yaml")
EXPECTED_RUNTIME_TIERS = {
    "focused": "fast-unit",
    "architecture_fitness": "architecture-fitness",
    "contract_validation": "contract-validation",
    "full_validation": "full",
}


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _copy_binding(tmp_path: Path) -> tuple[Path, Path]:
    bundle = tmp_path / BUNDLE_PATH
    handoff = tmp_path / HANDOFF_PATH
    bundle.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(PROJECT_ROOT / BUNDLE_PATH, bundle)
    shutil.copyfile(PROJECT_ROOT / HANDOFF_PATH, handoff)
    return bundle, handoff


def _write_json(path: Path, payload: object) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
        newline="\n",
    )


def _load_bundle(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_handoff(path: Path) -> dict[str, object]:
    payload = safe_load_yaml_path(path)
    assert isinstance(payload, dict)
    return payload


def _bind_handoff(
    bundle: dict[str, object],
    handoff_path: Path,
    handoff: dict[str, object],
) -> None:
    _write_json(handoff_path, handoff)
    bundle["source_handoff_sha256"] = _sha256(handoff_path)
    bundle["source_handoff_checksum"] = handoff["handoff_checksum"]


def _rewrite_artifact_summary(
    bundle: dict[str, object],
    handoff: dict[str, object],
    *,
    tier: str,
    mutate: Callable[[dict[str, object]], None],
) -> None:
    rows = bundle["artifacts"]
    assert isinstance(rows, list)
    row = next(item for item in rows if item["tier"] == tier)
    content = base64.b64decode(row["content_base64"], validate=True)
    summary = json.loads(content)
    mutate(summary)
    rewritten = json.dumps(
        summary,
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
    ).encode("utf-8") + b"\n"
    digest = hashlib.sha256(rewritten).hexdigest()
    row["content_base64"] = base64.b64encode(rewritten).decode("ascii")
    row["sha256"] = digest
    validation = handoff["validation_artifacts"]
    assert isinstance(validation, dict)
    validation[tier]["artifact_sha256"] = digest
    handoff["handoff_checksum"] = bootstrap_handoff_checksum(handoff)


def _validate_temp_binding(
    tmp_path: Path,
    *,
    source_base: str = "HEAD",
) -> dict[str, object]:
    bundle = tmp_path / BUNDLE_PATH
    return validate_portable_bootstrap_bundle(
        project_root=tmp_path,
        bundle_path=BUNDLE_PATH,
        expected_bundle_sha256=_sha256(bundle),
        handoff_path=HANDOFF_PATH,
        source_base_commit=source_base,
        git_project_root=PROJECT_ROOT,
    )


def test_canonical_policy_validates_exact_bundle_facts_and_git_lineage() -> None:
    report = validate_from_policy(
        project_root=PROJECT_ROOT,
        policy_path=DEFAULT_POLICY_PATH,
        source_base_commit="HEAD",
    )

    assert report["schema_version"] == PORTABLE_VALIDATION_REPORT_SCHEMA_VERSION
    assert report["status"] == "PASS"
    assert report["bundle"] == {
        "path": BUNDLE_PATH.as_posix(),
        "schema_version": BOOTSTRAP_VALIDATION_BUNDLE_SCHEMA_VERSION,
        "sha256": _sha256(PROJECT_ROOT / BUNDLE_PATH),
        "size_bytes": (PROJECT_ROOT / BUNDLE_PATH).stat().st_size,
        "artifact_count": 4,
    }
    assert report["handoff"]["canonical_validation"] == "PASS"
    assert report["handoff"]["frozen_tracked_file_count"] == 6
    assert report["git_lineage"]["status"] == "PASS"
    assert report["git_lineage"]["handoff_base_is_ancestor_of_handoff_head"] is True
    assert report["git_lineage"]["handoff_head_is_ancestor_of_source_base"] is True
    assert {
        row["bundle_tier"]: row["runtime_tier"] for row in report["artifacts"]
    } == EXPECTED_RUNTIME_TIERS
    assert all(row["size_bytes"] > 0 for row in report["artifacts"])
    assert all(row["status"] == "PASS" for row in report["artifacts"])
    assert all(row["exit_code"] == 0 for row in report["artifacts"])
    assert report["untracked_outputs_read"] is False
    assert report["production_effect"] == "none"
    assert report["broker_action"] == "none"


def test_standalone_cli_uses_tracked_policy_and_prints_content_derived_report() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "scripts/architecture_arch005_portable_validation_bundle.py",
            "--repository",
            ".",
            "--source-base",
            "HEAD",
        ],
        cwd=PROJECT_ROOT,
        check=False,
        capture_output=True,
        text=True,
        shell=False,
    )

    assert result.returncode == 0, result.stderr or result.stdout
    report = json.loads(result.stdout)
    assert report["status"] == "PASS"
    assert report["bundle"]["artifact_count"] == 4
    assert report["untracked_outputs_read"] is False


def test_missing_bundle_and_missing_git_history_fail_closed(tmp_path: Path) -> None:
    bundle, _ = _copy_binding(tmp_path)
    bundle.unlink()
    with pytest.raises(PortableValidationBundleError) as missing:
        validate_portable_bootstrap_bundle(
            project_root=tmp_path,
            bundle_path=BUNDLE_PATH,
            expected_bundle_sha256="0" * 64,
            handoff_path=HANDOFF_PATH,
            source_base_commit="HEAD",
            git_project_root=PROJECT_ROOT,
        )
    assert missing.value.code == "FILE_MISSING"

    _copy_binding(tmp_path)
    with pytest.raises(PortableValidationBundleError) as history:
        validate_portable_bootstrap_bundle(
            project_root=tmp_path,
            bundle_path=BUNDLE_PATH,
            expected_bundle_sha256=_sha256(tmp_path / BUNDLE_PATH),
            handoff_path=HANDOFF_PATH,
            source_base_commit="HEAD",
            git_project_root=tmp_path,
        )
    assert history.value.code == "GIT_COMMIT_UNAVAILABLE"


def test_bundle_file_hash_tamper_fails_before_decode(tmp_path: Path) -> None:
    bundle, _ = _copy_binding(tmp_path)
    bundle.write_bytes(bundle.read_bytes() + b" ")
    with pytest.raises(PortableValidationBundleError) as caught:
        validate_portable_bootstrap_bundle(
            project_root=tmp_path,
            bundle_path=BUNDLE_PATH,
            expected_bundle_sha256=_sha256(PROJECT_ROOT / BUNDLE_PATH),
            handoff_path=HANDOFF_PATH,
            source_base_commit="HEAD",
            git_project_root=PROJECT_ROOT,
        )
    assert caught.value.code == "VALIDATION_BUNDLE_FILE_HASH_DRIFT"


def test_crlf_normalization_is_rejected_even_when_base64_is_valid(tmp_path: Path) -> None:
    bundle_path, _ = _copy_binding(tmp_path)
    bundle = _load_bundle(bundle_path)
    row = bundle["artifacts"][0]
    content = base64.b64decode(row["content_base64"], validate=True)
    assert b"\r\n" in content
    row["content_base64"] = base64.b64encode(content.replace(b"\r\n", b"\n")).decode("ascii")
    _write_json(bundle_path, bundle)

    with pytest.raises(PortableValidationBundleError) as caught:
        _validate_temp_binding(tmp_path)
    assert caught.value.code == "VALIDATION_BUNDLE_CONTENT_HASH_DRIFT"


@pytest.mark.parametrize(
    ("field", "replacement", "expected_code"),
    [
        ("status", "FAIL", "VALIDATION_BUNDLE_ARTIFACT_STATUS"),
        ("exit_code", 7, "VALIDATION_BUNDLE_ARTIFACT_EXIT_CODE"),
        ("tier", "contract-validation", "VALIDATION_BUNDLE_ARTIFACT_TIER"),
    ],
)
def test_summary_status_exit_and_runtime_tier_drift_fail_closed(
    tmp_path: Path,
    field: str,
    replacement: object,
    expected_code: str,
) -> None:
    bundle_path, handoff_path = _copy_binding(tmp_path)
    bundle = _load_bundle(bundle_path)
    handoff = _load_handoff(handoff_path)

    def mutate(summary: dict[str, object]) -> None:
        if field == "tier":
            for key in ("tier", "requested_tier", "resolved_tier"):
                summary[key] = replacement
        else:
            summary[field] = replacement

    _rewrite_artifact_summary(bundle, handoff, tier="focused", mutate=mutate)
    _bind_handoff(bundle, handoff_path, handoff)
    _write_json(bundle_path, bundle)

    with pytest.raises(PortableValidationBundleError) as caught:
        _validate_temp_binding(tmp_path)
    assert caught.value.code == expected_code


def test_path_hash_missing_tier_and_duplicate_tier_fail_closed(tmp_path: Path) -> None:
    bundle_path, _ = _copy_binding(tmp_path)
    canonical = _load_bundle(bundle_path)

    path_drift = copy.deepcopy(canonical)
    path_drift["artifacts"][0]["original_path"] = "outputs/other.json"
    _write_json(bundle_path, path_drift)
    with pytest.raises(PortableValidationBundleError) as path_error:
        _validate_temp_binding(tmp_path)
    assert path_error.value.code == "VALIDATION_BUNDLE_PATH_DRIFT"

    hash_drift = copy.deepcopy(canonical)
    hash_drift["artifacts"][0]["sha256"] = "0" * 64
    _write_json(bundle_path, hash_drift)
    with pytest.raises(PortableValidationBundleError) as hash_error:
        _validate_temp_binding(tmp_path)
    assert hash_error.value.code == "VALIDATION_BUNDLE_HANDOFF_HASH_DRIFT"

    missing = copy.deepcopy(canonical)
    missing["artifacts"].pop()
    missing["artifact_count"] = 3
    _write_json(bundle_path, missing)
    with pytest.raises(PortableValidationBundleError) as missing_error:
        _validate_temp_binding(tmp_path)
    assert missing_error.value.code == "VALIDATION_BUNDLE_TIER_SET"

    duplicate = copy.deepcopy(canonical)
    duplicate["artifacts"][-1]["tier"] = "focused"
    _write_json(bundle_path, duplicate)
    with pytest.raises(PortableValidationBundleError) as duplicate_error:
        _validate_temp_binding(tmp_path)
    assert duplicate_error.value.code == "VALIDATION_BUNDLE_DUPLICATE_TIER"


def test_handoff_checksum_and_source_base_drift_fail_closed(tmp_path: Path) -> None:
    bundle_path, handoff_path = _copy_binding(tmp_path)
    bundle = _load_bundle(bundle_path)
    handoff = _load_handoff(handoff_path)
    handoff["handoff_checksum"] = "0" * 64
    _bind_handoff(bundle, handoff_path, handoff)
    _write_json(bundle_path, bundle)

    with pytest.raises(PortableValidationBundleError) as checksum:
        _validate_temp_binding(tmp_path)
    assert checksum.value.code == "HANDOFF_CHECKSUM_DRIFT"

    _copy_binding(tmp_path)
    canonical_handoff = _load_handoff(tmp_path / HANDOFF_PATH)
    with pytest.raises(PortableValidationBundleError) as lineage:
        _validate_temp_binding(
            tmp_path,
            source_base=str(canonical_handoff["base_commit"]),
        )
    assert lineage.value.code == "HANDOFF_HEAD_SOURCE_BASE_LINEAGE"

    with pytest.raises(PortableValidationBundleError) as unknown:
        _validate_temp_binding(tmp_path, source_base="definitely-not-a-commit")
    assert unknown.value.code == "GIT_COMMIT_UNAVAILABLE"


def test_clean_local_clone_cli_needs_no_validation_outputs(tmp_path: Path) -> None:
    clone = tmp_path / "clean-clone"
    subprocess.run(
        [
            "git",
            "clone",
            "--quiet",
            "--no-hardlinks",
            str(PROJECT_ROOT),
            str(clone),
        ],
        check=True,
        capture_output=True,
        shell=False,
    )
    assert not (clone / "outputs" / "validation_runtime").exists()
    result = subprocess.run(
        [
            sys.executable,
            "scripts/architecture_arch005_portable_validation_bundle.py",
            "--repository",
            ".",
            "--source-base",
            "HEAD",
        ],
        cwd=clone,
        check=False,
        capture_output=True,
        text=True,
        shell=False,
    )
    assert result.returncode == 0, result.stderr or result.stdout
    report = json.loads(result.stdout)
    assert report["status"] == "PASS"
    assert report["untracked_outputs_read"] is False
