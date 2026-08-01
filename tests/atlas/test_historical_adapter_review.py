from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from pathlib import Path

import pytest
import yaml

from ai_trading_system.atlas import historical_adapter_review as review_module
from ai_trading_system.atlas.historical_adapter_review import (
    NEEDS_SCHEMA_NORMALIZATION,
    NEEDS_SOURCE_REGISTRATION,
    READY_FOR_OWNER_ADAPTER_REVIEW,
    HistoricalAdapterReviewError,
    build_historical_adapter_review,
    render_historical_adapter_review_markdown,
    validate_historical_adapter_review,
    write_historical_adapter_review_artifacts,
)

COMMIT = "1" * 40
POLICY_PATH = "config/atlas/historical_adapter_review_policy.yaml"
JSON_PATH = "docs/research/example_result.json"
MARKDOWN_PATH = "docs/research/example_result.md"
INVENTORY_PATH = "outputs/atlas/inventory.json"


def _inventory_bytes() -> bytes:
    payload = {
        "schema_version": "atlas_historical_coverage_inventory.v1",
        "inventory_id": "inventory_test",
        "tracked_path_records": [
            {
                "path": JSON_PATH,
                "classification": "TRACKED_UNREGISTERED_REVIEW_REQUIRED",
            },
            {
                "path": MARKDOWN_PATH,
                "classification": "TRACKED_UNREGISTERED_REVIEW_REQUIRED",
            },
        ],
    }
    return (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _policy_bytes(*, include_lineage: bool = True) -> bytes:
    inventory_bytes = _inventory_bytes()
    lineage_fields = ["source_artifacts"] if include_lineage else ["unavailable_lineage"]
    payload = {
        "schema_version": "atlas_historical_adapter_review_policy.v1",
        "policy_id": "TEST_POLICY",
        "policy_version": 1,
        "status": "REVIEWED_PILOT_V1",
        "owner": "test",
        "rationale": "test",
        "review_condition": "test",
        "inventory": {
            "path": INVENTORY_PATH,
            "expected_inventory_id": "inventory_test",
            "expected_sha256": hashlib.sha256(inventory_bytes).hexdigest(),
            "required_schema_version": "atlas_historical_coverage_inventory.v1",
            "required_classification": "TRACKED_UNREGISTERED_REVIEW_REQUIRED",
        },
        "excluded_paths": ["docs/research/growth_tilt_owner_diagnosis_pack.md"],
        "candidate_families": [
            {
                "candidate_family_id": "candidate_test",
                "role_code": "BASELINE",
                "json_path": JSON_PATH,
                "markdown_path": MARKDOWN_PATH,
            }
        ],
        "required_slots": [
            "identity",
            "window",
            "lineage",
            "result_or_status",
            "limitation",
        ],
        "slot_rules": {
            "identity": {"exact_fields": ["task_id"], "suffixes": []},
            "window": {"exact_fields": ["window"], "suffixes": []},
            "lineage": {"exact_fields": lineage_fields, "suffixes": []},
            "result_or_status": {"exact_fields": ["status"], "suffixes": ["_status"]},
            "limitation": {"exact_fields": ["limitations"], "suffixes": []},
        },
        "identity_value_fields": ["task_id"],
        "markdown_tokens": ["status", "result"],
        "max_pointer_records_per_candidate": 100,
        "max_markdown_title_characters": 160,
        "max_identity_token_characters": 128,
        "disposition_codes": [
            "READY_FOR_OWNER_ADAPTER_REVIEW",
            "NEEDS_SCHEMA_NORMALIZATION",
            "NEEDS_SOURCE_REGISTRATION",
            "REJECTED_FROM_FIRST_BATCH",
        ],
        "safety": {
            "candidate_artifact_content_read_count": 12,
            "allowlist_outside_research_content_read_count": 0,
            "source_registration_performed": False,
            "atlas_result_projection_performed": False,
            "research_value_projection_performed": False,
            "investment_conclusion_generated": False,
            "production_effect": "none",
            "broker_action": "none",
        },
    }
    return yaml.safe_dump(payload, sort_keys=False, allow_unicode=True).encode()


def _candidate_bytes() -> tuple[bytes, bytes]:
    payload = {
        "task_id": "TASK-EXAMPLE",
        "window": {"start": "2021-02-22", "end": "2024-01-01"},
        "source_artifacts": ["source.json"],
        "status": "VALUE_MUST_NOT_BE_SERIALIZED",
        "limitations": ["also must not be serialized"],
    }
    json_bytes = json.dumps(payload, sort_keys=True).encode()
    markdown_bytes = b"# Example Result\n\nTASK-EXAMPLE status result\n"
    return json_bytes, markdown_bytes


def _install_fake_git_inputs(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    include_lineage: bool = True,
) -> list[str]:
    inventory_file = tmp_path / INVENTORY_PATH
    inventory_file.parent.mkdir(parents=True)
    inventory_file.write_bytes(_inventory_bytes())
    json_bytes, markdown_bytes = _candidate_bytes()
    blobs = {
        POLICY_PATH: _policy_bytes(include_lineage=include_lineage),
        JSON_PATH: json_bytes,
        MARKDOWN_PATH: markdown_bytes,
    }
    reads: list[str] = []

    def fake_blob_bytes(_root: Path, _commit: str, path: str) -> bytes:
        reads.append(path)
        return blobs[path]

    def fake_blob_sha1(_root: Path, _commit: str, path: str) -> str:
        reads.append(f"sha:{path}")
        return hashlib.sha1(blobs[path], usedforsecurity=False).hexdigest()

    monkeypatch.setattr(review_module, "_git_blob_bytes", fake_blob_bytes)
    monkeypatch.setattr(review_module, "_git_blob_sha1", fake_blob_sha1)
    return reads


def test_builder_is_deterministic_and_never_serializes_research_values(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    reads = _install_fake_git_inputs(monkeypatch, tmp_path)
    first = build_historical_adapter_review(
        repository_root=tmp_path, exact_commit=COMMIT, policy_repository_path=POLICY_PATH
    )
    second = build_historical_adapter_review(
        repository_root=tmp_path, exact_commit=COMMIT, policy_repository_path=POLICY_PATH
    )
    assert first.canonical_json_bytes() == second.canonical_json_bytes()
    record = first.candidate_records[0]
    assert record["disposition"] == NEEDS_SOURCE_REGISTRATION
    assert record["owner_review_readiness"] == READY_FOR_OWNER_ADAPTER_REVIEW
    slot_coverage = record["slot_coverage"]
    assert isinstance(slot_coverage, dict)
    assert all(slot_coverage.values())
    assert b"VALUE_MUST_NOT_BE_SERIALIZED" not in first.canonical_json_bytes()
    assert b"also must not be serialized" not in first.canonical_json_bytes()
    assert "docs/research/growth_tilt_owner_diagnosis_pack.md" not in reads


def test_missing_required_slot_needs_schema_normalization(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _install_fake_git_inputs(monkeypatch, tmp_path, include_lineage=False)
    pack = build_historical_adapter_review(
        repository_root=tmp_path, exact_commit=COMMIT, policy_repository_path=POLICY_PATH
    )
    record = pack.candidate_records[0]
    assert record["disposition"] == NEEDS_SCHEMA_NORMALIZATION
    assert record["missing_required_slots"] == ("lineage",)


def test_inventory_sha_or_classification_drift_fails_closed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _install_fake_git_inputs(monkeypatch, tmp_path)
    inventory_file = tmp_path / INVENTORY_PATH
    inventory_file.write_bytes(inventory_file.read_bytes() + b" ")
    with pytest.raises(HistoricalAdapterReviewError, match="INVENTORY_SHA_MISMATCH"):
        build_historical_adapter_review(
            repository_root=tmp_path,
            exact_commit=COMMIT,
            policy_repository_path=POLICY_PATH,
        )


def test_validation_detects_pack_tamper(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _install_fake_git_inputs(monkeypatch, tmp_path)
    pack = build_historical_adapter_review(
        repository_root=tmp_path, exact_commit=COMMIT, policy_repository_path=POLICY_PATH
    )
    tampered_summary = {**pack.summary, "candidate_family_count": 99}
    validation = validate_historical_adapter_review(
        replace(pack, summary=tampered_summary),
        repository_root=tmp_path,
        policy_repository_path=POLICY_PATH,
    )
    assert validation.status == "FAIL"
    assert validation.errors == ("REVIEW_PACK_CANONICAL_REBUILD_MISMATCH",)


def test_writer_is_byte_deterministic_and_markdown_stays_non_projecting(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _install_fake_git_inputs(monkeypatch, tmp_path)
    pack = build_historical_adapter_review(
        repository_root=tmp_path, exact_commit=COMMIT, policy_repository_path=POLICY_PATH
    )
    output = tmp_path / "out"
    first = write_historical_adapter_review_artifacts(
        pack,
        output,
        repository_root=tmp_path,
        policy_repository_path=POLICY_PATH,
    )
    before = {item.path: (item.sha256, item.size_bytes) for item in first}
    second = write_historical_adapter_review_artifacts(
        pack,
        output,
        repository_root=tmp_path,
        policy_repository_path=POLICY_PATH,
    )
    after = {item.path: (item.sha256, item.size_bytes) for item in second}
    assert before == after
    markdown = render_historical_adapter_review_markdown(pack)
    assert "本包不是策略结论" in markdown
    assert "VALUE_MUST_NOT_BE_SERIALIZED" not in markdown
