from __future__ import annotations

import subprocess
from dataclasses import replace
from pathlib import Path

import pytest
import yaml

import ai_trading_system.atlas.historical_coverage_inventory as inventory_module
from ai_trading_system.atlas.historical_coverage_inventory import (
    ATLAS_SOURCE_BOUND,
    CLASSIFICATION_CODES,
    DECLARED_NON_TRACKED_OR_RUNTIME_ARTIFACT,
    MANDATORY_EXCLUDED_REPOSITORY_PATH,
    REGISTERED_RESEARCH_ARTIFACT,
    TRACKED_UNREGISTERED_REVIEW_REQUIRED,
    WILDCARD_DECLARATION_REVIEW_REQUIRED,
    HistoricalCoverageInventory,
    HistoricalCoverageInventoryError,
    HistoricalCoveragePolicy,
    build_historical_coverage_inventory,
    build_historical_coverage_inventory_from_payloads,
    validate_historical_coverage_inventory,
    write_historical_coverage_inventory_artifacts,
)

EXACT_COMMIT = "a" * 40


def _policy() -> HistoricalCoveragePolicy:
    return HistoricalCoveragePolicy(
        policy_id="test-policy",
        report_registry_path="config/report_registry.yaml",
        atlas_source_registry_path="config/atlas/source_registry.yaml",
        tracked_research_roots=("docs/research", "inputs/research_reviews"),
        excluded_paths=(MANDATORY_EXCLUDED_REPOSITORY_PATH,),
        research_report_group="research",
    )


def _report_registry() -> dict[str, object]:
    return {
        "reports": [
            {
                "report_id": "research-example",
                "title": "Research example",
                "group": "research",
                "command": "python example",
                "owner_action": "review",
                "artifact_globs": [
                    "docs/research/final_result.md",
                    "outputs/research/*/result.json",
                    MANDATORY_EXCLUDED_REPOSITORY_PATH,
                ],
            },
            {
                "report_id": "research-runtime",
                "title": "Runtime result",
                "group": "research",
                "command": "python runtime",
                "owner_action": "review",
                "artifact_globs": ["outputs/research/runtime/result.json"],
            },
            {
                "report_id": "operations-example",
                "title": "Operations example",
                "group": "operations",
                "command": "python operations",
                "owner_action": "operate",
                "artifact_globs": ["docs/research/not_in_research_group.md"],
            },
        ]
    }


def _source_registry() -> dict[str, object]:
    return {
        "sources": [
            {
                "source_ref_id": "source-final",
                "source_path": "docs/research/final_result.md",
            },
            {
                "source_ref_id": "source-config",
                "source_path": "config/atlas/source_registry.yaml",
            },
        ]
    }


def _receipts() -> tuple[dict[str, object], ...]:
    return (
        {"path": "config/atlas/historical_coverage_inventory.yaml", "sha256": "1" * 64},
        {"path": "config/report_registry.yaml", "sha256": "2" * 64},
        {"path": "config/atlas/source_registry.yaml", "sha256": "3" * 64},
    )


def _inventory() -> HistoricalCoverageInventory:
    return build_historical_coverage_inventory_from_payloads(
        exact_commit=EXACT_COMMIT,
        policy=_policy(),
        report_registry=_report_registry(),
        atlas_source_registry=_source_registry(),
        tracked_paths=(
            "inputs/research_reviews/review.md",
            MANDATORY_EXCLUDED_REPOSITORY_PATH,
            "docs/research/final_result.md",
        ),
        input_receipts=_receipts(),
    )


def test_builder_is_deterministic_and_keeps_classification_mechanical() -> None:
    first = _inventory()
    second = _inventory()
    assert first.canonical_json_bytes() == second.canonical_json_bytes()
    assert first.inventory_id == second.inventory_id
    assert first.summary == {
        "report_registry_total_count": 3,
        "research_report_count": 2,
        "artifact_declaration_count": 3,
        "exact_declaration_count": 2,
        "unique_exact_artifact_path_count": 2,
        "wildcard_declaration_count": 1,
        "tracked_research_path_count": 2,
        "tracked_registered_path_count": 1,
        "tracked_unregistered_path_count": 1,
        "tracked_atlas_source_path_count": 1,
        "atlas_source_count": 2,
        "atlas_source_registered_exact_count": 1,
        "atlas_source_outside_tracked_roots_count": 1,
        "declared_non_tracked_or_runtime_count": 1,
    }

    tracked = {str(item["path"]): item for item in first.tracked_path_records}
    assert tracked["docs/research/final_result.md"]["classification"] == ATLAS_SOURCE_BOUND
    assert (
        tracked["inputs/research_reviews/review.md"]["classification"]
        == TRACKED_UNREGISTERED_REVIEW_REQUIRED
    )
    declarations = {str(item["artifact_pattern"]): item for item in first.declaration_records}
    assert (
        declarations["docs/research/final_result.md"]["classification"]
        == REGISTERED_RESEARCH_ARTIFACT
    )
    assert (
        declarations["outputs/research/*/result.json"]["classification"]
        == WILDCARD_DECLARATION_REVIEW_REQUIRED
    )
    assert (
        declarations["outputs/research/runtime/result.json"]["classification"]
        == DECLARED_NON_TRACKED_OR_RUNTIME_ARTIFACT
    )
    assert MANDATORY_EXCLUDED_REPOSITORY_PATH not in first.canonical_json_bytes().decode()
    assert first.to_dict()["safety"] == {
        "historical_repository_coverage_complete": False,
        "research_artifact_content_read": False,
        "result_projection_allowed": False,
        "investment_conclusion_generated": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def test_builder_rejects_duplicate_report_ids() -> None:
    registry = _report_registry()
    raw_reports = registry["reports"]
    assert isinstance(raw_reports, list)
    reports = list(raw_reports)
    assert isinstance(reports[0], dict)
    reports.append(dict(reports[0]))
    registry["reports"] = reports
    with pytest.raises(HistoricalCoverageInventoryError, match="DUPLICATE_REPORT_ID"):
        build_historical_coverage_inventory_from_payloads(
            exact_commit=EXACT_COMMIT,
            policy=_policy(),
            report_registry=registry,
            atlas_source_registry=_source_registry(),
            tracked_paths=("docs/research/final_result.md",),
            input_receipts=_receipts(),
        )


def test_git_adapter_reads_only_authority_blobs_and_filters_exclusion(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    policy_payload = {
        "schema_version": "atlas_historical_coverage_policy.v1",
        "policy_id": "test-policy",
        "authority_inputs": {
            "report_registry_path": "config/report_registry.yaml",
            "atlas_source_registry_path": "config/atlas/source_registry.yaml",
        },
        "tracked_research_roots": ["docs/research", "inputs/research_reviews"],
        "excluded_paths": [MANDATORY_EXCLUDED_REPOSITORY_PATH],
        "research_report_group": "research",
        "classification_codes": list(CLASSIFICATION_CODES),
        "safety": {
            "historical_repository_coverage_complete": False,
            "research_artifact_content_read": False,
            "result_projection_allowed": False,
            "investment_conclusion_generated": False,
            "production_effect": "none",
            "broker_action": "none",
        },
    }
    blobs = {
        "config/atlas/historical_coverage_inventory.yaml": yaml.safe_dump(policy_payload).encode(),
        "config/report_registry.yaml": yaml.safe_dump(_report_registry()).encode(),
        "config/atlas/source_registry.yaml": yaml.safe_dump(_source_registry()).encode(),
    }
    requested_blobs: list[str] = []

    def fake_run(args: list[str], **_: object) -> subprocess.CompletedProcess[bytes]:
        if args[1:3] == ["cat-file", "blob"]:
            repository_path = args[3].split(":", maxsplit=1)[1]
            requested_blobs.append(repository_path)
            return subprocess.CompletedProcess(args, 0, stdout=blobs[repository_path], stderr=b"")
        assert args[1:5] == ["ls-tree", "-r", "-z", "--name-only"]
        tree = b"docs/research/final_result.md\0" + (
            MANDATORY_EXCLUDED_REPOSITORY_PATH.encode() + b"\0"
        )
        return subprocess.CompletedProcess(args, 0, stdout=tree, stderr=b"")

    monkeypatch.setattr(subprocess, "run", fake_run)
    inventory = build_historical_coverage_inventory(
        repository_root=tmp_path,
        exact_commit=EXACT_COMMIT,
    )
    assert requested_blobs == [
        "config/atlas/historical_coverage_inventory.yaml",
        "config/report_registry.yaml",
        "config/atlas/source_registry.yaml",
    ]
    assert MANDATORY_EXCLUDED_REPOSITORY_PATH not in inventory.canonical_json_bytes().decode()


def test_validation_fails_closed_on_tampered_inventory(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    original = _inventory()
    tampered = replace(original, summary={**original.summary, "research_report_count": 99})
    monkeypatch.setattr(
        inventory_module,
        "build_historical_coverage_inventory",
        lambda **_: original,
    )
    result = validate_historical_coverage_inventory(tampered, repository_root=tmp_path)
    assert result.status == "FAIL"
    assert result.errors == ("INVENTORY_CANONICAL_REBUILD_MISMATCH",)


def test_writer_is_byte_deterministic(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    inventory = _inventory()
    monkeypatch.setattr(
        inventory_module,
        "build_historical_coverage_inventory",
        lambda **_: inventory,
    )
    first = write_historical_coverage_inventory_artifacts(
        inventory,
        tmp_path / "output",
        repository_root=tmp_path,
    )
    first_payloads = {item.path: Path(item.path).read_bytes() for item in first}
    second = write_historical_coverage_inventory_artifacts(
        inventory,
        tmp_path / "output",
        repository_root=tmp_path,
    )
    assert [(item.path, item.sha256, item.size_bytes) for item in first] == [
        (item.path, item.sha256, item.size_bytes) for item in second
    ]
    assert first_payloads == {item.path: Path(item.path).read_bytes() for item in second}
