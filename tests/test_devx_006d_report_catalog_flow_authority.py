from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pytest
import yaml

from ai_trading_system.platform.architecture import (
    report_catalog_flow_authority as authority,
)
from ai_trading_system.platform.architecture.compatibility_authority import (
    load_compatibility_authority,
)
from ai_trading_system.platform.architecture.report_catalog_flow_authority import (
    ReportCatalogFlowAuthorityError,
    build_repository_authority,
    load_policy,
    render_shadow_bytes,
    validate_repository_authority,
)
from ai_trading_system.platform.artifacts.writer import canonical_json_bytes


def _seal(content: bytes) -> dict[str, object]:
    return {
        "byte_count": len(content),
        "file_sha256": hashlib.sha256(content).hexdigest(),
        "lf_sha256": hashlib.sha256(content.replace(b"\r\n", b"\n")).hexdigest(),
        "git_blob": authority._git_blob_id(content),
    }


def _write_fixture(root: Path) -> dict[str, Any]:
    sources = {
        "config/report_registry.yaml": (
            b"schema_version: 1\nreports:\n"
            b"  - report_id: alpha\n    title: Alpha\n"
            b"  - report_id: beta\n    title: Beta\n"
        ),
        "docs/artifact_catalog.md": b"# Catalog\n\n## Alpha\n\nalpha body\n",
        "docs/system_flow.md": b"# Flow\n\nalpha -> beta\n\n",
    }
    target_specs = (
        (
            "report_registry",
            "config/report_registry.yaml",
            "YAML_REPORT_REGISTRY",
            "YAML_REPORT_ITEMS_WITH_PREFIX_V1",
        ),
        (
            "artifact_catalog",
            "docs/artifact_catalog.md",
            "MARKDOWN",
            "EXACT_BLANK_LINE_BLOCKS_V1",
        ),
        (
            "system_flow",
            "docs/system_flow.md",
            "MARKDOWN",
            "EXACT_BLANK_LINE_BLOCKS_V1",
        ),
    )
    targets: list[dict[str, object]] = []
    for target_id, portable, target_format, splitter in target_specs:
        content = sources[portable]
        path = root / portable
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        target: dict[str, object] = {
            "target_id": target_id,
            "path": portable,
            "format": target_format,
            "splitter": splitter,
            **_seal(content),
            "entry_count": (
                len(authority._split_report_registry(content))
                if splitter == "YAML_REPORT_ITEMS_WITH_PREFIX_V1"
                else len(authority._split_markdown(target_id, content))
            ),
        }
        targets.append(target)
    policy = {
        "schema_version": "devx_006d_report_catalog_flow_authority_policy.v1",
        "status": "INACTIVE_SHADOW",
        "task_id": "DEVX-006D_REPORT_CATALOG_FLOW_LOSSLESS_FRAGMENTATION",
        "exact_start_base": "0" * 40,
        "owner_decision": "fixture-owner-decision",
        "partition_count": 64,
        "fragment_root": "registry/report_catalog_flow_authority/fragments",
        "index_path": ("inputs/architecture/devx_006d_report_catalog_flow_authority_index.json"),
        "consumer_inventory_path": (
            "inputs/architecture/devx_006d_report_catalog_flow_consumer_inventory.json"
        ),
        "targets": targets,
        "contract": {
            "source_of_truth": "LEGACY_MONOLITH",
            "fragment_shadow_active": False,
            "aggregate_write_allowed": False,
            "fragment_identity": "FULL_ENTRY_RAW_SHA256",
            "partition_identity": "RAW_SHA256_LOW_6_BITS",
            "index_chain": "SHA256",
            "coverage_required_percent": 100,
            "silent_drop_allowed": False,
            "rollback_mode": "IGNORE_INACTIVE_SHADOW",
        },
        "production_effect": "none",
        "broker_action": "none",
    }
    policy_path = root / authority.DEFAULT_POLICY_PATH
    policy_path.parent.mkdir(parents=True, exist_ok=True)
    policy_path.write_text(
        yaml.safe_dump(policy, sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )
    result = build_repository_authority(root, write=True)
    return {"sources": sources, "policy": policy, "result": result}


def test_repository_authority_is_fresh_lossless_and_inactive() -> None:
    result = validate_repository_authority()
    policy = load_policy()

    assert result["status"] == "PASS"
    assert result["source_of_truth"] == "LEGACY_MONOLITH"
    assert result["fragment_shadow_active"] is False
    assert result["target_count"] == 3
    assert result["entry_count"] == 2924
    assert 1 <= result["fragment_count"] <= 192
    assert policy["contract"] == {
        "source_of_truth": "LEGACY_MONOLITH",
        "fragment_shadow_active": False,
        "aggregate_write_allowed": False,
        "fragment_identity": "FULL_ENTRY_RAW_SHA256",
        "partition_identity": "RAW_SHA256_LOW_6_BITS",
        "index_chain": "SHA256",
        "coverage_required_percent": 100,
        "silent_drop_allowed": False,
        "rollback_mode": "IGNORE_INACTIVE_SHADOW",
    }


def test_compatibility_authority_carries_the_inactive_shadow_contract() -> None:
    merged = load_compatibility_authority()
    section = merged["phase_devx_006d_report_catalog_flow_lossless_fragmentation"]
    fragment_authority = section["report_catalog_flow_fragment_authority"]

    assert next(reversed(merged)) == "phase_arch_005_s5_canonical_task_source_cutover"
    assert fragment_authority["source_of_truth"] == "LEGACY_MONOLITH"
    assert fragment_authority["fragment_shadow_active"] is False
    assert fragment_authority["target_count"] == 3
    assert fragment_authority["entry_count"] == 2924
    assert fragment_authority["fragment_count"] == 192
    assert section["consumer_contract"]["cutover_ready"] is False


@pytest.mark.parametrize(
    "target_id,source_path,expected_sha256,expected_entry_count",
    [
        (
            "report_registry",
            "config/report_registry.yaml",
            "c25f1dac0a6c2822dc88f0ae0a06b93af2a21b7004abba3dd7400df93faae1af",
            1371,
        ),
        (
            "artifact_catalog",
            "docs/artifact_catalog.md",
            "7bb3355d3cf895ee1a8b8b63cb47e75691f45e581aca96f78f1064482ec3aca7",
            556,
        ),
        (
            "system_flow",
            "docs/system_flow.md",
            "ff17c208496c6231e74cc4fe75a6e60b8218418e36db43592721926318416f3c",
            997,
        ),
    ],
)
def test_each_shadow_render_is_byte_identical_and_fully_covered(
    target_id: str,
    source_path: str,
    expected_sha256: str,
    expected_entry_count: int,
) -> None:
    rendered = render_shadow_bytes(target_id)
    source = Path(source_path).read_bytes()
    index = json.loads(
        Path("inputs/architecture/devx_006d_report_catalog_flow_authority_index.json").read_text(
            encoding="utf-8"
        )
    )
    target = next(row for row in index["targets"] if row["target_id"] == target_id)

    assert rendered == source
    assert hashlib.sha256(rendered).hexdigest() == expected_sha256
    assert target["entry_count"] == expected_entry_count
    assert target["coverage_bytes"] == len(source)
    assert target["coverage_percent"] == 100
    assert len(target["entry_order"]) == expected_entry_count
    assert target["fragment_count"] == len(target["fragments"])


def test_build_is_repeatable_and_never_writes_monoliths() -> None:
    sources = {
        path: Path(path).read_bytes()
        for path in (
            "config/report_registry.yaml",
            "docs/artifact_catalog.md",
            "docs/system_flow.md",
        )
    }
    first = build_repository_authority(write=False)
    second = build_repository_authority(write=False)

    assert first == second
    assert all(Path(path).read_bytes() == content for path, content in sources.items())


def test_consumer_inventory_is_complete_and_cutover_remains_blocked() -> None:
    inventory = json.loads(
        Path("inputs/architecture/devx_006d_report_catalog_flow_consumer_inventory.json").read_text(
            encoding="utf-8"
        )
    )

    assert inventory["status"] == "PASS"
    assert inventory["source_of_truth"] == "LEGACY_MONOLITH"
    assert inventory["cutover_ready"] is False
    assert inventory["pending_owner_cutover_count"] > 0
    assert inventory["consumer_count"] == len(inventory["consumers"])
    assert inventory["target_paths"] == [
        "config/report_registry.yaml",
        "docs/artifact_catalog.md",
        "docs/system_flow.md",
    ]
    assert all(
        row["migration_status"]
        in {
            "MIGRATED_SHADOW",
            "PENDING_OWNER_CUTOVER",
            "RETAINED_UNTIL_OWNER_CUTOVER",
        }
        for row in inventory["consumers"]
    )


def test_fixture_round_trip_and_report_entry_identity(tmp_path: Path) -> None:
    fixture = _write_fixture(tmp_path)

    assert validate_repository_authority(tmp_path)["status"] == "PASS"
    for target in fixture["policy"]["targets"]:
        target_id = str(target["target_id"])
        source_path = str(target["path"])
        assert render_shadow_bytes(target_id, tmp_path) == fixture["sources"][source_path]
    index = json.loads((tmp_path / fixture["policy"]["index_path"]).read_text(encoding="utf-8"))
    report_entries = index["targets"][0]["entry_order"]
    assert [entry["entry_id"] for entry in report_entries] == [
        "report_registry:prefix",
        "report_registry:report:alpha",
        "report_registry:report:beta",
    ]


def test_source_drift_fails_closed(tmp_path: Path) -> None:
    _write_fixture(tmp_path)
    source = tmp_path / "docs/system_flow.md"
    source.write_bytes(source.read_bytes() + b"drift\n")

    with pytest.raises(ReportCatalogFlowAuthorityError) as error:
        validate_repository_authority(tmp_path)
    assert error.value.code == "RCF_SOURCE_SEAL_DRIFT"


def test_missing_fragment_fails_closed(tmp_path: Path) -> None:
    fixture = _write_fixture(tmp_path)
    fragment = tmp_path / fixture["result"]["fragment_paths"][0]
    fragment.unlink()

    with pytest.raises(ReportCatalogFlowAuthorityError) as error:
        validate_repository_authority(tmp_path)
    assert error.value.code == "RCF_FILE_MISSING"


def test_fragment_tamper_fails_closed(tmp_path: Path) -> None:
    fixture = _write_fixture(tmp_path)
    fragment = tmp_path / fixture["result"]["fragment_paths"][0]
    fragment.write_bytes(fragment.read_bytes() + b" ")

    with pytest.raises(ReportCatalogFlowAuthorityError) as error:
        validate_repository_authority(tmp_path)
    assert error.value.code == "RCF_FRAGMENT_STALE"


@pytest.mark.parametrize("mutation", ["duplicate", "reorder", "path_escape"])
def test_index_duplicate_reorder_or_path_escape_fails_closed(
    tmp_path: Path,
    mutation: str,
) -> None:
    fixture = _write_fixture(tmp_path)
    index_path = tmp_path / fixture["policy"]["index_path"]
    index = json.loads(index_path.read_text(encoding="utf-8"))
    entries = index["targets"][0]["entry_order"]
    if mutation == "duplicate":
        entries.insert(1, dict(entries[0]))
        index["targets"][0]["entry_count"] += 1
    elif mutation == "reorder":
        entries[0], entries[1] = entries[1], entries[0]
    else:
        index["targets"][0]["fragments"][0]["fragment_path"] = "../outside.json"
    index_path.write_bytes(
        canonical_json_bytes(index, sort_keys=True, indent=2, ensure_ascii=False)
    )

    with pytest.raises(ReportCatalogFlowAuthorityError):
        validate_repository_authority(tmp_path)


def test_noncanonical_index_fails_closed(tmp_path: Path) -> None:
    fixture = _write_fixture(tmp_path)
    index_path = tmp_path / fixture["policy"]["index_path"]
    index_path.write_bytes(index_path.read_bytes() + b"\n")

    with pytest.raises(ReportCatalogFlowAuthorityError) as error:
        validate_repository_authority(tmp_path)
    assert error.value.code == "RCF_INDEX_STALE"
