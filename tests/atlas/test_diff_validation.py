from __future__ import annotations

import copy
from dataclasses import replace
from datetime import timedelta
from pathlib import Path

from ai_trading_system.atlas.diff_validation import (
    diff_validation_json_bytes,
    validate_serialized_snapshot_diff,
    validate_snapshot_diff_bundle,
)
from ai_trading_system.atlas.snapshot_builder import build_atlas_bundle
from ai_trading_system.atlas.snapshot_diff import load_snapshot_diff_bundle
from ai_trading_system.contracts import (
    ExplorerDiffChangeKind,
    ExplorerEntityChange,
    StrategyResearchExplorerDiff,
    StrategyResearchExplorerSnapshot,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _bundle(tmp_path: Path):
    before = build_atlas_bundle(
        repository_root=PROJECT_ROOT,
        exact_commit="f" * 40,
    ).snapshot
    after = StrategyResearchExplorerSnapshot.build(
        title=before.title + " next",
        generated_at=before.generated_at + timedelta(days=1),
        sources=(
            replace(
                before.sources[0],
                exact_commit="e" * 40,
                as_of=before.sources[0].as_of + timedelta(days=1),
            ),
            *before.sources[1:],
        ),
        nodes=before.nodes,
        edges=before.edges,
        results=(
            replace(
                before.results[0],
                reader_summary=before.results[0].reader_summary + " 已复核。",
            ),
            *before.results[1:],
        ),
        attributions=before.attributions,
    )
    before_path = tmp_path / "v1" / "snapshot.json"
    after_path = tmp_path / "v1_1" / "snapshot.json"
    before_path.parent.mkdir()
    after_path.parent.mkdir()
    before_path.write_bytes(before.canonical_json_bytes())
    after_path.write_bytes(after.canonical_json_bytes())
    return load_snapshot_diff_bundle(
        before_path=before_path,
        after_path=after_path,
        recorded_at=after.generated_at,
        path_root=tmp_path,
    )


def test_valid_bundle_passes_independent_validation(
    tmp_path: Path,
    monkeypatch,
) -> None:
    bundle = _bundle(tmp_path)

    def forbidden_builder(*args, **kwargs):
        del args, kwargs
        raise AssertionError("validator must not call the diff builder")

    monkeypatch.setattr(
        "ai_trading_system.atlas.snapshot_diff.build_snapshot_diff",
        forbidden_builder,
    )
    result = validate_snapshot_diff_bundle(bundle)
    assert result.status == "PASS"
    assert result.error_count == 0
    assert result.change_count == 2
    assert diff_validation_json_bytes(result).endswith(b"\n")


def test_serialized_validator_fails_closed_for_tamper(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path)
    diff_payload = copy.deepcopy(bundle.diff.to_dict())
    diff_payload["changes"][0]["after_sha256"] = "0" * 64
    result = validate_serialized_snapshot_diff(
        before_payload=bundle.before.to_dict(),
        after_payload=bundle.after.to_dict(),
        diff_payload=diff_payload,
    )
    assert result.status == "FAIL"
    assert any(item.startswith("ATLAS_DIFF_CONTRACT_INVALID") for item in result.errors)


def test_independent_validator_rejects_validly_reidentified_wrong_entity_hash(
    tmp_path: Path,
) -> None:
    bundle = _bundle(tmp_path)
    changed_index = next(
        index
        for index, item in enumerate(bundle.diff.changes)
        if item.change_kind is ExplorerDiffChangeKind.CHANGED
    )
    original = bundle.diff.changes[changed_index]
    wrong_change = ExplorerEntityChange.build(
        entity_kind=original.entity_kind,
        entity_id=original.entity_id,
        change_kind=original.change_kind,
        significance=original.significance,
        before_sha256="0" * 64,
        after_sha256=original.after_sha256,
        field_changes=original.field_changes,
    )
    changes = list(bundle.diff.changes)
    changes[changed_index] = wrong_change
    wrong_diff = StrategyResearchExplorerDiff.build(
        before_snapshot_id=bundle.diff.before_snapshot_id,
        after_snapshot_id=bundle.diff.after_snapshot_id,
        before_generated_at=bundle.diff.before_generated_at,
        after_generated_at=bundle.diff.after_generated_at,
        changes=changes,
        entity_summaries=bundle.diff.entity_summaries,
    )
    result = validate_snapshot_diff_bundle(replace(bundle, diff=wrong_diff))
    assert result.status == "FAIL"
    assert any("BEFORE_HASH_MISMATCH" in item for item in result.errors)


def test_serialized_validator_rejects_wrong_snapshot_binding(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path)
    before_payload = copy.deepcopy(bundle.before.to_dict())
    before_payload["title"] = "tampered without rebuilding snapshot identity"
    result = validate_serialized_snapshot_diff(
        before_payload=before_payload,
        after_payload=bundle.after.to_dict(),
        diff_payload=bundle.diff.to_dict(),
    )
    assert result.status == "FAIL"
    assert any(item.startswith("ATLAS_DIFF_CONTRACT_INVALID") for item in result.errors)
