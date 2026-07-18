from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from dynamic_v3_weight_batch_search_helpers import run_near_miss_ab_comparison_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_weight_search_targeted as targeted
from ai_trading_system.platform.artifacts.validation_session import (
    with_artifact_validation_session,
)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _assert_fail(validator: Callable[[], dict[str, Any]]) -> None:
    payload = validator()
    assert payload["status"] == "FAIL"
    assert payload["failed_check_count"] > 0


def _tamper_snapshot_field(
    path: Path,
    validator: Callable[[], dict[str, Any]],
    mutate: Callable[[dict[str, Any]], None],
) -> None:
    original = path.read_bytes()
    try:
        payload = json.loads(original.decode("utf-8"))
        mutate(payload)
        _write_json(path, payload)
        _assert_fail(validator)
    finally:
        path.write_bytes(original)


@with_artifact_validation_session
def test_targeted_chain_rebuilds_all_views_and_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = run_near_miss_ab_comparison_fixture(tmp_path, compact_test_matrix=True)
    scope_cases = (
        (
            tmp_path / "search_coverage_gap" / fixture["coverage_gap"]["coverage_gap_id"],
            "search_coverage_gap_input_snapshot.json",
        ),
        (
            tmp_path / "near_miss_candidates" / fixture["near_miss"]["near_miss_id"],
            "near_miss_candidates_input_snapshot.json",
        ),
        (
            tmp_path / "weight_scorecard" / fixture["scorecard"]["scorecard_id"],
            "weight_scorecard_input_snapshot.json",
        ),
        (
            tmp_path / "weight_batch_backfill" / fixture["weight_backfill"]["batch_backfill_id"],
            "weight_batch_backfill_input_snapshot.json",
        ),
        (
            tmp_path / "paper_shadow_backfill" / fixture["backfill"]["backfill_id"],
            "paper_shadow_backfill_input_snapshot.json",
        ),
    )
    scopes = [
        targeted._targeted_upstream_validation_scope(
            artifact_root=artifact_root,
            snapshot_name=snapshot_name,
        )
        for artifact_root, snapshot_name in scope_cases
    ]
    assert all(scope is not None for scope in scopes)
    assert all(scope is not None and scope.discover_bound_paths for scope in scopes)
    assert all(
        scope is not None
        and {inventory.patterns for inventory in scope.inventories}
        == {("*/model_target_manifest.json",), ("*/daily_advisory_manifest.json",)}
        for scope in scopes
    )

    paper_id = fixture["backfill"]["backfill_id"]
    paper_dir = tmp_path / "paper_shadow_backfill"
    paper_root = paper_dir / paper_id
    paper_snapshot_path = paper_root / "paper_shadow_backfill_input_snapshot.json"
    paper_snapshot = json.loads(paper_snapshot_path.read_text(encoding="utf-8"))
    paper_cache_paths = {
        Path(binding["path"]).resolve() for binding in paper_snapshot["cache_bindings"]
    }
    assert all(
        scope is not None and paper_cache_paths.issubset({path.resolve() for path in scope.paths})
        for scope in scopes
    )
    weight_snapshot_path = (
        tmp_path
        / "weight_batch_backfill"
        / fixture["weight_backfill"]["batch_backfill_id"]
        / "weight_batch_backfill_input_snapshot.json"
    )
    weight_snapshot = json.loads(weight_snapshot_path.read_text(encoding="utf-8"))
    weight_price_path = Path(weight_snapshot["price_source"]["path"])
    weight_scope = scopes[3]
    assert weight_scope is not None
    assert {
        weight_price_path.parent / "download_manifest.csv",
        weight_price_path.parent / "prices_marketstack_daily.csv",
    }.issubset(set(weight_scope.paths))

    adapter_calls: dict[str, int] = {}

    def counted_pass(name: str) -> Callable[..., dict[str, Any]]:
        adapter_calls[name] = 0

        def validator(**_: Any) -> dict[str, Any]:
            adapter_calls[name] += 1
            return {"status": "PASS"}

        return validator

    adapter_cases = (
        (
            "coverage",
            targeted.diagnostics,
            "validate_search_coverage_gap_artifact",
            lambda: targeted._validated_coverage(
                fixture["coverage_gap"]["coverage_gap_id"],
                tmp_path / "search_coverage_gap",
            ),
        ),
        (
            "near_miss",
            targeted.diagnostics,
            "validate_near_miss_candidates_artifact",
            lambda: targeted._validated_near_miss(
                fixture["near_miss"]["near_miss_id"],
                tmp_path / "near_miss_candidates",
            ),
        ),
        (
            "scorecard",
            targeted.evaluation,
            "validate_weight_scorecard_artifact",
            lambda: targeted._validated_scorecard(
                fixture["scorecard"]["scorecard_id"],
                tmp_path / "weight_scorecard",
            ),
        ),
        (
            "weight_backfill",
            targeted.foundation,
            "validate_weight_batch_backfill_artifact",
            lambda: targeted._validated_weight_backfill(
                fixture["weight_backfill"]["batch_backfill_id"],
                tmp_path / "weight_batch_backfill",
            ),
        ),
        (
            "paper_backfill",
            targeted.st,
            "validate_paper_shadow_backfill_artifact",
            lambda: targeted._validated_paper_backfill(paper_id, paper_dir),
        ),
    )
    with monkeypatch.context() as adapter_patch:
        for name, module, attribute, _ in adapter_cases:
            adapter_patch.setattr(module, attribute, counted_pass(name))
        for name, _, _, adapter in adapter_cases:
            adapter()
            adapter()
            assert adapter_calls[name] == 1

        original_weight_snapshot = weight_snapshot_path.read_bytes()
        independent_price_dir = tmp_path / "weight_independent_price_cache"
        independent_price_dir.mkdir()
        independent_price_path = independent_price_dir / weight_price_path.name
        independent_price_bytes = weight_price_path.read_bytes()
        independent_price_path.write_bytes(independent_price_bytes)
        independent_weight_snapshot = json.loads(original_weight_snapshot.decode("utf-8"))
        independent_weight_snapshot["price_source"] = {
            **independent_weight_snapshot["price_source"],
            "path": str(independent_price_path),
            "sha256": hashlib.sha256(independent_price_bytes).hexdigest(),
            "size_bytes": len(independent_price_bytes),
        }
        _write_json(weight_snapshot_path, independent_weight_snapshot)
        try:
            isolated_scope = targeted._targeted_upstream_validation_scope(
                artifact_root=weight_snapshot_path.parent,
                snapshot_name=weight_snapshot_path.name,
            )
            assert isolated_scope is not None
            independent_optional_path = independent_price_dir / "download_manifest.csv"
            assert {
                independent_optional_path,
                independent_price_dir / "prices_marketstack_daily.csv",
            }.issubset(set(isolated_scope.paths))
            assert not independent_optional_path.exists()

            weight_calls_before = adapter_calls["weight_backfill"]
            adapter_cases[3][3]()
            adapter_cases[3][3]()
            assert adapter_calls["weight_backfill"] == weight_calls_before + 1

            independent_optional_path.write_text("materialized-after-pass\n", encoding="utf-8")
            materialized_calls_before = adapter_calls["weight_backfill"]
            adapter_cases[3][3]()
            adapter_cases[3][3]()
            assert adapter_calls["weight_backfill"] == materialized_calls_before + 1
            independent_optional_path.unlink()
            restored_independent_calls = adapter_calls["weight_backfill"]
            adapter_cases[3][3]()
            assert adapter_calls["weight_backfill"] == restored_independent_calls
        finally:
            (independent_price_dir / "download_manifest.csv").unlink(missing_ok=True)
            weight_snapshot_path.write_bytes(original_weight_snapshot)
            independent_price_path.unlink(missing_ok=True)
            independent_price_dir.rmdir()
        restored_original_calls = adapter_calls["weight_backfill"]
        adapter_cases[3][3]()
        assert adapter_calls["weight_backfill"] == restored_original_calls

    with monkeypatch.context() as cache_patch:
        original_paper_validator = targeted.st.validate_paper_shadow_backfill_artifact
        validator_calls = 0

        def counted_paper_validator(**kwargs: Any) -> dict[str, Any]:
            nonlocal validator_calls
            validator_calls += 1
            return original_paper_validator(**kwargs)

        cache_patch.setattr(
            targeted.st,
            "validate_paper_shadow_backfill_artifact",
            counted_paper_validator,
        )

        def validate_paper() -> dict[str, Any]:
            return targeted._validated_paper_backfill(paper_id, paper_dir)

        validate_paper()
        validate_paper()
        assert validator_calls == 1

        def assert_invalidated_without_caching_fail(
            mutate: Callable[[], None],
            restore: Callable[[], None],
        ) -> None:
            nonlocal validator_calls
            before = validator_calls
            mutate()
            try:
                for _ in range(2):
                    with pytest.raises(ValueError, match="source paper backfill validation failed"):
                        validate_paper()
                assert validator_calls == before + 2
            finally:
                restore()
            restored_calls = validator_calls
            validate_paper()
            assert validator_calls == restored_calls

        model_selection = paper_snapshot["model_target_selection"]
        selected_model = Path(model_selection["root"]) / model_selection["artifact_id"]
        model_sibling = Path(model_selection["root"]) / "same-as-of-model-sibling"
        model_sibling_manifest = model_sibling / "model_target_manifest.json"

        def add_model_sibling() -> None:
            model_sibling.mkdir()
            model_sibling_manifest.write_bytes(
                (selected_model / "model_target_manifest.json").read_bytes()
            )

        def remove_model_sibling() -> None:
            model_sibling_manifest.unlink(missing_ok=True)
            model_sibling.rmdir()

        assert_invalidated_without_caching_fail(add_model_sibling, remove_model_sibling)

        model_snapshot = json.loads(
            (selected_model / "model_target_input_snapshot.json").read_text(encoding="utf-8")
        )
        daily_selection = model_snapshot["source_selection"]
        selected_daily = Path(daily_selection["root"]) / daily_selection["artifact_id"]
        daily_sibling = Path(daily_selection["root"]) / "same-as-of-daily-sibling"
        daily_sibling_manifest = daily_sibling / "daily_advisory_manifest.json"

        def add_daily_sibling() -> None:
            daily_sibling.mkdir()
            daily_sibling_manifest.write_bytes(
                (selected_daily / "daily_advisory_manifest.json").read_bytes()
            )

        def remove_daily_sibling() -> None:
            daily_sibling_manifest.unlink(missing_ok=True)
            daily_sibling.rmdir()

        assert_invalidated_without_caching_fail(add_daily_sibling, remove_daily_sibling)

        optional_binding = next(
            binding for binding in paper_snapshot["cache_bindings"] if binding["commitment"] is None
        )
        optional_path = Path(optional_binding["path"])
        assert not optional_path.exists()

        def materialize_optional_source() -> None:
            optional_path.write_text("materialized-after-pass\n", encoding="utf-8")

        def remove_optional_source() -> None:
            optional_path.unlink(missing_ok=True)

        assert_invalidated_without_caching_fail(
            materialize_optional_source,
            remove_optional_source,
        )

        required_binding = next(
            binding
            for binding in paper_snapshot["cache_bindings"]
            if binding["commitment"] is not None
        )
        required_path = Path(required_binding["path"])
        required_bytes = required_path.read_bytes()

        def tamper_required_source() -> None:
            required_path.write_bytes(required_bytes + b"\n")

        def restore_required_source() -> None:
            required_path.write_bytes(required_bytes)

        assert_invalidated_without_caching_fail(
            tamper_required_source,
            restore_required_source,
        )

        exception_calls = 0

        def exploding_validator(**_: Any) -> dict[str, Any]:
            nonlocal exception_calls
            exception_calls += 1
            raise RuntimeError("targeted validator exception")

        for _ in range(2):
            with pytest.raises(RuntimeError, match="targeted validator exception"):
                targeted._validated_upstream_with_hardened_scope(
                    validator=exploding_validator,
                    validator_key="backfill_id",
                    artifact_id=paper_id,
                    output_dir=paper_dir,
                    snapshot_name="paper_shadow_backfill_input_snapshot.json",
                )
        assert exception_calls == 2

    with monkeypatch.context():
        bypass_calls = 0

        def bypass_validator(**_: Any) -> dict[str, Any]:
            nonlocal bypass_calls
            bypass_calls += 1
            return {"status": "PASS"}

        original_paper_snapshot = paper_snapshot_path.read_bytes()
        malformed_snapshot = json.loads(original_paper_snapshot.decode("utf-8"))
        malformed_snapshot["cache_bindings"][0]["path"] = "relative-cache.csv"
        _write_json(paper_snapshot_path, malformed_snapshot)
        try:
            for _ in range(2):
                targeted._validated_upstream_with_hardened_scope(
                    validator=bypass_validator,
                    validator_key="backfill_id",
                    artifact_id=paper_id,
                    output_dir=paper_dir,
                    snapshot_name="paper_shadow_backfill_input_snapshot.json",
                )
            assert bypass_calls == 2
        finally:
            paper_snapshot_path.write_bytes(original_paper_snapshot)

    matrix = fixture["targeted_v3"]
    backfill = fixture["targeted_v3_backfill"]
    comparison = fixture["near_miss_ab"]

    matrix_root = Path(matrix["v3_matrix_dir"])
    backfill_root = Path(backfill["v3_backfill_dir"])
    ab_root = Path(comparison["ab_dir"])

    def validate_matrix() -> dict[str, Any]:
        return targeted.validate_targeted_search_v3_artifact(
            v3_matrix_id=matrix["v3_matrix_id"],
            output_dir=tmp_path / "targeted_search_v3",
        )

    def validate_backfill() -> dict[str, Any]:
        return targeted.validate_targeted_v3_backfill_artifact(
            v3_backfill_id=backfill["v3_backfill_id"],
            output_dir=tmp_path / "targeted_v3_backfill",
        )

    def validate_ab() -> dict[str, Any]:
        return targeted.validate_near_miss_ab_comparison_artifact(
            ab_id=comparison["ab_id"],
            output_dir=tmp_path / "near_miss_ab_comparison",
        )

    stages = (
        (
            matrix_root,
            "targeted_search_v3_input_snapshot.json",
            targeted.MATRIX_INPUT_SCHEMA,
            targeted.MATRIX_VIEWS,
            validate_matrix,
        ),
        (
            backfill_root,
            "targeted_v3_backfill_input_snapshot.json",
            targeted.BACKFILL_INPUT_SCHEMA,
            targeted.BACKFILL_VIEWS,
            validate_backfill,
        ),
        (
            ab_root,
            "near_miss_ab_comparison_input_snapshot.json",
            targeted.AB_INPUT_SCHEMA,
            targeted.AB_VIEWS,
            validate_ab,
        ),
    )

    assert sum(len(view_names) for _, _, _, view_names, _ in stages) == 16
    for root, snapshot_name, expected_schema, view_names, validator in stages:
        snapshot_path = root / snapshot_name
        snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
        assert snapshot["schema_version"] == expected_schema
        assert set(snapshot["view_hashes"]) == set(view_names)

        for view_name in view_names:
            view_path = root / view_name
            original_view = view_path.read_bytes()
            try:
                view_path.write_bytes(original_view + b"\n")
                _assert_fail(validator)
            finally:
                view_path.write_bytes(original_view)

        _tamper_snapshot_field(
            snapshot_path,
            validator,
            lambda payload: payload.__setitem__("schema_version", "tampered.schema"),
        )

    lineage_cases = (
        (
            matrix_root / "targeted_search_v3_input_snapshot.json",
            "coverage_source",
            validate_matrix,
        ),
        (
            backfill_root / "targeted_v3_backfill_input_snapshot.json",
            "matrix_source",
            validate_backfill,
        ),
        (
            ab_root / "near_miss_ab_comparison_input_snapshot.json",
            "backfill_source",
            validate_ab,
        ),
    )
    for snapshot_path, binding_name, validator in lineage_cases:
        _tamper_snapshot_field(
            snapshot_path,
            validator,
            lambda payload, binding_name=binding_name: payload[binding_name].__setitem__(
                "artifact_id", "cross-lineage-artifact"
            ),
        )

    matrix_snapshot_path = matrix_root / "targeted_search_v3_input_snapshot.json"
    _tamper_snapshot_field(
        matrix_snapshot_path,
        validate_matrix,
        lambda payload: payload["policy_source"].__setitem__("sha256", "0" * 64),
    )

    backfill_snapshot_path = backfill_root / "targeted_v3_backfill_input_snapshot.json"
    for binding_name in ("price_source", "rates_source"):
        _tamper_snapshot_field(
            backfill_snapshot_path,
            validate_backfill,
            lambda payload, binding_name=binding_name: payload[binding_name].__setitem__(
                "sha256", "0" * 64
            ),
        )

    progress_path = backfill_root / targeted.BACKFILL_VIEWS[1]
    original_progress = progress_path.read_bytes()
    try:
        progress_path.write_bytes(original_progress + b"\n")
        with pytest.raises(ValueError, match="validation failed before resume"):
            targeted.resume_targeted_v3_backfill(
                v3_backfill_id=backfill["v3_backfill_id"],
                output_dir=tmp_path / "targeted_v3_backfill",
            )
    finally:
        progress_path.write_bytes(original_progress)

    matrix_manifest = matrix_root / targeted.MATRIX_VIEWS[0]
    held_manifest = matrix_manifest.with_suffix(".held")
    matrix_manifest.rename(held_manifest)
    try:
        with pytest.raises(ValueError, match="validation failed before resume"):
            targeted.resume_targeted_v3_backfill(
                v3_backfill_id=backfill["v3_backfill_id"],
                output_dir=tmp_path / "targeted_v3_backfill",
            )
    finally:
        held_manifest.rename(matrix_manifest)

    early_output = tmp_path / "chronology_must_not_materialize"
    with pytest.raises(ValueError, match="source chronology invalid"):
        targeted.build_targeted_search_v3(
            coverage_gap_id=fixture["coverage_gap"]["coverage_gap_id"],
            coverage_gap_dir=tmp_path / "search_coverage_gap",
            near_miss_dir=tmp_path / "near_miss_candidates",
            output_dir=early_output,
            generated_at=datetime(2000, 1, 1, tzinfo=UTC),
        )
    assert not early_output.exists()
