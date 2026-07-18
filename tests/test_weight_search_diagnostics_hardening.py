from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from dynamic_v3_weight_batch_search_helpers import run_search_coverage_gap_fixture

from ai_trading_system.etf_portfolio import (
    dynamic_v3_weight_search_diagnostics as diagnostics,
)
from ai_trading_system.etf_portfolio import (
    dynamic_v3_weight_search_validation_scope as validation_scope,
)
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


@with_artifact_validation_session
def test_diagnostics_chain_rebuilds_every_view_and_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = run_search_coverage_gap_fixture(tmp_path, compact_test_matrix=True)
    review = fixture["no_promotion_review"]
    near_miss = fixture["near_miss"]
    attribution = fixture["cash_buffer_attribution"]
    coverage = fixture["coverage_gap"]

    scope_cases = (
        (
            Path(review["review_dir"]),
            "no_promotion_review_input_snapshot.json",
        ),
        (
            Path(near_miss["near_miss_dir"]),
            "near_miss_candidates_input_snapshot.json",
        ),
        (
            Path(attribution["attribution_dir"]),
            "cash_buffer_attribution_input_snapshot.json",
        ),
        (
            Path(coverage["coverage_gap_dir"]),
            "search_coverage_gap_input_snapshot.json",
        ),
    )
    scopes = [
        validation_scope.hardened_upstream_validation_scope(
            artifact_root=artifact_root,
            snapshot_name=snapshot_name,
        )
        for artifact_root, snapshot_name in scope_cases
    ]
    assert all(scope is not None and scope.discover_bound_paths for scope in scopes)
    assert all(
        scope is not None
        and {inventory.patterns for inventory in scope.inventories}
        == {("*/model_target_manifest.json",), ("*/daily_advisory_manifest.json",)}
        for scope in scopes
    )

    paper_snapshot_path = (
        tmp_path
        / "paper_shadow_backfill"
        / fixture["backfill"]["backfill_id"]
        / "paper_shadow_backfill_input_snapshot.json"
    )
    paper_snapshot = json.loads(paper_snapshot_path.read_text(encoding="utf-8"))
    paper_cache_paths = {
        Path(binding["path"]).resolve() for binding in paper_snapshot["cache_bindings"]
    }
    assert Path(fixture["prices_path"]).resolve() in paper_cache_paths
    assert Path(fixture["rates_path"]).resolve() in paper_cache_paths
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
    assert all(
        scope is not None
        and {
            weight_price_path.parent / "download_manifest.csv",
            weight_price_path.parent / "prices_marketstack_daily.csv",
        }.issubset(set(scope.paths))
        for scope in scopes
    )

    search_scope = validation_scope.hardened_upstream_validation_scope(
        artifact_root=Path(fixture["search_space"]["search_space_dir"]),
        snapshot_name="weight_search_space_input_snapshot.json",
    )
    assert search_scope is not None and search_scope.discover_bound_paths

    adapter_calls: dict[str, int] = {}

    def counted_result(name: str, status: str = "PASS") -> Callable[..., dict[str, Any]]:
        adapter_calls[name] = 0

        def validator(**_: Any) -> dict[str, Any]:
            adapter_calls[name] += 1
            return {"status": status}

        return validator

    adapter_cases = (
        (
            "review",
            diagnostics,
            "_validate_no_promotion_review_artifact_uncached",
            lambda: diagnostics.validate_no_promotion_review_artifact(
                review_id=review["review_id"], output_dir=tmp_path / "no_promotion_review"
            ),
        ),
        (
            "near_miss",
            diagnostics,
            "_validate_near_miss_candidates_artifact_uncached",
            lambda: diagnostics.validate_near_miss_candidates_artifact(
                near_miss_id=near_miss["near_miss_id"],
                output_dir=tmp_path / "near_miss_candidates",
            ),
        ),
        (
            "cash",
            diagnostics,
            "_validate_cash_buffer_attribution_artifact_uncached",
            lambda: diagnostics.validate_cash_buffer_attribution_artifact(
                attribution_id=attribution["attribution_id"],
                output_dir=tmp_path / "cash_buffer_attribution",
            ),
        ),
        (
            "coverage",
            diagnostics,
            "_validate_search_coverage_gap_artifact_uncached",
            lambda: diagnostics.validate_search_coverage_gap_artifact(
                coverage_gap_id=coverage["coverage_gap_id"],
                output_dir=tmp_path / "search_coverage_gap",
            ),
        ),
        (
            "scorecard",
            diagnostics.evaluation,
            "validate_weight_scorecard_artifact",
            lambda: diagnostics._validated_scorecard(
                fixture["scorecard"]["scorecard_id"], tmp_path / "weight_scorecard"
            ),
        ),
        (
            "search_space",
            diagnostics.foundation,
            "validate_weight_search_space_artifact",
            lambda: diagnostics._validated_search_space(
                fixture["search_space"]["search_space_id"], tmp_path / "weight_search_space"
            ),
        ),
    )
    with monkeypatch.context() as adapter_patch:
        for name, module, attribute, _ in adapter_cases:
            adapter_patch.setattr(module, attribute, counted_result(name))
        for name, _, _, adapter in adapter_cases:
            adapter()
            adapter()
            assert adapter_calls[name] == 1

    with monkeypatch.context() as fail_patch:
        fail_patch.setattr(
            diagnostics,
            "_validate_no_promotion_review_artifact_uncached",
            counted_result("fail", "FAIL"),
        )
        for _ in range(2):
            assert (
                diagnostics.validate_no_promotion_review_artifact(
                    review_id=review["review_id"], output_dir=tmp_path / "no_promotion_review"
                )["status"]
                == "FAIL"
            )
        assert adapter_calls["fail"] == 2

    exception_calls = 0

    def exploding_validator(**_: Any) -> dict[str, Any]:
        nonlocal exception_calls
        exception_calls += 1
        raise RuntimeError("diagnostics validator exception")

    for _ in range(2):
        with pytest.raises(RuntimeError, match="diagnostics validator exception"):
            validation_scope.validate_upstream_with_hardened_scope(
                validator=exploding_validator,
                validator_key="review_id",
                artifact_id=review["review_id"],
                output_dir=tmp_path / "no_promotion_review",
                snapshot_name="no_promotion_review_input_snapshot.json",
            )
    assert exception_calls == 2

    bypass_calls = 0

    def resolver_exception(**_: Any) -> None:
        raise LookupError("synthetic resolver failure")

    def bypass_validator(**_: Any) -> dict[str, Any]:
        nonlocal bypass_calls
        bypass_calls += 1
        return {"status": "PASS"}

    with monkeypatch.context() as resolver_patch:
        resolver_patch.setattr(
            validation_scope,
            "hardened_upstream_validation_scope",
            resolver_exception,
        )
        for _ in range(2):
            validation_scope.validate_upstream_with_hardened_scope(
                validator=bypass_validator,
                validator_key="review_id",
                artifact_id=review["review_id"],
                output_dir=tmp_path / "no_promotion_review",
                snapshot_name="no_promotion_review_input_snapshot.json",
            )
    assert bypass_calls == 2

    stages = (
        (
            Path(review["review_dir"]),
            "no_promotion_review_input_snapshot.json",
            diagnostics.REVIEW_INPUT_SCHEMA,
            lambda: diagnostics.validate_no_promotion_review_artifact(
                review_id=review["review_id"],
                output_dir=tmp_path / "no_promotion_review",
            ),
        ),
        (
            Path(near_miss["near_miss_dir"]),
            "near_miss_candidates_input_snapshot.json",
            diagnostics.NEAR_MISS_INPUT_SCHEMA,
            lambda: diagnostics.validate_near_miss_candidates_artifact(
                near_miss_id=near_miss["near_miss_id"],
                output_dir=tmp_path / "near_miss_candidates",
            ),
        ),
        (
            Path(attribution["attribution_dir"]),
            "cash_buffer_attribution_input_snapshot.json",
            diagnostics.CASH_INPUT_SCHEMA,
            lambda: diagnostics.validate_cash_buffer_attribution_artifact(
                attribution_id=attribution["attribution_id"],
                output_dir=tmp_path / "cash_buffer_attribution",
            ),
        ),
        (
            Path(coverage["coverage_gap_dir"]),
            "search_coverage_gap_input_snapshot.json",
            diagnostics.COVERAGE_INPUT_SCHEMA,
            lambda: diagnostics.validate_search_coverage_gap_artifact(
                coverage_gap_id=coverage["coverage_gap_id"],
                output_dir=tmp_path / "search_coverage_gap",
            ),
        ),
    )

    assert (
        sum(
            len(json.loads((root / snapshot).read_text(encoding="utf-8"))["view_hashes"])
            for root, snapshot, _, _ in stages
        )
        == 21
    )

    for root, snapshot_name, expected_schema, validator in stages:
        assert validator()["status"] == "PASS"
        snapshot_path = root / snapshot_name
        original_snapshot = snapshot_path.read_bytes()
        snapshot = json.loads(original_snapshot)
        assert snapshot["schema_version"] == expected_schema

        for view_name in snapshot["view_hashes"]:
            view_path = root / view_name
            original_view = view_path.read_bytes()
            view_path.write_bytes(original_view + b"\n")
            _assert_fail(validator)
            view_path.write_bytes(original_view)

        snapshot["schema_version"] = "tampered.schema"
        _write_json(snapshot_path, snapshot)
        _assert_fail(validator)
        snapshot_path.write_bytes(original_snapshot)
        assert validator()["status"] == "PASS"

    tamper_cases = (
        (
            Path(near_miss["near_miss_dir"]) / "near_miss_candidates_input_snapshot.json",
            "review_source",
            stages[1][3],
        ),
        (
            Path(attribution["attribution_dir"]) / "cash_buffer_attribution_input_snapshot.json",
            "near_miss_source",
            stages[2][3],
        ),
        (
            Path(coverage["coverage_gap_dir"]) / "search_coverage_gap_input_snapshot.json",
            "attribution_source",
            stages[3][3],
        ),
    )
    for snapshot_path, binding_name, validator in tamper_cases:
        original = snapshot_path.read_bytes()
        snapshot = json.loads(original)
        snapshot[binding_name]["artifact_id"] = "cross-lineage-artifact"
        _write_json(snapshot_path, snapshot)
        _assert_fail(validator)
        snapshot_path.write_bytes(original)

    review_snapshot_path = Path(review["review_dir"]) / "no_promotion_review_input_snapshot.json"
    original = review_snapshot_path.read_bytes()
    snapshot = json.loads(original)
    snapshot["policy_source"]["sha256"] = "0" * 64
    _write_json(review_snapshot_path, snapshot)
    _assert_fail(stages[0][3])
    review_snapshot_path.write_bytes(original)

    early_output = tmp_path / "chronology_must_not_materialize"
    with pytest.raises(diagnostics.DynamicV3WeightSearchDiagnosticsError):
        diagnostics.run_no_promotion_review(
            scorecard_id=fixture["scorecard"]["scorecard_id"],
            scorecard_dir=tmp_path / "weight_scorecard",
            output_dir=early_output,
            generated_at=datetime(2000, 1, 1, tzinfo=UTC),
        )
    assert not early_output.exists()
