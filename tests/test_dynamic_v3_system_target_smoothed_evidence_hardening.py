from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest
import yaml
from dynamic_v3_system_target_helpers import run_smoothed_review_chain_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target
from ai_trading_system.etf_portfolio import (
    dynamic_v3_system_target_smoothed_evidence as evidence,
)
from ai_trading_system.platform.artifacts.validation_session import (
    with_artifact_validation_session,
)


def _run_evidence_chain(tmp_path: Path) -> dict[str, object]:
    fixture = run_smoothed_review_chain_fixture(tmp_path)
    attribution = system_target.run_smoothed_review_attribution(
        review_id=fixture["review"]["review_id"],
        comparison_id=fixture["comparison"]["comparison_id"],
        backfill_id=fixture["smoothed"]["smoothed_backfill_id"],
        review_dir=tmp_path / "smoothed_review",
        comparison_dir=tmp_path / "smoothed_comparison",
        backfill_dir=tmp_path / "smoothed_backfill",
        output_dir=tmp_path / "smoothed_review_attribution",
    )
    benefit = system_target.run_smoothing_benefit_lag_drilldown(
        smoothed_backfill_id=fixture["smoothed"]["smoothed_backfill_id"],
        comparison_id=fixture["comparison"]["comparison_id"],
        backfill_dir=tmp_path / "smoothed_backfill",
        comparison_dir=tmp_path / "smoothed_comparison",
        output_dir=tmp_path / "smoothing_benefit_lag",
    )
    regime = system_target.run_smoothed_regime_validation(
        smoothed_backfill_id=fixture["smoothed"]["smoothed_backfill_id"],
        smoothed_backfill_dir=tmp_path / "smoothed_backfill",
        baseline_backfill_dir=tmp_path / "paper_shadow_backfill",
        output_dir=tmp_path / "smoothed_regime_validation",
    )
    confirmation = system_target.register_smoothed_confirmation_targets(
        review_id=fixture["review"]["review_id"],
        regime_validation_id=regime["regime_validation_id"],
        review_dir=tmp_path / "smoothed_review",
        regime_validation_dir=tmp_path / "smoothed_regime_validation",
        output_dir=tmp_path / "smoothed_forward_confirmation",
    )
    watch = system_target.run_smoothed_watch_pack(
        review_attribution_id=attribution["attribution_id"],
        benefit_lag_id=benefit["drilldown_id"],
        regime_validation_id=regime["regime_validation_id"],
        confirmation_id=confirmation["confirmation_id"],
        attribution_dir=tmp_path / "smoothed_review_attribution",
        benefit_lag_dir=tmp_path / "smoothing_benefit_lag",
        regime_validation_dir=tmp_path / "smoothed_regime_validation",
        confirmation_dir=tmp_path / "smoothed_forward_confirmation",
        output_dir=tmp_path / "smoothed_watch_pack",
    )
    return {
        **fixture,
        "attribution": attribution,
        "benefit": benefit,
        "regime": regime,
        "confirmation": confirmation,
        "watch": watch,
    }


def _write_evidence_policy(
    path: Path, *, minimum_regime_observations: int | None = None
) -> Path:
    source = yaml.safe_load(
        evidence.DEFAULT_SMOOTHED_LIMITED_CONFIG_PATH.read_text(encoding="utf-8")
    )
    if minimum_regime_observations is not None:
        source["evidence_policy"][
            "minimum_regime_observations"
        ] = minimum_regime_observations
    path.write_text(
        yaml.safe_dump(source, sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )
    return path


@with_artifact_validation_session
def test_smoothed_evidence_chain_is_replayable_and_preserves_no_candidate(
    tmp_path: Path,
) -> None:
    chain = _run_evidence_chain(tmp_path)

    confirmation = chain["confirmation"]["smoothed_confirmation_targets"]
    watch = chain["watch"]["smoothed_watch_summary"]
    assert confirmation["candidate_method"] is None
    assert confirmation["targets"] == []
    assert confirmation["status"] == "INSUFFICIENT_EVIDENCE"
    assert watch["candidate_method"] is None
    assert watch["forward_confirmation_status"] == "NOT_REGISTERED"

    validations = [
        system_target.validate_smoothed_review_attribution_artifact(
            attribution_id=chain["attribution"]["attribution_id"],
            output_dir=tmp_path / "smoothed_review_attribution",
        ),
        system_target.validate_smoothing_benefit_lag_artifact(
            drilldown_id=chain["benefit"]["drilldown_id"],
            output_dir=tmp_path / "smoothing_benefit_lag",
        ),
        system_target.validate_smoothed_regime_validation_artifact(
            regime_validation_id=chain["regime"]["regime_validation_id"],
            output_dir=tmp_path / "smoothed_regime_validation",
        ),
        system_target.validate_smoothed_confirmation_artifact(
            confirmation_id=chain["confirmation"]["confirmation_id"],
            output_dir=tmp_path / "smoothed_forward_confirmation",
        ),
        system_target.validate_smoothed_watch_pack_artifact(
            watch_pack_id=chain["watch"]["watch_pack_id"],
            output_dir=tmp_path / "smoothed_watch_pack",
        ),
    ]
    assert {row["status"] for row in validations} == {"PASS"}


@with_artifact_validation_session
def test_attribution_rejects_valid_but_cross_comparison_lineage_before_output(
    tmp_path: Path,
) -> None:
    fixture = run_smoothed_review_chain_fixture(tmp_path)
    comparison_two = system_target.run_smoothed_comparison(
        smoothed_backfill_id=fixture["smoothed"]["smoothed_backfill_id"],
        baseline_backfill_id=fixture["smoothed"]["source_paper_shadow_backfill"][
            "backfill_id"
        ],
        risk_capped_backfill_id=fixture["risk_capped"]["risk_capped_backfill_id"],
        smoothed_backfill_dir=tmp_path / "smoothed_backfill",
        baseline_backfill_dir=tmp_path / "paper_shadow_backfill",
        risk_capped_backfill_dir=tmp_path / "risk_capped_backfill",
        output_dir=tmp_path / "smoothed_comparison",
    )
    review_two = system_target.build_smoothed_review_pack(
        comparison_id=comparison_two["comparison_id"],
        smoothed_backfill_id=fixture["smoothed"]["smoothed_backfill_id"],
        comparison_dir=tmp_path / "smoothed_comparison",
        smoothed_backfill_dir=tmp_path / "smoothed_backfill",
        output_dir=tmp_path / "smoothed_review",
    )
    output_dir = tmp_path / "cross_lineage_attribution"

    with pytest.raises(
        system_target.DynamicV3SystemTargetError,
        match="review/comparison lineage mismatch",
    ):
        system_target.run_smoothed_review_attribution(
            review_id=review_two["review_id"],
            comparison_id=fixture["comparison"]["comparison_id"],
            backfill_id=fixture["smoothed"]["smoothed_backfill_id"],
            review_dir=tmp_path / "smoothed_review",
            comparison_dir=tmp_path / "smoothed_comparison",
            backfill_dir=tmp_path / "smoothed_backfill",
            output_dir=output_dir,
        )
    assert not output_dir.exists()


@with_artifact_validation_session
def test_regime_policy_floor_keeps_insufficient_samples_null(tmp_path: Path) -> None:
    fixture = run_smoothed_review_chain_fixture(tmp_path)
    config_path = _write_evidence_policy(
        tmp_path / "smoothed_evidence_high_floor.yaml",
        minimum_regime_observations=100_000,
    )
    regime = system_target.run_smoothed_regime_validation(
        smoothed_backfill_id=fixture["smoothed"]["smoothed_backfill_id"],
        smoothed_backfill_dir=tmp_path / "smoothed_backfill",
        baseline_backfill_dir=tmp_path / "paper_shadow_backfill",
        output_dir=tmp_path / "smoothed_regime_validation",
        config_path=config_path,
    )

    for row in regime["sideways_validation_summary"]["methods"]:
        assert row["sideways_status"] == "INSUFFICIENT_DATA"
        assert row["return_delta_vs_limited"] is None
        assert row["drawdown_delta_vs_limited"] is None
    for row in regime["recovery_lag_validation_summary"]["methods"]:
        assert row["lag_status"] == "INSUFFICIENT_DATA"
        assert row["return_delta_vs_limited"] is None
        assert row["missed_upside"] is None
    assert (
        system_target.validate_smoothed_regime_validation_artifact(
            regime_validation_id=regime["regime_validation_id"],
            output_dir=tmp_path / "smoothed_regime_validation",
        )["status"]
        == "PASS"
    )


def test_regime_rejects_missing_turnover_instead_of_filling_zero(tmp_path: Path) -> None:
    fixture = run_smoothed_review_chain_fixture(tmp_path)
    smoothed = copy.deepcopy(fixture["smoothed"])
    baseline = copy.deepcopy(smoothed["source_paper_shadow_backfill"])
    baseline["paper_shadow_backfill_input_snapshot"] = json.loads(
        (
            tmp_path
            / "paper_shadow_backfill"
            / baseline["backfill_id"]
            / "paper_shadow_backfill_input_snapshot.json"
        ).read_text(encoding="utf-8")
    )
    for row in smoothed["smoothed_method_states"]:
        row["turnover"] = None
    config = yaml.safe_load(
        evidence.DEFAULT_SMOOTHED_LIMITED_CONFIG_PATH.read_text(encoding="utf-8")
    )

    with pytest.raises(
        evidence.DynamicV3SmoothedEvidenceError,
        match="turnover observations must be finite",
    ):
        evidence._regime_summaries(
            smoothed,
            baseline,
            config["evidence_policy"],
            evidence.method._evaluation_policy(config),
        )


def test_confirmation_targets_follow_eligible_method_instead_of_fixed_3d() -> None:
    method_name = "smooth_weights_5d_limited_adjustment"
    review = {
        "review_id": "review-5d",
        "smoothed_decision": {
            "decision": "CONTINUE_OBSERVATION",
            "decision_confidence": "LOW",
            "recommended_method": method_name,
            "candidate_evidence": [
                {"method": method_name, "promotion_eligible": True}
            ],
        },
    }
    regime = {
        "sideways_validation_summary": {
            "methods": [{"method": method_name, "sideways_status": "IMPROVED"}]
        },
        "recovery_lag_validation_summary": {
            "methods": [{"method": method_name, "lag_status": "LOW"}]
        },
    }
    config = yaml.safe_load(
        evidence.DEFAULT_SMOOTHED_LIMITED_CONFIG_PATH.read_text(encoding="utf-8")
    )

    result = evidence._confirmation_targets(
        review,
        regime,
        evidence._evidence_policy(config),
    )

    assert result["candidate_method"] == method_name
    assert len(result["targets"]) == 4
    assert {row["method"] for row in result["targets"]} == {method_name}
    assert {row["target_id"] for row in result["targets"]} == {
        "smooth_weights_5d_vs_limited",
        "smooth_weights_5d_vs_static_baseline",
        "smooth_weights_5d_sideways_choppy_improvement",
        "smooth_weights_5d_recovery_lag_watch",
    }


@with_artifact_validation_session
def test_policy_drift_and_render_tamper_fail_validation(tmp_path: Path) -> None:
    fixture = run_smoothed_review_chain_fixture(tmp_path)
    config_path = _write_evidence_policy(tmp_path / "smoothed_evidence_policy.yaml")
    attribution = system_target.run_smoothed_review_attribution(
        review_id=fixture["review"]["review_id"],
        comparison_id=fixture["comparison"]["comparison_id"],
        backfill_id=fixture["smoothed"]["smoothed_backfill_id"],
        review_dir=tmp_path / "smoothed_review",
        comparison_dir=tmp_path / "smoothed_comparison",
        backfill_dir=tmp_path / "smoothed_backfill",
        output_dir=tmp_path / "smoothed_review_attribution",
        config_path=config_path,
    )
    root = attribution["attribution_dir"]
    report = root / "smoothed_review_attribution_report.md"
    report.write_text(report.read_text(encoding="utf-8") + "\ntampered\n", encoding="utf-8")
    assert (
        system_target.validate_smoothed_review_attribution_artifact(
            attribution_id=attribution["attribution_id"],
            output_dir=tmp_path / "smoothed_review_attribution",
        )["status"]
        == "FAIL"
    )

    snapshot = json.loads(
        (root / "smoothed_review_attribution_input_snapshot.json").read_text(
            encoding="utf-8"
        )
    )
    report.write_bytes(
        evidence._attribution_views(
            snapshot,
            attribution_id=attribution["attribution_id"],
            root=root,
        )[0]["smoothed_review_attribution_report.md"]
    )
    policy = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    policy["evidence_policy"]["required_forward_events"] += 1
    config_path.write_text(
        yaml.safe_dump(policy, sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )
    assert (
        system_target.validate_smoothed_review_attribution_artifact(
            attribution_id=attribution["attribution_id"],
            output_dir=tmp_path / "smoothed_review_attribution",
        )["status"]
        == "FAIL"
    )


@with_artifact_validation_session
def test_watch_rejects_cross_comparison_confirmation_before_output(tmp_path: Path) -> None:
    chain = _run_evidence_chain(tmp_path)
    comparison_two = system_target.run_smoothed_comparison(
        smoothed_backfill_id=chain["smoothed"]["smoothed_backfill_id"],
        baseline_backfill_id=chain["smoothed"]["source_paper_shadow_backfill"][
            "backfill_id"
        ],
        risk_capped_backfill_id=chain["risk_capped"]["risk_capped_backfill_id"],
        smoothed_backfill_dir=tmp_path / "smoothed_backfill",
        baseline_backfill_dir=tmp_path / "paper_shadow_backfill",
        risk_capped_backfill_dir=tmp_path / "risk_capped_backfill",
        output_dir=tmp_path / "smoothed_comparison",
    )
    review_two = system_target.build_smoothed_review_pack(
        comparison_id=comparison_two["comparison_id"],
        smoothed_backfill_id=chain["smoothed"]["smoothed_backfill_id"],
        comparison_dir=tmp_path / "smoothed_comparison",
        smoothed_backfill_dir=tmp_path / "smoothed_backfill",
        output_dir=tmp_path / "smoothed_review",
    )
    confirmation_two = system_target.register_smoothed_confirmation_targets(
        review_id=review_two["review_id"],
        regime_validation_id=chain["regime"]["regime_validation_id"],
        review_dir=tmp_path / "smoothed_review",
        regime_validation_dir=tmp_path / "smoothed_regime_validation",
        output_dir=tmp_path / "smoothed_forward_confirmation",
    )
    output_dir = tmp_path / "cross_lineage_watch"

    with pytest.raises(
        system_target.DynamicV3SystemTargetError,
        match="watch (review_id|comparison_id) mismatch",
    ):
        system_target.run_smoothed_watch_pack(
            review_attribution_id=chain["attribution"]["attribution_id"],
            benefit_lag_id=chain["benefit"]["drilldown_id"],
            regime_validation_id=chain["regime"]["regime_validation_id"],
            confirmation_id=confirmation_two["confirmation_id"],
            attribution_dir=tmp_path / "smoothed_review_attribution",
            benefit_lag_dir=tmp_path / "smoothing_benefit_lag",
            regime_validation_dir=tmp_path / "smoothed_regime_validation",
            confirmation_dir=tmp_path / "smoothed_forward_confirmation",
            output_dir=output_dir,
        )
    assert not output_dir.exists()
