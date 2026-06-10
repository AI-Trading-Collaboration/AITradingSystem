from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli_commands.etf_portfolio import etf_app
from ai_trading_system.etf_portfolio.dynamic_allocation import (
    load_dynamic_allocation_policy_config,
)
from ai_trading_system.etf_portfolio.dynamic_rescue import (
    load_dynamic_failure_diagnostics_policy_config,
)
from ai_trading_system.etf_portfolio.dynamic_robustness import (
    _synthetic_validation_prices,
    load_dynamic_robustness_policy_config,
)
from ai_trading_system.etf_portfolio.dynamic_v3_failure_attribution import (
    DYNAMIC_V3_FAILURE_ATTRIBUTION_REPORT_TYPE,
    build_dynamic_v3_failure_attribution_report,
    load_dynamic_v3_failure_attribution_policy_config,
    write_dynamic_v3_failure_attribution_report,
)
from ai_trading_system.etf_portfolio.dynamic_v3_real_evaluation import (
    build_dynamic_v3_real_evaluation_report,
    load_dynamic_v3_real_evaluation_policy_config,
)
from ai_trading_system.etf_portfolio.dynamic_v3_rescue import (
    load_dynamic_v3_rescue_policy_config,
)
from ai_trading_system.etf_portfolio.models import load_etf_config_bundle
from ai_trading_system.reports import reader_brief


def test_dynamic_v3_failure_attribution_policy_loads() -> None:
    policy = load_dynamic_v3_failure_attribution_policy_config()

    assert policy.inputs.require_real_evaluation_reject is True
    assert policy.inputs.v0_4_policy_id == "dynamic_regime_overlay_v0_4_lower_turnover"
    assert policy.constraint_attribution.material_hit_count_delta >= 0
    assert policy.v0_4_promotion_review.promote_v0_4_status == "promote_v0_4"
    assert policy.v0_5_design.constraint_guard_status == "recommend_v0_5_constraint_guard"
    assert policy.safety.production_state_mutated is False
    assert policy.safety.shadow_enrollment_allowed is False


def test_dynamic_v3_failure_attribution_report_and_reader_brief(tmp_path: Path) -> None:
    report = _sample_failure_attribution_report()

    assert report["report_type"] == DYNAMIC_V3_FAILURE_ATTRIBUTION_REPORT_TYPE
    assert report["real_evaluation_decision"] in {
        "promote_candidate",
        "observe_only",
        "reject",
    }
    assert report["summary"]["v0_4_promotion_review"] in {
        "promote_v0_4",
        "observe_v0_4_with_constraint_guard",
        "do_not_promote_v0_4",
    }
    assert report["summary"]["v0_5_design_recommendation"] in {
        "not_required",
        "recommend_v0_5_constraint_guard",
        "recommend_v0_5_exposure_redesign",
    }
    assert report["v0_3_vs_v0_4_metric_delta_table"]
    assert report["constraint_hit_failure_bucket_breakdown"]["v0_3"]["row_count"] > 0
    assert report["drawdown_degradation_attribution"]["conclusion"]
    assert report["robustness_overfit_review_required_explanation"]["v0_3"]["status"]
    assert report["shadow_enrollment_allowed"] is False
    assert report["automatic_candidate_promotion"] is False
    assert report["validation_context"]["daily_paths_persisted_in_report"] is False

    paths = write_dynamic_v3_failure_attribution_report(
        report,
        output_dir=tmp_path / "reports",
    )
    summary = reader_brief._etf_dynamic_v3_failure_attribution_summary(
        {"reports": [_report_record("etf_dynamic_v3_failure_attribution_report", paths["json"])]}
    )
    assert summary["availability"] == "AVAILABLE"
    assert summary["v0_4_promotion_review"] == report["summary"]["v0_4_promotion_review"]
    assert summary["v0_5_design_recommendation"] == report["summary"]["v0_5_design_recommendation"]
    assert summary["shadow_enrollment_allowed"] is False

    missing = reader_brief._etf_dynamic_v3_failure_attribution_summary({"reports": []})
    assert missing["availability"] == "MISSING"


def test_dynamic_v3_failure_attribution_validation_report_and_cli_pass(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "validation"
    result = CliRunner().invoke(
        etf_app,
        [
            "dynamic-v3-rescue",
            "validate-attribution",
            "--output-dir",
            str(output_dir),
        ],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )
    assert result.exit_code == 0, result.output
    assert "status=PASS" in result.output
    assert "automatic_enrollment_allowed=false" in result.output
    validation = _single_validation_payload(output_dir)

    assert validation["status"] == "PASS"
    assert validation["failed_check_count"] == 0
    assert validation["no_auto_approval"] is True
    assert validation["no_auto_enrollment"] is True


def _sample_failure_attribution_report() -> dict[str, object]:
    policy = load_dynamic_v3_failure_attribution_policy_config()
    real_policy = load_dynamic_v3_real_evaluation_policy_config()
    robustness_policy = load_dynamic_robustness_policy_config()
    prices = _synthetic_validation_prices(robustness_policy)
    etf_config = load_etf_config_bundle()
    v3_policy = load_dynamic_v3_rescue_policy_config()
    dynamic_policy = load_dynamic_allocation_policy_config()
    failure_policy = load_dynamic_failure_diagnostics_policy_config()
    real_report = build_dynamic_v3_real_evaluation_report(
        prices=prices,
        etf_config=etf_config,
        policy=real_policy,
        v3_rescue_policy=v3_policy,
        dynamic_robustness_policy=robustness_policy,
        dynamic_policy=dynamic_policy,
        failure_policy=failure_policy,
        start=policy.market_regime.default_backtest_start,
        data_quality_status="SYNTHETIC_VALIDATION_PASS",
        data_quality_report="validation_sample",
        prices_path=Path("validation_sample_prices"),
    )
    return build_dynamic_v3_failure_attribution_report(
        prices=prices,
        etf_config=etf_config,
        policy=policy,
        real_evaluation_report=real_report,
        real_evaluation_report_path=Path("validation_sample_real_evaluation"),
        real_policy=real_policy,
        v3_rescue_policy=v3_policy,
        dynamic_robustness_policy=robustness_policy,
        dynamic_policy=dynamic_policy,
        failure_policy=failure_policy,
        start=policy.market_regime.default_backtest_start,
        data_quality_status="SYNTHETIC_VALIDATION_PASS",
        data_quality_report="validation_sample",
        prices_path=Path("validation_sample_prices"),
        allow_non_reject_for_validation=True,
    )


def _report_record(report_id: str, path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return {
        "report_id": report_id,
        "latest_artifact_path": str(path),
        "latest_artifact_name": path.name,
        "artifact_date": "2026-06-06",
        "freshness_status": "FRESH",
        "artifact_status": payload.get("status", "PASS"),
        "exists": True,
        "age_days": 0,
    }


def _single_validation_payload(output_dir: Path) -> dict[str, object]:
    paths = list(output_dir.glob("dynamic-v3-failure-attribution-validation_*.json"))
    assert len(paths) == 1
    return json.loads(paths[0].read_text(encoding="utf-8"))
