from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli_commands.etf_portfolio import etf_app
from ai_trading_system.etf_portfolio.dynamic_allocation import (
    load_dynamic_allocation_policy_config,
)
from ai_trading_system.etf_portfolio.dynamic_rescue import (
    apply_dynamic_rescue_template,
    load_dynamic_failure_diagnostics_policy_config,
)
from ai_trading_system.etf_portfolio.dynamic_robustness import (
    _synthetic_validation_prices,
    load_dynamic_robustness_policy_config,
)
from ai_trading_system.etf_portfolio.dynamic_v3_real_evaluation import (
    DYNAMIC_V3_REAL_EVALUATION_REPORT_TYPE,
    build_dynamic_v3_real_evaluation_report,
    build_dynamic_v3_real_evaluation_validation_report,
    load_dynamic_v3_real_evaluation_policy_config,
    materialize_dynamic_v3_real_candidate_policy,
    write_dynamic_v3_real_evaluation_report,
)
from ai_trading_system.etf_portfolio.dynamic_v3_rescue import (
    load_dynamic_v3_rescue_policy_config,
)
from ai_trading_system.etf_portfolio.models import load_etf_config_bundle
from ai_trading_system.reports import reader_brief


def test_dynamic_v3_real_policy_loads_and_materializes_without_mutating_base() -> None:
    real_policy = load_dynamic_v3_real_evaluation_policy_config()
    v3_policy = load_dynamic_v3_rescue_policy_config()
    failure_policy = load_dynamic_failure_diagnostics_policy_config()
    dynamic_policy = load_dynamic_allocation_policy_config()
    v0_4_template = next(
        item
        for item in failure_policy.rescue_policy_templates
        if item.policy_id == real_policy.comparison.v0_4_policy_id
    )
    base_v0_4 = apply_dynamic_rescue_template(dynamic_policy, v0_4_template)
    before_hash = base_v0_4.model_dump(mode="json")

    candidate = materialize_dynamic_v3_real_candidate_policy(
        base_v0_4_policy=base_v0_4,
        template=v3_policy.candidate_templates[0],
        v3_rescue_policy=v3_policy,
        real_policy=real_policy,
    )

    assert candidate.default_policy_id == "dynamic_regime_overlay_v0_3a_constraint_smooth"
    assert candidate.policy_metadata.status == "candidate_only_real_evaluation"
    assert candidate.rebalance_policy.max_single_rebalance_delta <= 0.05
    assert base_v0_4.model_dump(mode="json") == before_hash
    assert real_policy.safety.production_state_mutated is False
    assert real_policy.safety.automatic_candidate_promotion is False


def test_dynamic_v3_real_evaluation_report_and_reader_brief(tmp_path: Path) -> None:
    report = _sample_real_evaluation_report()

    assert report["report_type"] == DYNAMIC_V3_REAL_EVALUATION_REPORT_TYPE
    assert report["promotion_gate_decision"] in {
        "promote_candidate",
        "observe_only",
        "reject",
    }
    assert report["summary"]["data_quality_status"] == "SYNTHETIC_VALIDATION_PASS"
    rows = report["comparison_table"]
    assert any(row["policy_id"] == "dynamic_regime_overlay_v0_1_baseline" for row in rows)
    assert any(row["policy_id"] == "dynamic_regime_overlay_v0_2_less_defensive" for row in rows)
    assert any(row["policy_id"] == "dynamic_regime_overlay_v0_4_lower_turnover" for row in rows)
    assert sum(1 for row in rows if row.get("group") == "dynamic_v0_3_rescue") == 4
    assert report["constraint_hit_analysis"]["status"] in {"PASS", "FAIL"}
    assert report["false_risk_off_analysis"]["status"] in {"PASS", "FAIL"}
    assert report["drawdown_preservation_analysis"]["status"] in {"PASS", "FAIL"}
    assert report["turnover_analysis"]["status"] in {"PASS", "FAIL"}
    assert report["static_gap_analysis"]["status"] in {"PASS", "FAIL"}
    assert report["overfit_analysis"]["status"] in {"PASS", "FAIL"}
    assert report["shadow_enrollment_allowed"] is False
    assert report["automatic_candidate_promotion"] is False

    paths = write_dynamic_v3_real_evaluation_report(report, output_dir=tmp_path / "reports")
    summary = reader_brief._etf_dynamic_v3_real_evaluation_summary(
        {
            "reports": [
                _report_record("etf_dynamic_v3_real_evaluation_report", paths["json"])
            ]
        }
    )
    assert summary["availability"] == "AVAILABLE"
    assert summary["promotion_gate_decision"] == report["promotion_gate_decision"]
    assert summary["best_candidate"] == report["best_candidate"]["policy_id"]
    assert summary["shadow_enrollment_allowed"] is False

    missing = reader_brief._etf_dynamic_v3_real_evaluation_summary({"reports": []})
    assert missing["availability"] == "MISSING"


def test_dynamic_v3_real_validation_report_and_cli_pass(tmp_path: Path) -> None:
    validation = build_dynamic_v3_real_evaluation_validation_report()

    assert validation["status"] == "PASS"
    assert validation["failed_check_count"] == 0
    assert validation["no_auto_approval"] is True
    assert validation["no_auto_enrollment"] is True

    result = CliRunner().invoke(
        etf_app,
        [
            "dynamic-v3-rescue",
            "validate-real",
            "--output-dir",
            str(tmp_path / "validation"),
        ],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )
    assert result.exit_code == 0, result.output
    assert "status=PASS" in result.output
    assert "automatic_enrollment_allowed=false" in result.output


def _sample_real_evaluation_report() -> dict[str, object]:
    real_policy = load_dynamic_v3_real_evaluation_policy_config()
    robustness_policy = load_dynamic_robustness_policy_config()
    return build_dynamic_v3_real_evaluation_report(
        prices=_synthetic_validation_prices(robustness_policy),
        etf_config=load_etf_config_bundle(),
        policy=real_policy,
        v3_rescue_policy=load_dynamic_v3_rescue_policy_config(),
        dynamic_robustness_policy=robustness_policy,
        dynamic_policy=load_dynamic_allocation_policy_config(),
        failure_policy=load_dynamic_failure_diagnostics_policy_config(),
        start=real_policy.market_regime.default_backtest_start,
        data_quality_status="SYNTHETIC_VALIDATION_PASS",
        data_quality_report="validation_sample",
        prices_path=Path("validation_sample_prices"),
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
