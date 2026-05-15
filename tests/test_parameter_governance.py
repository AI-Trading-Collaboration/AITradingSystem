from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.parameter_governance import (
    build_parameter_governance_report,
    render_parameter_governance_report,
)


def test_parameter_governance_report_maps_candidates_to_actions(tmp_path: Path) -> None:
    paths = _write_governance_inputs(tmp_path)

    report = build_parameter_governance_report(
        as_of=date(2026, 4, 10),
        manifest_path=paths["manifest"],
        candidate_ledger_path=paths["ledger"],
        generated_at=datetime.fromisoformat("2026-04-10T12:00:00+00:00"),
    )
    markdown = render_parameter_governance_report(report)

    assert report.status == "PASS_WITH_LIMITATIONS"
    assert report.action_counts == {
        "BLOCKED_BY_DATA": 1,
        "COLLECT_MORE_EVIDENCE": 1,
        "PREPARE_FORWARD_SHADOW": 1,
    }
    by_id = {item.parameter_id: item for item in report.parameters}
    assert by_id["weights"].action == "PREPARE_FORWARD_SHADOW"
    assert by_id["backtest_gate"].action == "BLOCKED_BY_DATA"
    assert by_id["sample_policy"].action == "COLLECT_MORE_EVIDENCE"
    assert "owner 暂无可量化配置输入" in markdown
    assert "production_effect：none" in markdown


def test_parameter_governance_cli_writes_markdown_and_summary(tmp_path: Path) -> None:
    paths = _write_governance_inputs(tmp_path)
    report_path = tmp_path / "parameter_governance.md"
    summary_path = tmp_path / "parameter_governance.json"

    result = CliRunner().invoke(
        app,
        [
            "feedback",
            "evaluate-parameter-governance",
            "--as-of",
            "2026-04-10",
            "--manifest-path",
            str(paths["manifest"]),
            "--parameter-candidate-ledger-path",
            str(paths["ledger"]),
            "--output-path",
            str(report_path),
            "--summary-output-path",
            str(summary_path),
        ],
    )

    assert result.exit_code == 0
    assert "参数治理状态：PASS_WITH_LIMITATIONS" in result.output
    assert report_path.exists()
    payload = json.loads(summary_path.read_text(encoding="utf-8"))
    assert payload["report_type"] == "parameter_governance"
    assert payload["production_effect"] == "none"
    assert payload["action_counts"]["PREPARE_FORWARD_SHADOW"] == 1


def test_parameter_governance_missing_config_fails_policy(tmp_path: Path) -> None:
    paths = _write_governance_inputs(tmp_path)
    raw = paths["manifest"].read_text(encoding="utf-8")
    paths["manifest"].write_text(
        raw.replace(_yaml_path(paths["weights_config"]), _yaml_path(tmp_path / "missing.yaml")),
        encoding="utf-8",
    )

    report = build_parameter_governance_report(
        as_of=date(2026, 4, 10),
        manifest_path=paths["manifest"],
        candidate_ledger_path=paths["ledger"],
    )

    assert report.status == "FAIL"
    weights = next(item for item in report.parameters if item.parameter_id == "weights")
    assert weights.action == "BLOCKED_BY_POLICY"
    assert weights.config_exists is False
    assert "配置路径不存在" in weights.action_reason


def _write_governance_inputs(tmp_path: Path) -> dict[str, Path]:
    weights_config = tmp_path / "weight_profile_current.yaml"
    gate_config = tmp_path / "backtest_validation_policy.yaml"
    sample_config = tmp_path / "feedback_sample_policy.yaml"
    for path in (weights_config, gate_config, sample_config):
        path.write_text("version: test\n", encoding="utf-8")
    manifest_path = tmp_path / "parameter_governance.yaml"
    weights_config_text = _yaml_path(weights_config)
    gate_config_text = _yaml_path(gate_config)
    sample_config_text = _yaml_path(sample_config)
    manifest_path.write_text(
        f"""
version: parameter_governance_test
status: pilot
owner: system
market_regime_id: ai_after_chatgpt
owner_quantitative_input_status: unavailable
production_effect: none
rationale: "test manifest"
validation: "test validation"
review_after: "test review"
actions:
  keep_current: "keep"
  collect_more_evidence: "collect"
  prepare_forward_shadow: "shadow"
  owner_decision_required: "owner"
  blocked_by_data: "data"
  blocked_by_policy: "policy"
parameters:
  - parameter_id: weights
    surface: module_weights
    config_path: "{weights_config_text}"
    source_level: pilot_prior
    owner: system
    status: active
    rationale: "weight rationale"
    validation_evidence: ["weight evidence"]
    review_after: "review"
    exit_condition: "exit"
    production_effect: none
    candidate_categories: [module_weight_perturbation]
    allowed_actions:
      - keep_current
      - collect_more_evidence
      - prepare_forward_shadow
      - owner_decision_required
      - blocked_by_data
      - blocked_by_policy
    requires_owner_quantitative_input_for_production: true
    allows_shadow_without_owner_quantitative_input: true
  - parameter_id: backtest_gate
    surface: backtest_candidate_evidence
    config_path: "{gate_config_text}"
    source_level: pilot_prior
    owner: system
    status: active
    rationale: "gate rationale"
    validation_evidence: ["gate evidence"]
    review_after: "review"
    exit_condition: "exit"
    production_effect: none
    candidate_categories: [cost]
    allowed_actions:
      - keep_current
      - collect_more_evidence
      - prepare_forward_shadow
      - owner_decision_required
      - blocked_by_data
      - blocked_by_policy
    requires_owner_quantitative_input_for_production: true
    allows_shadow_without_owner_quantitative_input: true
  - parameter_id: sample_policy
    surface: feedback_sample_floors
    config_path: "{sample_config_text}"
    source_level: temporary_baseline
    owner: system
    status: active
    rationale: "sample rationale"
    validation_evidence: ["sample evidence"]
    review_after: "review"
    exit_condition: "exit"
    production_effect: none
    candidate_categories: []
    allowed_actions:
      - keep_current
      - collect_more_evidence
      - owner_decision_required
      - blocked_by_policy
    requires_owner_quantitative_input_for_production: true
    allows_shadow_without_owner_quantitative_input: false
""".lstrip(),
        encoding="utf-8",
    )
    ledger_path = tmp_path / "parameter_candidates.json"
    ledger_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "report_type": "parameter_candidate_ledger",
                "status": "PASS_WITH_LIMITATIONS",
                "trial_count": 2,
                "candidate_count": 2,
                "warnings": ["存在候选被数据、OOS 或随机基线 veto。"],
                "candidates": [
                    {
                        "candidate_id": "candidate:weights",
                        "category": "module_weight_perturbation",
                        "recommendation_status": "READY_FOR_FORWARD_SHADOW",
                        "veto_reasons": [],
                    },
                    {
                        "candidate_id": "candidate:cost",
                        "category": "cost",
                        "recommendation_status": "BLOCKED_BY_DATA",
                        "veto_reasons": ["data_credibility_blocked"],
                    },
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return {
        "manifest": manifest_path,
        "ledger": ledger_path,
        "weights_config": weights_config,
    }


def _yaml_path(path: Path) -> str:
    return path.as_posix()
