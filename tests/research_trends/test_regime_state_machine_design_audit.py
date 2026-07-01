from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.cli_commands.research_trends import trends_app
from ai_trading_system.regime_state_machine_design_audit import (
    DATA_QUALITY_STATUS,
    EXPECTED_LABELS,
    STATUS,
    build_anti_lookahead_guardrail_matrix,
    build_regime_label_taxonomy,
    run_regime_state_machine_design_audit,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path


def test_regime_state_machine_design_audit_cli_is_registered() -> None:
    result = CliRunner().invoke(trends_app, ["--help"])

    assert result.exit_code == 0
    assert "regime-state-machine-design-audit" in result.output


def test_regime_state_machine_design_policy_is_governed() -> None:
    policy = safe_load_yaml_path(
        Path("config/research/regime_state_machine_design_policy.yaml")
    )

    assert policy["policy_id"] == "regime_state_machine_design_policy"
    assert policy["version"] == "v1"
    assert policy["status"] == "pilot_research"
    assert policy["owner"] == "research_governance"
    assert policy["market_regime"] == "ai_after_chatgpt"
    assert policy["validation_evidence"]
    assert policy["review_condition"]
    assert policy["expiry_condition"]
    assert policy["data_quality"]["status"] == DATA_QUALITY_STATUS
    assert set(policy["label_taxonomy"]) == set(EXPECTED_LABELS)
    assert len(policy["transition_design"]["transition_rows"]) == 8
    assert len(policy["guardrails"]) == 6
    assert set(policy["candidate_segmentation_use_cases"]) == {
        "volatility_risk_cap",
        "breadth_proxy",
        "ai_leadership",
        "liquidity_rates",
    }

    safety = policy["safety"]
    assert safety["research_only"] is True
    assert safety["diagnostic_only"] is True
    assert safety["design_audit_only"] is True
    assert safety["candidate_signal_generated"] is False
    assert safety["regime_label_series_generated"] is False
    assert safety["generator_implemented"] is False
    assert safety["actual_path_validation_executed"] is False
    assert safety["promotion_allowed"] is False
    assert safety["paper_shadow_allowed"] is False
    assert safety["production_allowed"] is False
    assert safety["broker_action"] == "none"
    assert safety["dynamic_promotion_status"] == "BLOCKED"


def test_regime_state_machine_design_builders_preserve_boundaries() -> None:
    policy = safe_load_yaml_path(
        Path("config/research/regime_state_machine_design_policy.yaml")
    )

    labels = build_regime_label_taxonomy(policy)
    guardrails = build_anti_lookahead_guardrail_matrix(policy)

    assert [row["label_id"] for row in labels] == list(EXPECTED_LABELS)
    assert all(row["label_series_generated"] is False for row in labels)
    assert all(row["runtime_generation_allowed"] is False for row in labels)
    assert {row["guardrail_id"] for row in guardrails} >= {
        "no_future_outcome_labeling",
        "no_hindsight_episode_relabeling",
        "diagnostic_only_usage",
    }
    assert all(row["runtime_enforced_now"] is False for row in guardrails)
    assert all(
        row["must_be_enforced_before_label_generation"] is True
        for row in guardrails
    )


def test_regime_state_machine_design_registry_catalog_and_flow_are_non_promotion() -> None:
    registry = safe_load_yaml_path(Path("config/report_registry.yaml"))
    entry = next(
        report
        for report in registry["reports"]
        if report["report_id"] == "regime_state_machine_design_audit"
    )

    assert entry["command"] == "aits research trends regime-state-machine-design-audit"
    assert entry["artifact_role"] == "regime_state_machine_design_audit"
    assert entry["data_quality_status"] == DATA_QUALITY_STATUS
    assert entry["validation_status"] == STATUS
    assert entry["diagnostic_only"] is True
    assert entry["design_audit_only"] is True
    assert entry["candidate_signal_generated"] is False
    assert entry["regime_label_series_generated"] is False
    assert entry["generator_implemented"] is False
    assert entry["actual_path_validation_executed"] is False
    assert entry["promotion_eligible"] is False
    assert entry["paper_shadow_allowed"] is False
    assert entry["production_allowed"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert entry["dynamic_promotion_status"] == "BLOCKED"
    assert set(entry["label_ids"]) == set(EXPECTED_LABELS)
    assert set(entry["candidate_segmentation_use_cases"]) == {
        "volatility_risk_cap",
        "breadth_proxy",
        "ai_leadership",
        "liquidity_rates",
    }

    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    assert "regime_state_machine_design_audit" in catalog
    assert STATUS in catalog
    assert DATA_QUALITY_STATUS in catalog
    assert "不是 regime label series" in catalog

    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    assert "TRADING-2315" in system_flow
    assert "regime-state-machine-design-audit" in system_flow
    assert DATA_QUALITY_STATUS in system_flow
    assert "candidate_signal_generated=false" in system_flow


def test_regime_state_machine_design_cli_writes_outputs(tmp_path: Path) -> None:
    output_dir = tmp_path / "out"
    docs_root = tmp_path / "docs"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "regime-state-machine-design-audit",
            "--output-dir",
            str(output_dir),
            "--docs-root",
            str(docs_root),
        ],
    )

    assert result.exit_code == 0, result.output
    required = [
        "regime_state_machine_design_audit_summary.json",
        "regime_label_taxonomy.json",
        "regime_label_taxonomy.csv",
        "regime_transition_rule_matrix.json",
        "regime_transition_rule_matrix.csv",
        "regime_anti_lookahead_guardrail_matrix.json",
        "regime_anti_lookahead_guardrail_matrix.csv",
        "regime_candidate_segmentation_use_case_matrix.json",
        "regime_candidate_segmentation_use_case_matrix.csv",
        "regime_label_generator_poc_route.json",
        "regime_state_machine_safety_boundary.json",
    ]
    for filename in required:
        assert (output_dir / filename).exists(), filename
    assert not list(output_dir.glob("data_quality_*.md"))
    assert (docs_root / "regime_state_machine_design_audit.md").exists()

    summary_payload = json.loads(
        (output_dir / "regime_state_machine_design_audit_summary.json").read_text(
            encoding="utf-8"
        )
    )
    summary = summary_payload["summary"]
    assert summary_payload["status"] == STATUS
    assert summary["status"] == STATUS
    assert summary["market_regime"] == "ai_after_chatgpt"
    assert summary["actual_requested_date_range"] == "owner_static_design_audit"
    assert summary["data_quality_status"] == DATA_QUALITY_STATUS
    assert summary["label_count"] == 9
    assert summary["transition_rule_count"] == 8
    assert summary["guardrail_count"] == 6
    assert summary["candidate_segmentation_use_case_count"] == 4
    assert summary["next_task"] == "TRADING-2316_REGIME_LABEL_GENERATOR_DIAGNOSTIC_POC"
    assert summary["candidate_signal_generated"] is False
    assert summary["regime_label_series_generated"] is False
    assert summary["generator_implemented"] is False
    assert summary["promotion_allowed"] is False
    assert summary["paper_shadow_allowed"] is False
    assert summary["production_allowed"] is False
    assert summary["broker_action"] == "none"

    labels = json.loads(
        (output_dir / "regime_label_taxonomy.json").read_text(encoding="utf-8")
    )["rows"]
    assert [row["label_id"] for row in labels] == list(EXPECTED_LABELS)
    assert all(row["runtime_generation_allowed"] is False for row in labels)

    guardrails = json.loads(
        (output_dir / "regime_anti_lookahead_guardrail_matrix.json").read_text(
            encoding="utf-8"
        )
    )["rows"]
    assert {row["guardrail_id"] for row in guardrails} >= {
        "no_future_outcome_labeling",
        "no_hindsight_episode_relabeling",
        "diagnostic_only_usage",
    }

    use_cases = json.loads(
        (output_dir / "regime_candidate_segmentation_use_case_matrix.json").read_text(
            encoding="utf-8"
        )
    )["rows"]
    assert {row["use_case_id"] for row in use_cases} == {
        "volatility_risk_cap",
        "breadth_proxy",
        "ai_leadership",
        "liquidity_rates",
    }

    route = json.loads(
        (output_dir / "regime_label_generator_poc_route.json").read_text(
            encoding="utf-8"
        )
    )["generator_route"]
    assert route["route_status"] == "READY_FOR_DIAGNOSTIC_POC_DESIGN_ONLY"
    assert "regime_label_series" in route["blocked_until_trading_2316"]

    safety = json.loads(
        (output_dir / "regime_state_machine_safety_boundary.json").read_text(
            encoding="utf-8"
        )
    )
    assert safety["does_not_read_cached_market_data"] is True
    assert safety["does_not_generate_regime_label_series"] is True
    assert safety["does_not_generate_candidate_signal"] is True
    assert safety["does_not_allow_direct_strategy_signal"] is True
    assert safety["does_not_allow_position_sizing"] is True
    assert safety["does_not_allow_broker_action"] is True


def test_regime_state_machine_design_direct_payload_has_string_paths(
    tmp_path: Path,
) -> None:
    payload = run_regime_state_machine_design_audit(
        output_dir=tmp_path / "out",
        docs_root=tmp_path / "docs",
    )

    assert payload["status"] == STATUS
    assert all(isinstance(path, str) for path in payload["artifact_paths"].values())


def test_regime_state_machine_design_rejects_wrong_mode(tmp_path: Path) -> None:
    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "regime-state-machine-design-audit",
            "--output-dir",
            str(tmp_path / "out"),
            "--mode",
            "promotion",
        ],
    )

    assert result.exit_code != 0
