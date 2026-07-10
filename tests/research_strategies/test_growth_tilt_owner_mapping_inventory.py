from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from ai_trading_system import dynamic_strategy_growth_tilt_owner_mapping_inventory as impl
from ai_trading_system.cli import app
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)
from ai_trading_system.research_quality import growth_tilt_owner_mapping_inventory as inventory
from ai_trading_system.yaml_loader import safe_load_yaml_path


def test_real_inventory_is_ready_for_owner_review_but_m2_remains_blocked() -> None:
    payload = _build()

    assert payload["status"] == inventory.READY_STATUS
    assert payload["m2_mapping_status"] == inventory.M2_BLOCKED_STATUS
    assert payload["owner_mapping_ready_count"] == 0
    assert payload["owner_mapping_required_count"] == 2
    assert payload["m2_eligible_candidate_count"] == 0
    assert payload["do_not_de_risk_pass"] is False
    assert payload["risk_on_veto_pass"] is True
    assert payload["strict_validation_errors"] == []


def test_re_risk_output_mapping_is_separate_from_offline_selection() -> None:
    payload = _build()
    row = _signal(payload, "re_risk_allowed_probability")

    assert row["channel"] == "defensive"
    assert row["source_family"] == "drawdown_recovery"
    assert row["callable_runtime_source"] is True
    assert row["pit_approved"] is False
    assert row["producer_callable"] is True
    assert row["output_path_resolved"] is True
    assert row["semantics_registered"] is True
    assert row["pit_lineage_valid"] is False
    assert row["baseline_consumption_ready"] is False
    assert row["offline_selection_pass"] is False
    assert row["offline_selection_role"] == (
        "OFFLINE_SELECTION_RESULT_NOT_RUNTIME_VALUE"
    )
    assert row["selection_status"] == "FAILED_FINAL_SELECTION"
    assert row["candidate_a_eligible"] is False
    assert "RECOVERY_PIT_LINEAGE_CONTRACT_MISSING" in row["blocker_codes"]
    assert "offline selection failure" in row["selection_notes"][1]
    assert payload["eligible_recovery_signal_ids"] == []


def test_recovery_persistence_contract_is_not_inferred_from_same_row_rule() -> None:
    payload = _build()

    assert payload["baseline"]["recovery_persistence_contract_ready"] is False
    assert "persistence" in payload["baseline"]["recovery_persistence_blocker"]
    assert (
        "A_BASELINE_RECOVERY_PERSISTENCE_CONTRACT_UNRESOLVED"
        in payload["mapping_blocker_codes"]
    )


def test_no_single_callable_pit_soft_confirmation_is_inventoried() -> None:
    payload = _build()
    rows = payload["baseline_confirmation_inventory"]["confirmations"]

    assert payload["eligible_soft_confirmation_ids"] == []
    assert all(row["candidate_b_eligible"] is False for row in rows)
    same_row = next(
        row
        for row in rows
        if row["confirmation_id"]
        == "do_not_de_risk_same_row_neutralization_threshold"
    )
    assert same_row["hard_or_soft"] == "UNCLASSIFIED"
    assert same_row["sole_transition_cause_traceable"] is False


def test_unrelated_dynamic_allocation_confirmation_is_not_reused() -> None:
    payload = _build()
    row = next(
        item
        for item in payload["baseline_confirmation_inventory"]["confirmations"]
        if item["confirmation_id"] == "dynamic_allocation_regime_confirmation_window"
    )

    assert row["required_steps"] == 3
    assert row["baseline_binding_status"] == "DIFFERENT_ASSET_UNIVERSE_AND_POLICY_FAMILY"
    assert row["pit_approved"] is False


def test_complete_hard_veto_set_is_checked_in_declared_order() -> None:
    payload = _build()

    assert payload["required_hard_veto_ids"] == list(inventory.EXPECTED_VETO_IDS)
    assert payload["resolved_hard_veto_ids"] == ["volatility_veto", "tqqq_veto"]
    assert payload["unresolved_hard_veto_ids"] == [
        "event_risk_veto",
        "risk_off_veto",
        "trend_break_veto",
    ]
    assert payload["baseline_veto_inventory"][
        "complete_callable_pit_valid_set_ready"
    ] is False


def test_risk_off_alias_is_not_treated_as_semantically_resolved() -> None:
    payload = _build()
    row = _veto(payload, "risk_off_veto")

    assert row["callable_runtime_source"] is True
    assert row["pit_approved"] is True
    assert row["semantic_binding_resolved"] is False
    assert "AMBIGUOUS_ALIAS" in row["semantic_binding_status"]


def test_event_and_trend_vetoes_have_no_valid_runtime_producer() -> None:
    payload = _build()

    for veto_id in ("event_risk_veto", "trend_break_veto"):
        row = _veto(payload, veto_id)
        assert row["callable_runtime_source"] is False
        assert row["pit_approved"] is False
        assert row["semantic_binding_resolved"] is False


def test_rates_liquidity_has_no_independent_baseline_veto_id() -> None:
    payload = _build()
    row = _veto(payload, "rates_liquidity_veto")

    assert row["required_by_baseline"] is False
    assert row["callable_runtime_source"] is False
    assert row["semantic_binding_resolved"] is False
    assert "NO_INDEPENDENT_BASELINE_VETO_ID" in row["semantic_binding_status"]


def test_regime_transitions_are_observed_not_governed() -> None:
    payload = _build()
    rows = payload["baseline_regime_inventory"]["regimes"]
    trend_rows = [row for row in rows if row["regime_type"] == "FIRST_LAYER_TREND_STATE"]

    assert [row["regime_id"] for row in trend_rows] == list(
        inventory.EXPECTED_TREND_STATES
    )
    assert all(
        row["transition_contract_status"] == "OBSERVED_NOT_GOVERNED"
        for row in trend_rows
    )
    assert payload["governed_regime_ids"] == []


def test_market_regime_is_not_misused_as_portfolio_state() -> None:
    payload = _build()
    market = next(
        row
        for row in payload["baseline_regime_inventory"]["regimes"]
        if row["regime_id"] == "ai_after_chatgpt"
    )

    assert market["regime_type"] == "MARKET_RESEARCH_WINDOW"
    assert market["transition_contract_status"] == "NOT_A_PORTFOLIO_STATE"


def test_qqq_equivalent_formula_and_cap_are_evidenced_but_scalar_binding_is_not() -> None:
    payload = _build()
    exposure = payload["baseline_exposure_unit_inventory"]["exposure"]

    assert exposure["unit"] == "QQQ_EQUIVALENT_EXPOSURE"
    assert exposure["formula"] == "QQQ_weight + 3.0 * TQQQ_weight"
    assert exposure["formula_callable_runtime_source"] is True
    assert exposure["maximum_value"] == 0.75
    assert exposure["minimum_increment"] is None
    assert exposure["scalar_binding_ready"] is False


def test_transition_samples_preserve_prior_evidence_without_cause_inference() -> None:
    payload = _build()
    section = payload["baseline_transition_trace_sample"]

    assert section["trace_sample_count"] >= 1
    assert section["decision_trace_schema_version"] is None
    assert section["sample_role"] == "PRIOR_RESEARCH_ARTIFACT_EVIDENCE_ONLY"
    assert all(
        row["cause_attribution_ready"] is False for row in section["trace_samples"]
    )


def test_source_artifacts_include_hash_size_and_absolute_path() -> None:
    payload = _build()

    assert payload["source_artifacts"]
    assert all(len(row["sha256"]) == 64 for row in payload["source_artifacts"])
    assert all(Path(row["path"]).is_absolute() for row in payload["source_artifacts"])
    assert all(row["size_bytes"] > 0 for row in payload["source_artifacts"])


def test_tracked_transition_snapshot_retains_original_prediction_provenance() -> None:
    payload = _build()
    source = next(
        row
        for row in payload["source_artifacts"]
        if row["path"].endswith("growth_tilt_baseline_transition_trace_source.csv")
    )

    assert source["schema_version"] == "growth_tilt_baseline_transition_trace_source.v1"
    for trace in payload["baseline_transition_trace_sample"]["trace_samples"]:
        for row_name in ("previous_baseline_row", "current_baseline_row"):
            row = trace[row_name]
            assert row["source_artifact_path"].endswith(
                "first_layer_composer_v2_predictions.csv"
            )
            assert len(row["source_artifact_sha256"]) == 64


def test_schema_drift_is_a_strict_error() -> None:
    sources = _sources()
    sources["channel_config"]["schema_version"] = "wrong.v1"
    payload = _build(sources=sources)

    assert "source_schema_mismatch:channel_config" in payload[
        "strict_validation_errors"
    ]
    assert payload["status"] == inventory.BLOCKED_STATUS


def test_veto_identity_drift_is_a_strict_error() -> None:
    sources = _sources()
    sources["risk_veto_policy"]["veto_types"] = list(
        reversed(sources["risk_veto_policy"]["veto_types"])
    )
    payload = _build(sources=sources)

    assert "baseline_veto_identity_or_order_mismatch" in payload[
        "strict_validation_errors"
    ]


def test_inventory_is_read_only_and_has_no_replay_or_approval_side_effect() -> None:
    payload = _build()

    assert payload["data_quality_gate_executed"] is False
    assert payload["data_quality_status"].startswith("NOT_APPLICABLE")
    assert payload["read_only_inventory"] is True
    assert payload["replay_run"] is False
    assert payload["owner_preregistration_completed"] is False
    assert payload["policy_approval_recorded"] is False
    assert payload["paper_shadow_enabled"] is False
    assert payload["production_effect"] == "none"
    assert payload["broker_action"] == "none"


def test_dynamic_runner_writes_required_inventory_artifacts(tmp_path: Path) -> None:
    payload = impl.run_growth_tilt_owner_mapping_inventory(
        output_root=tmp_path / "outputs",
        docs_root=tmp_path / "docs",
        as_of_date=impl.date(2026, 7, 10),
        strict=True,
    )

    assert payload["status"] == inventory.READY_STATUS
    for filename in (
        "owner_mapping_inventory_result.json",
        "baseline_signal_inventory.json",
        "baseline_confirmation_inventory.json",
        "baseline_veto_inventory.json",
        "baseline_regime_inventory.json",
        "baseline_exposure_unit_inventory.json",
        "baseline_transition_trace_sample.json",
    ):
        assert (tmp_path / "outputs" / filename).exists()
    assert (tmp_path / "docs" / "growth_tilt_owner_mapping_inventory.md").exists()


def test_dynamic_runner_strict_mode_rejects_missing_source(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="channel_config missing"):
        impl.run_growth_tilt_owner_mapping_inventory(
            channel_config_path=tmp_path / "missing.yaml",
            output_root=tmp_path / "outputs",
            docs_root=tmp_path / "docs",
            as_of_date=impl.date(2026, 7, 10),
            strict=True,
        )


def test_cli_real_inventory_run(tmp_path: Path) -> None:
    result = CliRunner().invoke(
        app,
        [
            "research",
            "strategies",
            "growth-tilt-owner-mapping-inventory",
            "--output-root",
            str(tmp_path / "outputs"),
            "--docs-root",
            str(tmp_path / "docs"),
            "--as-of",
            "2026-07-10",
            "--strict",
        ],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )

    assert result.exit_code == 0, result.output
    assert inventory.READY_STATUS in result.output
    assert "m2_mapping_status=BLOCKED_UNRESOLVED_BASELINE_RUNTIME_MAPPING" in result.output
    assert "owner_mapping_ready_count=0" in result.output
    assert "m2_eligible_candidate_count=0" in result.output
    assert "replay_run=false" in result.output


def test_registry_catalog_system_flow_and_task_register_are_aligned() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {row["report_id"]: row for row in registry["reports"]}
    entry = entries[inventory.REPORT_TYPE]

    assert entry["command"] == (
        "aits research strategies growth-tilt-owner-mapping-inventory"
    )
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert all(
        item in Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
        for item in inventory.REQUIRED_CATALOG_REFERENCES
    )
    assert all(
        item in Path("docs/system_flow.md").read_text(encoding="utf-8")
        for item in inventory.REQUIRED_FLOW_REFERENCES
    )
    assert (
        "TRADING-2438M1C_GROWTH_TILT_BASELINE_RUNTIME_MAPPING_INVENTORY_AND_"
        "OWNER_PREREGISTRATION"
        in Path("docs/task_register.md").read_text(encoding="utf-8")
    )


def _build(
    *, sources: dict[str, Any] | None = None
) -> dict[str, Any]:
    resolved_sources = sources or _sources()
    return inventory.build_growth_tilt_owner_mapping_inventory(
        resolved_sources,
        baseline_prediction_rows=impl._load_csv_rows(
            impl.DEFAULT_BASELINE_PREDICTIONS_PATH
        ),
        channel_prediction_rows=impl._load_csv_rows(
            impl.DEFAULT_CHANNEL_PREDICTIONS_PATH
        ),
        compiler_trace_rows=impl._load_csv_rows(impl.DEFAULT_COMPILER_TRACE_PATH),
        source_artifacts=impl._source_artifact_records(
            _all_default_paths(), resolved_sources, _yaml_default_paths()
        ),
        report_registry=resolved_sources["report_registry"],
        artifact_catalog_text=resolved_sources["artifact_catalog_text"],
        system_flow_text=resolved_sources["system_flow_text"],
        requirement_text=resolved_sources["requirement_text"],
        as_of="2026-07-10",
    )


def _sources() -> dict[str, Any]:
    sources = {
        key: copy.deepcopy(safe_load_yaml_path(path))
        for key, path in _yaml_default_paths().items()
    }
    sources.update(
        {
            "channel_code_text": impl.DEFAULT_CHANNEL_CODE_PATH.read_text(
                encoding="utf-8"
            ),
            "compiler_code_text": impl.DEFAULT_COMPILER_CODE_PATH.read_text(
                encoding="utf-8"
            ),
            "requirement_text": impl.DEFAULT_REQUIREMENT_DOC_PATH.read_text(
                encoding="utf-8"
            ),
            "artifact_catalog_text": impl.DEFAULT_ARTIFACT_CATALOG_PATH.read_text(
                encoding="utf-8"
            ),
            "system_flow_text": impl.DEFAULT_SYSTEM_FLOW_PATH.read_text(
                encoding="utf-8"
            ),
        }
    )
    return sources


def _yaml_default_paths() -> dict[str, Path]:
    return {
        "channel_config": impl.DEFAULT_CHANNEL_CONFIG_PATH,
        "signal_usage_matrix": impl.DEFAULT_SIGNAL_USAGE_MATRIX_PATH,
        "final_matrix": impl.DEFAULT_FINAL_MATRIX_PATH,
        "composer_config": impl.DEFAULT_COMPOSER_CONFIG_PATH,
        "base_policy": impl.DEFAULT_BASE_POLICY_PATH,
        "risk_veto_policy": impl.DEFAULT_RISK_VETO_POLICY_PATH,
        "probe_registry": impl.DEFAULT_PROBE_REGISTRY_PATH,
        "dynamic_allocation_policy": impl.DEFAULT_DYNAMIC_ALLOCATION_POLICY_PATH,
        "report_registry": impl.DEFAULT_REPORT_REGISTRY_PATH,
    }


def _all_default_paths() -> list[Path]:
    return [
        *_yaml_default_paths().values(),
        impl.DEFAULT_CHANNEL_CODE_PATH,
        impl.DEFAULT_COMPILER_CODE_PATH,
        impl.DEFAULT_REQUIREMENT_DOC_PATH,
        impl.DEFAULT_ARTIFACT_CATALOG_PATH,
        impl.DEFAULT_SYSTEM_FLOW_PATH,
        impl.DEFAULT_BASELINE_PREDICTIONS_PATH,
        impl.DEFAULT_CHANNEL_PREDICTIONS_PATH,
        impl.DEFAULT_COMPILER_TRACE_PATH,
    ]


def _signal(payload: dict[str, Any], signal_id: str) -> dict[str, Any]:
    return next(
        row
        for row in payload["baseline_signal_inventory"]["signals"]
        if row["signal_id"] == signal_id
    )


def _veto(payload: dict[str, Any], veto_id: str) -> dict[str, Any]:
    return next(
        row
        for row in payload["baseline_veto_inventory"]["vetoes"]
        if row["veto_id"] == veto_id
    )
