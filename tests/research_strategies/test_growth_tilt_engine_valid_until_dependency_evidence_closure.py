from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

import ai_trading_system.dynamic_strategy_growth_tilt_engine_contract_gap_remediation_plan as m2411
import ai_trading_system.dynamic_strategy_valid_until_window_stale_signal_remediation_plan as m2407
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_pit_gate_readiness_snapshot as m2415,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_pit_gate_remaining_blocker_closure_plan as m2416,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_signal_validity_dependency_remediation as m2414,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_source_traceability_upstream_artifact_closure as m2417,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_valid_until_dependency_evidence_closure as impl,
)
from ai_trading_system.cli import app
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)
from ai_trading_system.research_quality import (
    growth_tilt_engine_valid_until_dependency_evidence_closure as closure,
)


def test_valid_until_dependency_evidence_closure_builder_preserves_gate_state() -> None:
    sources = _source_documents()
    payload = closure.build_growth_tilt_valid_until_dependency_evidence_closure(
        sources["closure_result_2417"],
        sources["remaining_blocker_summary_2417"],
        sources["closure_result_2416"],
        sources["remaining_blocker_matrix_2416"],
        sources["valid_until_dependency_closure_plan_2416"],
        sources["readiness_snapshot_result_2415"],
        sources["readiness_matrix_2415"],
        sources["signal_validity_dependency_remediation_result_2414"],
        sources["signal_validity_dependency_contract_metadata_2414"],
        sources["remaining_blocker_summary_2414"],
        sources["remediation_plan_result_2411"],
        sources["remediation_plan_result_2407"],
        sources["valid_until_semantics_review_2407"],
        sources["stale_signal_risk_audit_2407"],
        sources["signal_validity_contract_plan_2407"],
        sources["validation_plan_2407"],
        pit_input_registry=_pit_input_registry(),
        strategy_execution_policy_registry=_strategy_execution_policy_registry(),
    )

    assert payload["status"] == closure.READY_STATUS
    assert payload["source_feature_count"] == 10
    assert payload["pit_gate_ready_count"] == 0
    assert payload["contract_ready_count"] == 0
    assert payload["pit_gate_blocked_count"] == 10
    assert payload["blocked_by_valid_until_window_count"] == 1
    assert payload["valid_until_window_dependency_blocker_count_from_2415"] == 1
    assert payload["valid_until_dependency_evidence_ready"] is True
    assert payload["signal_validity_contract_evidence_ready"] is True
    assert payload["stale_signal_policy_evidence_ready"] is True
    assert payload["growth_tilt_valid_until_alignment_evidence_ready"] is True
    assert payload["remaining_blocker_summary_ready"] is True
    assert payload["source_traceability_still_blocked"] == [
        "growth_tilt_engine_signal_artifact"
    ]
    assert payload["pit_gate_recheck_required"] is True
    assert payload["auto_mark_pit_gate_ready"] is False
    assert payload["auto_mark_contract_ready"] is False
    assert payload["growth_tilt_engine_blocking_gap_resolved"] is False
    assert payload["valid_until_window_blocking_gap_resolved"] is False
    assert payload["recommended_next_research_task"] == closure.NEXT_ROUTE

    evidence_row = payload["valid_until_dependency_evidence"]["evidence_rows"][0]
    assert evidence_row["dependent_feature_or_signal"] == (
        "execution_signal_validity_policy"
    )
    assert evidence_row["evidence_status"] == "CLOSED_WITH_EVIDENCE"
    assert evidence_row["ready_for_pit_gate_recheck"] is True
    assert evidence_row["pit_gate_ready_after_2418"] is False

    contract = payload["signal_validity_contract_evidence"]
    assert contract["required_field_count"] == 13
    assert contract["evidence_available_count"] == 13
    assert contract["ready_for_recheck"] is True

    stale_policy = payload["stale_signal_policy_evidence"]
    assert stale_policy["required_policy_invariants"] == {
        "expired_signal_cannot_trigger_new_trade": True,
        "missing_valid_until_blocks_dependent_strategy_recheck": True,
        "carry_forward_requires_explicit_rule": True,
        "owner_review_required_for_carry_forward_in_observation_context": True,
    }


def test_growth_tilt_engine_valid_until_dependency_evidence_closure_strategy(
    tmp_path: Path,
) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "closure"
    docs_root = tmp_path / "docs" / "research"

    payload = impl.run_growth_tilt_engine_valid_until_dependency_evidence_closure(
        **_source_kwargs(paths),
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 8),
    )

    assert payload["status"] == impl.READY_STATUS
    assert payload["source_validation_errors"] == []
    assert payload["valid_until_dependency_evidence_ready"] is True
    assert payload["signal_validity_contract_evidence_ready"] is True
    assert payload["stale_signal_policy_evidence_ready"] is True
    assert payload["growth_tilt_valid_until_alignment_evidence_ready"] is True
    assert payload["remaining_blocker_summary_ready"] is True
    assert payload["source_traceability_still_blocked"] == [
        "growth_tilt_engine_signal_artifact"
    ]
    assert payload["pit_gate_ready_count"] == 0
    assert payload["contract_ready_count"] == 0
    assert payload["data_quality_gate_executed"] is False
    assert payload["data_quality_gate_reason"] == impl.DATA_QUALITY_GATE_REASON
    assert payload["fresh_market_data_read"] is False
    assert payload["production_effect"] == "none"
    assert payload["broker_action"] == "none"
    for field in impl.SAFETY_FALSE_FIELDS:
        assert payload[field] is False

    for key in (
        "json_path",
        "valid_until_dependency_evidence_json",
        "signal_validity_contract_evidence_json",
        "stale_signal_policy_evidence_json",
        "growth_tilt_valid_until_alignment_evidence_json",
        "remaining_blocker_summary_json",
        "markdown_path",
        "signal_validity_contract_evidence_markdown",
        "stale_signal_policy_evidence_markdown",
        "growth_tilt_valid_until_alignment_evidence_markdown",
        "next_route_markdown",
    ):
        assert Path(payload["artifact_paths"][key]).exists()


def test_growth_tilt_engine_valid_until_dependency_evidence_closure_cli(
    tmp_path: Path,
) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "closure_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "growth-tilt-engine-valid-until-dependency-evidence-closure",
            *_source_args(paths),
            "--as-of",
            "2026-07-08",
            "--output-root",
            str(output_root),
            "--docs-root",
            str(docs_root),
        ],
        env={"COLUMNS": "240"},
        terminal_width=240,
    )

    assert result.exit_code == 0, result.output
    assert impl.READY_STATUS in result.output
    assert "valid_until_dependency_evidence_ready=true" in result.output
    assert "signal_validity_contract_evidence_ready=true" in result.output
    assert "stale_signal_policy_evidence_ready=true" in result.output
    assert "growth_tilt_valid_until_alignment_evidence_ready=true" in result.output
    assert "remaining_blocker_summary_ready=true" in result.output
    assert "pit_gate_recheck_required=true" in result.output
    assert "auto_mark_pit_gate_ready=false" in result.output
    assert "auto_mark_contract_ready=false" in result.output
    assert "candidate_search_resumed=false" in result.output
    assert "paper_shadow_enabled=false" in result.output
    assert "event_append_enabled=false" in result.output
    assert "outcome_binding_enabled=false" in result.output
    assert "scheduler_enabled=false" in result.output
    assert "production_enabled=false" in result.output
    assert "broker_action_enabled=false" in result.output
    assert "daily_report_generated=false" in result.output
    assert "source_feature_count=10" in result.output
    assert "pit_gate_ready_count=0" in result.output
    assert "contract_ready_count=0" in result.output
    assert "pit_gate_blocked_count=10" in result.output
    assert "blocked_by_valid_until_window_count=1" in result.output
    assert "valid_until_window_dependency_blocker_count_from_2415=1" in result.output
    assert "valid_until_dependency_evidence_row_count=1" in result.output
    assert f"next_route={impl.NEXT_ROUTE}" in result.output
    assert (output_root / "closure_result.json").exists()
    assert (output_root / "valid_until_dependency_evidence.json").exists()


def test_growth_tilt_engine_valid_until_dependency_evidence_closure_docs() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries["growth_tilt_engine_valid_until_dependency_evidence_closure"]

    assert entry["command"] == (
        "aits research strategies "
        "growth-tilt-engine-valid-until-dependency-evidence-closure"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any("closure_result.json" in item for item in entry["artifact_globs"])
    assert any("dynamic_strategy_2419_route.md" in item for item in entry["artifact_globs"])

    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    task_registers = (
        Path("docs/task_register.md").read_text(encoding="utf-8")
        + Path("docs/task_register_completed.md").read_text(encoding="utf-8")
    )
    assert "growth_tilt_engine_valid_until_dependency_evidence_closure" in catalog
    assert "growth-tilt-engine-valid-until-dependency-evidence-closure" in system_flow
    assert impl.TASK_REGISTER_ID in task_registers


def _write_sources(tmp_path: Path) -> dict[str, Path]:
    root = tmp_path / "sources"
    root.mkdir(parents=True, exist_ok=True)
    sources = _source_documents()
    paths = {
        "closure_result_2417": root / "closure_result_2417.json",
        "source_traceability_closure_evidence_2417": (
            root / "source_traceability_closure_evidence_2417.json"
        ),
        "upstream_artifact_closure_evidence_2417": (
            root / "upstream_artifact_closure_evidence_2417.json"
        ),
        "updated_source_feature_mapping_2417": (
            root / "updated_source_feature_mapping_2417.json"
        ),
        "remaining_blocker_summary_2417": (
            root / "remaining_blocker_summary_2417.json"
        ),
        "closure_result_2416": root / "closure_result_2416.json",
        "remaining_blocker_matrix_2416": root / "remaining_blocker_matrix_2416.json",
        "valid_until_dependency_closure_plan_2416": (
            root / "valid_until_dependency_closure_plan_2416.json"
        ),
        "pit_gate_evidence_requirements_2416": (
            root / "pit_gate_evidence_requirements_2416.json"
        ),
        "readiness_snapshot_result_2415": root / "readiness_snapshot_result_2415.json",
        "readiness_matrix_2415": root / "readiness_matrix_2415.json",
        "signal_validity_dependency_remediation_result_2414": (
            root / "signal_validity_dependency_remediation_result_2414.json"
        ),
        "signal_validity_dependency_contract_metadata_2414": (
            root / "signal_validity_dependency_contract_metadata_2414.json"
        ),
        "remaining_blocker_summary_2414": (
            root / "remaining_blocker_summary_2414.json"
        ),
        "remediation_plan_result_2411": root / "remediation_plan_result_2411.json",
        "remediation_plan_result_2407": root / "remediation_plan_result_2407.json",
        "valid_until_semantics_review_2407": (
            root / "valid_until_semantics_review_2407.json"
        ),
        "stale_signal_risk_audit_2407": root / "stale_signal_risk_audit_2407.json",
        "signal_validity_contract_plan_2407": (
            root / "signal_validity_contract_plan_2407.json"
        ),
        "validation_plan_2407": root / "validation_plan_2407.json",
        "pit_input_registry": root / "pit_input_registry.yaml",
        "strategy_execution_policy_registry": (
            root / "strategy_execution_policy_registry.yaml"
        ),
        "report_registry": root / "report_registry.yaml",
        "artifact_catalog": root / "artifact_catalog.md",
    }
    for key, path in paths.items():
        if key in sources:
            _write_json(path, sources[key])
    paths["pit_input_registry"].write_text(_pit_input_registry_yaml(), encoding="utf-8")
    paths["strategy_execution_policy_registry"].write_text(
        _strategy_execution_policy_registry_yaml(),
        encoding="utf-8",
    )
    paths["report_registry"].write_text(_report_registry_yaml(), encoding="utf-8")
    paths["artifact_catalog"].write_text(_artifact_catalog_text(), encoding="utf-8")
    return paths


def _source_documents() -> dict[str, object]:
    return {
        "closure_result_2417": {
            "task_id": "TRADING-2417",
            "status": m2417.READY_STATUS,
            "recommended_next_research_task": m2417.NEXT_ROUTE,
            "source_feature_count": 10,
            "pit_gate_ready_count": 0,
            "contract_ready_count": 0,
            "pit_gate_blocked_count": 10,
            "blocked_by_source_traceability_count": 5,
            "blocked_by_valid_until_window_count": 1,
            "source_traceability_still_blocked_count": 1,
            "valid_until_window_blocking_gap_resolved": False,
            "production_effect": "none",
            "broker_action": "none",
            **{field: False for field in m2417.SAFETY_FALSE_FIELDS},
        },
        "source_traceability_closure_evidence_2417": {
            "task_id": "TRADING-2417",
            "status": m2417.READY_STATUS,
            "source_traceability_closure_evidence": {},
            "production_effect": "none",
            "broker_action": "none",
        },
        "upstream_artifact_closure_evidence_2417": {
            "task_id": "TRADING-2417",
            "status": m2417.READY_STATUS,
            "upstream_artifact_closure_evidence": {},
            "production_effect": "none",
            "broker_action": "none",
        },
        "updated_source_feature_mapping_2417": {
            "task_id": "TRADING-2417",
            "status": m2417.READY_STATUS,
            "updated_source_feature_mapping": {},
            "production_effect": "none",
            "broker_action": "none",
        },
        "remaining_blocker_summary_2417": {
            "task_id": "TRADING-2417",
            "status": m2417.READY_STATUS,
            "remaining_blocker_summary": {
                "source_traceability_still_blocked_feature_ids": [
                    "growth_tilt_engine_signal_artifact"
                ],
                "valid_until_window_blocked_feature_ids": [
                    "execution_signal_validity_policy"
                ],
                "production_effect": "none",
                "broker_action": "none",
            },
            "production_effect": "none",
            "broker_action": "none",
        },
        "closure_result_2416": {
            "task_id": "TRADING-2416",
            "status": m2416.READY_STATUS,
            "recommended_next_research_task": m2416.NEXT_ROUTE,
            "source_feature_count": 10,
            "pit_gate_ready_count": 0,
            "contract_ready_count": 0,
            "pit_gate_blocked_count": 10,
            "blocked_by_source_traceability_count": 5,
            "blocked_by_valid_until_window_count": 1,
            "production_effect": "none",
            "broker_action": "none",
        },
        "remaining_blocker_matrix_2416": _remaining_blocker_matrix_2416(),
        "valid_until_dependency_closure_plan_2416": {
            "task_id": "TRADING-2416",
            "status": m2416.READY_STATUS,
            "valid_until_dependency_closure_plan": {
                "dependent_feature_ids": ["execution_signal_validity_policy"],
                "dependent_feature_or_signal_count": 1,
                "requires_signal_validity_contract_evidence": True,
                "requires_stale_signal_policy_evidence": True,
                "requires_valid_from_valid_until_mapping": True,
                "valid_until_window_still_blocking": True,
                "production_effect": "none",
                "broker_action": "none",
            },
            "production_effect": "none",
            "broker_action": "none",
        },
        "pit_gate_evidence_requirements_2416": {
            "task_id": "TRADING-2416",
            "status": m2416.READY_STATUS,
            "pit_gate_evidence_requirements": {},
            "production_effect": "none",
            "broker_action": "none",
        },
        "readiness_snapshot_result_2415": {
            "task_id": "TRADING-2415",
            "status": m2415.READY_STATUS,
            "source_feature_count": 10,
            "pit_gate_ready_count": 0,
            "contract_ready_count": 0,
            "pit_gate_blocked_count": 10,
            "blocked_by_source_traceability_count": 5,
            "blocked_by_valid_until_window_count": 1,
            "production_effect": "none",
            "broker_action": "none",
        },
        "readiness_matrix_2415": {
            "task_id": "TRADING-2415",
            "status": m2415.READY_STATUS,
            "pit_gate_readiness_matrix": {
                "matrix_rows": [{"source_feature_id": f"feature_{index}"} for index in range(10)],
                "production_effect": "none",
                "broker_action": "none",
            },
            "production_effect": "none",
            "broker_action": "none",
        },
        "signal_validity_dependency_remediation_result_2414": {
            "task_id": "TRADING-2414",
            "status": m2414.READY_STATUS,
            "validity_dependency_blocked_by_valid_until_window_count": 1,
            "valid_until_window_blocker_resolved": False,
            "production_effect": "none",
            "broker_action": "none",
        },
        "signal_validity_dependency_contract_metadata_2414": {
            "task_id": "TRADING-2414",
            "status": m2414.READY_STATUS,
            "signal_validity_dependency_contract_metadata": {
                "metadata_rows": [_metadata_dependency_row()],
                "production_effect": "none",
                "broker_action": "none",
            },
            "production_effect": "none",
            "broker_action": "none",
        },
        "remaining_blocker_summary_2414": {
            "task_id": "TRADING-2414",
            "status": m2414.READY_STATUS,
            "remaining_blocker_summary": {
                "valid_until_window_blocker_resolved": False,
                "valid_until_window_blocker_downgraded": False,
                "production_effect": "none",
                "broker_action": "none",
            },
            "production_effect": "none",
            "broker_action": "none",
        },
        "remediation_plan_result_2411": {
            "task_id": "TRADING-2411",
            "status": m2411.READY_STATUS,
            "valid_until_window_blocker_resolved": False,
            "valid_until_window_blocker_downgraded": False,
            "production_effect": "none",
            "broker_action": "none",
        },
        "remediation_plan_result_2407": _remediation_plan_result_2407(),
        "valid_until_semantics_review_2407": _valid_until_semantics_review_2407(),
        "stale_signal_risk_audit_2407": _stale_signal_risk_audit_2407(),
        "signal_validity_contract_plan_2407": _signal_validity_contract_plan_2407(),
        "validation_plan_2407": _validation_plan_2407(),
    }


def _remaining_blocker_matrix_2416() -> dict[str, object]:
    rows = [
        {
            "feature_id": f"feature_{index}",
            "blocked_by_valid_until_window": False,
            "valid_until_required": False,
            "production_effect": "none",
            "broker_action": "none",
        }
        for index in range(9)
    ]
    rows.append(
        {
            "feature_id": "execution_signal_validity_policy",
            "source_system": "governed_config",
            "blocked_by_valid_until_window": True,
            "valid_until_available": False,
            "valid_until_required": True,
            "validity_dependency_status": "blocked",
            "current_pit_gate_status": "pit_gate_blocked_by_valid_until_window",
            "upstream_artifact_or_registry_reference": (
                "config/research/strategy_execution_policy_registry.yaml:"
                "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1.signal_policy"
            ),
            "required_closure_evidence": [
                "valid_from",
                "valid_until",
                "stale_signal_policy",
                "signal_validity_contract",
            ],
            "production_effect": "none",
            "broker_action": "none",
        }
    )
    return {
        "task_id": "TRADING-2416",
        "status": m2416.READY_STATUS,
        "remaining_blocker_matrix": {
            "matrix_rows": rows,
            "production_effect": "none",
            "broker_action": "none",
        },
        "production_effect": "none",
        "broker_action": "none",
    }


def _metadata_dependency_row() -> dict[str, object]:
    return {
        "source_feature_id": "execution_signal_validity_policy",
        "validity_dependency_id": (
            "growth_tilt_engine:execution_signal_validity_policy:"
            "signal_validity_dependency:v1"
        ),
        "valid_until_required": True,
        "valid_until_available": False,
        "validity_basis": "depends_on_valid_until_window_contract",
        "validity_blocking_reason": "valid_until_window_unresolved",
        "validity_end_reference": "valid_until_window",
        "staleness_policy": "blocked_pending_valid_until_window_contract",
        "expiration_policy": (
            "valid_until_window_required_before_expiration_can_be_evaluated"
        ),
        "production_effect": "none",
        "broker_action": "none",
    }


def _remediation_plan_result_2407() -> dict[str, object]:
    return {
        "task_id": "TRADING-2407",
        "status": m2407.READY_STATUS,
        "valid_until_window_blocking_gap_resolved": False,
        "valid_until_window_severity_downgraded": False,
        "growth_tilt_alignment_review": {
            "alignment_gap_summary": {
                "growth_tilt_signal_horizon": "VALID_UNTIL_WINDOW_RESEARCH_POLICY",
                "valid_until_derivation": "missing per-signal deterministic mapping",
                "high_volatility_shrink_rule": "missing",
                "recovery_conservatism_rule": "missing",
            },
            "alignment_questions": [
                "what growth_tilt horizon should valid_until derive from"
            ],
            "proposed_horizon_to_valid_until_mapping": [
                {
                    "signal_horizon_class": "medium_growth_tilt",
                    "valid_until_rule": "valid_from + medium governed horizon",
                }
            ],
            "proposed_confidence_to_expiry_mapping": [
                {"confidence_band": "MEDIUM", "expiry_policy": "use base horizon"}
            ],
        },
        "production_effect": "none",
        "broker_action": "none",
    }


def _valid_until_semantics_review_2407() -> dict[str, object]:
    return {
        "task_id": "TRADING-2407",
        "status": m2407.READY_STATUS,
        "valid_until_semantics_review": {
            "semantics": [
                {
                    "semantic_id": "valid_until_window",
                    "valid_from_source": "not emitted per signal; policy says next_trading_day",
                    "valid_until_source": "policy window=10 bdays; per-signal field missing",
                    "expiry_rule_source": (
                        "signal_validity_window_bdays exists but natural signal "
                        "expiry is not derived from signal horizon"
                    ),
                    "carry_forward_rule": "hold_previous_actual_position",
                    "signal_to_execution_lag_rule": "execution_lag_bdays=1",
                    "signal_horizon_source": "VALID_UNTIL_WINDOW_RESEARCH_POLICY",
                },
                {
                    "semantic_id": "signal_to_execution_lag",
                    "signal_to_execution_lag_rule": "execution_lag_bdays=1",
                },
                {
                    "semantic_id": "growth_tilt_valid_until_alignment",
                    "signal_horizon_source": "VALID_UNTIL_WINDOW_RESEARCH_POLICY",
                    "valid_until_source": "not derived from growth tilt horizon",
                },
            ],
            "production_effect": "none",
            "broker_action": "none",
        },
        "production_effect": "none",
        "broker_action": "none",
    }


def _stale_signal_risk_audit_2407() -> dict[str, object]:
    risks = [
        ("VALID_UNTIL_UNGROUNDED", "emit valid_until from generated_at"),
        ("CARRY_FORWARD_RISK", "block expired carry-forward"),
        ("SIGNAL_TO_EXECUTION_LAG_RISK", "record lag for every decision"),
        ("NEAR_EXPIRY_OVERTRADING_RISK", "define near-expiry behavior"),
    ]
    return {
        "task_id": "TRADING-2407",
        "status": m2407.READY_STATUS,
        "stale_signal_risk_audit": {
            "risks": [
                {
                    "risk_id": f"VUW-{index}",
                    "category": category,
                    "recommended_fix": fix,
                    "severity": "BLOCKING",
                }
                for index, (category, fix) in enumerate(risks, start=1)
            ],
            "production_effect": "none",
            "broker_action": "none",
        },
        "production_effect": "none",
        "broker_action": "none",
    }


def _signal_validity_contract_plan_2407() -> dict[str, object]:
    return {
        "task_id": "TRADING-2407",
        "status": m2407.READY_STATUS,
        "signal_validity_contract_plan": {
            "contract_plan_ready": True,
            "example_contract_template": {
                "signal_id": "growth_tilt_engine",
                "signal_version": "deterministic_signal_version",
                "as_of_date": "YYYY-MM-DD",
                "generated_at": "YYYY-MM-DDTHH:MM:SSZ",
                "source_data_cutoff": "YYYY-MM-DD",
                "valid_from": "generated_at_or_next_executable_time",
                "valid_until": "valid_from + governed_horizon(max_policy=10)",
                "stale_after": "valid_until_or_earlier_decay_boundary",
                "horizon_days": "TBD_FROM_SIGNAL_HORIZON",
                "expiry_rule": "BLOCK_AFTER_VALID_UNTIL",
            },
            "decision_policy": {
                "current_date > valid_until": "BLOCK_EXECUTION",
                "current_date > stale_after": "BLOCK_OR_DECAY_SIGNAL",
                "near valid_until": "APPLY_NEAR_EXPIRY_DECAY_OR_REQUIRE_REFRESH",
                "missing valid_until": (
                    "BLOCK_CANDIDATE_SEARCH_FOR_DEPENDENT_STRATEGY"
                ),
                "new signal overlaps old": "USE_NEWER_SIGNAL_IF_AS_OF_SAFE_AND_VALID",
            },
            "invariants": [
                "expired_signal_cannot_trigger_new_trade",
                "signal_to_execution_lag_must_be_recorded",
            ],
            "source_policy_context": {
                "signal_validity_window_bdays": 10,
                "execution_lag_bdays": 1,
            },
            "production_effect": "none",
            "broker_action": "none",
        },
        "production_effect": "none",
        "broker_action": "none",
    }


def _validation_plan_2407() -> dict[str, object]:
    return {
        "task_id": "TRADING-2407",
        "status": m2407.READY_STATUS,
        "validation_plan": {
            "stale_replay": [
                "expired signals do not execute",
                "signal-to-execution lag is measured",
                "near-expiry handling is deterministic",
                "carry-forward is logged or blocked",
            ],
            "production_effect": "none",
            "broker_action": "none",
        },
        "production_effect": "none",
        "broker_action": "none",
    }


def _source_kwargs(paths: dict[str, Path]) -> dict[str, Path]:
    return {
        "source_2417_closure_result_path": paths["closure_result_2417"],
        "source_2417_source_traceability_closure_evidence_path": paths[
            "source_traceability_closure_evidence_2417"
        ],
        "source_2417_upstream_artifact_closure_evidence_path": paths[
            "upstream_artifact_closure_evidence_2417"
        ],
        "source_2417_updated_source_feature_mapping_path": paths[
            "updated_source_feature_mapping_2417"
        ],
        "source_2417_remaining_blocker_summary_path": paths[
            "remaining_blocker_summary_2417"
        ],
        "source_2416_closure_result_path": paths["closure_result_2416"],
        "source_2416_remaining_blocker_matrix_path": paths[
            "remaining_blocker_matrix_2416"
        ],
        "source_2416_valid_until_dependency_closure_plan_path": paths[
            "valid_until_dependency_closure_plan_2416"
        ],
        "source_2416_pit_gate_evidence_requirements_path": paths[
            "pit_gate_evidence_requirements_2416"
        ],
        "source_2415_readiness_snapshot_result_path": paths[
            "readiness_snapshot_result_2415"
        ],
        "source_2415_readiness_matrix_path": paths["readiness_matrix_2415"],
        "source_2414_remediation_result_path": paths[
            "signal_validity_dependency_remediation_result_2414"
        ],
        "source_2414_contract_metadata_path": paths[
            "signal_validity_dependency_contract_metadata_2414"
        ],
        "source_2414_remaining_blocker_summary_path": paths[
            "remaining_blocker_summary_2414"
        ],
        "source_2411_remediation_plan_result_path": paths[
            "remediation_plan_result_2411"
        ],
        "source_2407_remediation_plan_result_path": paths[
            "remediation_plan_result_2407"
        ],
        "source_2407_valid_until_semantics_review_path": paths[
            "valid_until_semantics_review_2407"
        ],
        "source_2407_stale_signal_risk_audit_path": paths[
            "stale_signal_risk_audit_2407"
        ],
        "source_2407_signal_validity_contract_plan_path": paths[
            "signal_validity_contract_plan_2407"
        ],
        "source_2407_validation_plan_path": paths["validation_plan_2407"],
        "pit_input_registry_path": paths["pit_input_registry"],
        "strategy_execution_policy_registry_path": paths[
            "strategy_execution_policy_registry"
        ],
        "report_registry_path": paths["report_registry"],
        "artifact_catalog_path": paths["artifact_catalog"],
    }


def _source_args(paths: dict[str, Path]) -> list[str]:
    args: list[str] = []
    option_by_key = {
        "closure_result_2417": "--source-2417-closure-result",
        "source_traceability_closure_evidence_2417": (
            "--source-2417-source-traceability-closure-evidence"
        ),
        "upstream_artifact_closure_evidence_2417": (
            "--source-2417-upstream-artifact-closure-evidence"
        ),
        "updated_source_feature_mapping_2417": (
            "--source-2417-updated-source-feature-mapping"
        ),
        "remaining_blocker_summary_2417": "--source-2417-remaining-blocker-summary",
        "closure_result_2416": "--source-2416-closure-result",
        "remaining_blocker_matrix_2416": "--source-2416-remaining-blocker-matrix",
        "valid_until_dependency_closure_plan_2416": (
            "--source-2416-valid-until-dependency-closure-plan"
        ),
        "pit_gate_evidence_requirements_2416": (
            "--source-2416-pit-gate-evidence-requirements"
        ),
        "readiness_snapshot_result_2415": "--source-2415-readiness-snapshot-result",
        "readiness_matrix_2415": "--source-2415-readiness-matrix",
        "signal_validity_dependency_remediation_result_2414": (
            "--source-2414-remediation-result"
        ),
        "signal_validity_dependency_contract_metadata_2414": (
            "--source-2414-contract-metadata"
        ),
        "remaining_blocker_summary_2414": "--source-2414-remaining-blocker-summary",
        "remediation_plan_result_2411": "--source-2411-remediation-plan-result",
        "remediation_plan_result_2407": "--source-2407-remediation-plan-result",
        "valid_until_semantics_review_2407": (
            "--source-2407-valid-until-semantics-review"
        ),
        "stale_signal_risk_audit_2407": "--source-2407-stale-signal-risk-audit",
        "signal_validity_contract_plan_2407": (
            "--source-2407-signal-validity-contract-plan"
        ),
        "validation_plan_2407": "--source-2407-validation-plan",
        "pit_input_registry": "--pit-input-registry",
        "strategy_execution_policy_registry": "--strategy-execution-policy-registry",
        "report_registry": "--report-registry",
        "artifact_catalog": "--artifact-catalog",
    }
    for key, option in option_by_key.items():
        args.extend([option, str(paths[key])])
    return args


def _pit_input_registry() -> dict[str, object]:
    return {
        "entries": [
            {"input_id": "growth_tilt_engine", "severity": "BLOCKING"},
            {"input_id": "valid_until_window", "severity": "BLOCKING"},
        ]
    }


def _strategy_execution_policy_registry() -> dict[str, object]:
    return {
        "strategy_execution_policies": [
            {
                "strategy_id": closure.TARGET_STRATEGY_ID,
                "signal_policy": {
                    "signal_validity_window_bdays": 10,
                    "signal_effective_earliest": "next_trading_day",
                    "stale_signal_behavior": "hold_previous_actual_position",
                },
                "rebalance_policy": {"execution_lag_bdays": 1},
            }
        ]
    }


def _pit_input_registry_yaml() -> str:
    return "\n".join(
        [
            "entries:",
            "  - input_id: growth_tilt_engine",
            "    severity: BLOCKING",
            "  - input_id: valid_until_window",
            "    severity: BLOCKING",
            "",
        ]
    )


def _strategy_execution_policy_registry_yaml() -> str:
    return "\n".join(
        [
            "strategy_execution_policies:",
            f"  - strategy_id: {closure.TARGET_STRATEGY_ID}",
            "    signal_policy:",
            "      signal_validity_window_bdays: 10",
            "      signal_effective_earliest: next_trading_day",
            "      stale_signal_behavior: hold_previous_actual_position",
            "    rebalance_policy:",
            "      execution_lag_bdays: 1",
            "",
        ]
    )


def _report_registry_yaml() -> str:
    report_ids = [
        "dynamic_strategy_valid_until_window_stale_signal_remediation_plan",
        "growth_tilt_engine_contract_gap_remediation_plan",
        "growth_tilt_engine_signal_validity_dependency_remediation",
        "growth_tilt_engine_pit_gate_readiness_snapshot",
        "growth_tilt_engine_pit_gate_remaining_blocker_closure_plan",
        "growth_tilt_engine_source_traceability_upstream_artifact_closure",
        "growth_tilt_engine_valid_until_dependency_evidence_closure",
    ]
    lines = ["reports:"]
    for report_id in report_ids:
        lines.extend(
            [
                f"  - report_id: {report_id}",
                f"    title: {report_id}",
                "    group: research",
                "    cadence: ad_hoc",
                "    audience: project_owner",
                "    owner: research_governance",
                "    command: aits research strategies "
                "growth-tilt-engine-valid-until-dependency-evidence-closure",
                "    artifact_globs: []",
                "    artifact_selection_policy: latest_available",
                "    freshness_sla_days: 30",
                "    freshness_rationale: test fixture",
                "    owner_action: review",
                "    include_in_reader_brief: false",
                "    include_in_daily_task_dashboard: false",
                "    required_for_daily_reading: false",
                "    production_effect: none",
                "    broker_action: none",
            ]
        )
    return "\n".join(lines) + "\n"


def _artifact_catalog_text() -> str:
    return "\n".join(
        [
            "dynamic-strategy-valid-until-window-stale-signal-remediation-plan",
            "growth-tilt-engine-signal-validity-dependency-remediation",
            "growth-tilt-engine-source-traceability-upstream-artifact-closure",
            "growth-tilt-engine-valid-until-dependency-evidence-closure",
            "",
        ]
    )


def _write_json(path: Path, payload: object) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
