from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.dynamic_strategy_research_only_shadow_observation_replay_validation import (
    DEFAULT_REPLAY_COUNT,
    NEXT_ROUTE,
    OBSERVATION_MODE,
    READY_STATUS,
    VOLATILE_FIELDS,
    run_dynamic_strategy_research_only_shadow_observation_replay_validation,
)
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)

PRIMARY_CANDIDATE = "dynamic_regime_overlay_v0_4_lower_turnover"
RANKING_TOP = "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1"
SOURCE_2367_READY = (
    "DYNAMIC_STRATEGY_TOP_CANDIDATE_OWNER_REVIEW_AND_SHADOW_RESEARCH_GATE_READY"
)
SOURCE_2368_READY = "DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_PROTOCOL_READY"
SOURCE_2369_READY = "DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_DRY_RUN_READY"
SOURCE_2369_ROUTE = (
    "TRADING-2370_Dynamic_Strategy_Research_Only_Shadow_Observation_"
    "Replay_No_Side_Effect_Validation"
)
OWNER_REVIEW_REQUIRED = "OWNER_REVIEW_REQUIRED"


def test_dynamic_strategy_research_only_shadow_observation_replay_validation_builder(
    tmp_path: Path,
) -> None:
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "replay"
    docs_root = tmp_path / "docs" / "research"

    payload = run_dynamic_strategy_research_only_shadow_observation_replay_validation(
        source_dry_run_result_path=source_paths["dry_run_result"],
        source_dry_run_record_path=source_paths["dry_run_record"],
        source_dry_run_no_side_effect_evidence_path=source_paths[
            "dry_run_no_side_effect_evidence"
        ],
        source_observation_protocol_path=source_paths["observation_protocol"],
        source_owner_review_gate_path=source_paths["owner_review_gate"],
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 5),
    )

    assert payload["status"] == READY_STATUS
    assert payload["source_ready_for_replay_validation"] is True
    assert payload["observation_mode"] == OBSERVATION_MODE
    assert payload["primary_observation_candidate"] == PRIMARY_CANDIDATE
    assert payload["ranking_top_from_2365"] == RANKING_TOP
    assert payload["robustness_top_from_2366"] == PRIMARY_CANDIDATE
    assert payload["execution_cadence"] == "valid_until_window"
    assert payload["replay_count"] == DEFAULT_REPLAY_COUNT
    assert payload["stable_semantic_replay_passed"] is True
    assert payload["stable_semantic_hash_report_ready"] is True
    assert payload["volatile_field_exclusion_applied"] is True
    assert payload["no_side_effect_evidence_ready"] is True
    assert payload["observation_decision"] == OWNER_REVIEW_REQUIRED
    assert payload["owner_review_required"] is True
    assert payload["recommended_next_research_task"] == NEXT_ROUTE
    assert payload["next_route"] == NEXT_ROUTE
    assert payload["data_quality_gate_executed"] is False
    assert payload["data_quality_gate_reason"] == (
        "NOT_APPLICABLE_PRIOR_ARTIFACT_REPLAY_VALIDATION_ONLY_NO_FRESH_MARKET_DATA"
    )

    for key in (
        "paper_shadow_enabled",
        "paper_trade_created",
        "shadow_position_created",
        "scheduler_enabled",
        "event_append_enabled",
        "event_append_attempted",
        "outcome_binding_enabled",
        "outcome_binding_attempted",
        "production_enabled",
        "broker_action_enabled",
        "broker_action_attempted",
        "order_generated",
        "daily_report_generated",
    ):
        assert payload[key] is False
    assert payload["production_effect"] == "none"
    assert payload["broker_action"] == "none"

    hash_report = payload["stable_semantic_hash_report"]
    assert hash_report["replay_count"] == DEFAULT_REPLAY_COUNT
    assert hash_report["unique_hash_count"] == 1
    assert len(hash_report["replay_rows"]) == DEFAULT_REPLAY_COUNT
    assert hash_report["canonical_semantic_payload"]["task_id"] == "TRADING-2370"
    assert hash_report["canonical_semantic_payload"]["status"] == READY_STATUS
    assert hash_report["canonical_semantic_payload"]["source_tasks"] == [
        "TRADING-2365",
        "TRADING-2366",
        "TRADING-2367",
        "TRADING-2368",
        "TRADING-2369",
    ]
    assert hash_report["canonical_semantic_payload"]["observation_mode"] == (
        OBSERVATION_MODE
    )
    assert hash_report["canonical_semantic_payload"][
        "recommended_next_research_task"
    ] == NEXT_ROUTE
    assert not any(
        field in hash_report["canonical_semantic_payload"]
        for field in VOLATILE_FIELDS
    )

    evidence = payload["replay_no_side_effect_evidence"]
    assert evidence["status"] == "PASS"
    assert evidence["no_side_effect_assertions_passed"] is True
    assert evidence["event_append_attempted"] is False
    assert evidence["outcome_binding_attempted"] is False
    assert evidence["outcome_store_mutated"] is False
    assert evidence["paper_trade_created"] is False
    assert evidence["shadow_position_created"] is False
    assert evidence["daily_report_generated"] is False
    assert evidence["broker_action_attempted"] is False

    for key in (
        "json_path",
        "replay_no_side_effect_evidence_json",
        "stable_semantic_hash_report_json",
        "markdown_path",
        "replay_no_side_effect_evidence_markdown",
        "stable_semantic_hash_markdown",
        "next_route_markdown",
    ):
        assert Path(payload["artifact_paths"][key]).exists()


def test_dynamic_strategy_research_only_shadow_observation_replay_validation_cli(
    tmp_path: Path,
) -> None:
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "replay_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "dynamic-strategy-research-only-shadow-observation-replay-validation",
            "--source-dry-run-result",
            str(source_paths["dry_run_result"]),
            "--source-dry-run-record",
            str(source_paths["dry_run_record"]),
            "--source-dry-run-no-side-effect-evidence",
            str(source_paths["dry_run_no_side_effect_evidence"]),
            "--source-observation-protocol",
            str(source_paths["observation_protocol"]),
            "--source-owner-review-gate",
            str(source_paths["owner_review_gate"]),
            "--replay-count",
            "3",
            "--as-of",
            "2026-07-05",
            "--output-root",
            str(output_root),
            "--docs-root",
            str(docs_root),
        ],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )

    assert result.exit_code == 0, result.output
    assert READY_STATUS in result.output
    assert "paper_shadow_allowed=False" in result.output
    assert "broker_action=none" in result.output
    assert (output_root / "replay_validation_result.json").exists()
    assert (output_root / "replay_no_side_effect_evidence.json").exists()
    assert (output_root / "stable_semantic_hash_report.json").exists()

    payload = json.loads(
        (output_root / "replay_validation_result.json").read_text(encoding="utf-8")
    )
    assert payload["observation_mode"] == OBSERVATION_MODE
    assert payload["stable_semantic_replay_passed"] is True
    assert payload["next_route"] == NEXT_ROUTE


def test_dynamic_strategy_research_only_shadow_observation_replay_registry_and_docs() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries[
        "dynamic_strategy_research_only_shadow_observation_replay_validation"
    ]

    assert entry["command"] == (
        "aits research strategies "
        "dynamic-strategy-research-only-shadow-observation-replay-validation"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any(
        "replay_validation_result.json" in item for item in entry["artifact_globs"]
    )
    assert any(
        "stable_semantic_hash_report.json" in item
        for item in entry["artifact_globs"]
    )

    assert "dynamic_strategy_research_only_shadow_observation_replay_validation" in Path(
        "docs/artifact_catalog.md"
    ).read_text(encoding="utf-8")
    assert (
        "dynamic-strategy-research-only-shadow-observation-replay-validation"
        in Path("docs/system_flow.md").read_text(encoding="utf-8")
    )
    assert (
        "TRADING-2370_DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_REPLAY_"
        "NO_SIDE_EFFECT_VALIDATION"
        in Path("docs/task_register.md").read_text(encoding="utf-8")
    )
    assert READY_STATUS in Path(
        "docs/requirements/"
        "TRADING-2370_Dynamic_Strategy_Research_Only_Shadow_Observation_Replay_"
        "No_Side_Effect_Validation.md"
    ).read_text(encoding="utf-8")


def _write_source_artifacts(tmp_path: Path) -> dict[str, Path]:
    source_root = tmp_path / "source"
    source_root.mkdir(parents=True, exist_ok=True)
    documents = {
        "dry_run_result": _dry_run_result(),
        "dry_run_record": {
            "status": SOURCE_2369_READY,
            "observation_dry_run_record": {
                "observation_id": "TRADING-2369_2026-07-05_dynamic",
                "observation_mode": "RESEARCH_ONLY_DRY_RUN",
                "review": {
                    "observation_decision": OWNER_REVIEW_REQUIRED,
                    "owner_review_required": True,
                },
                "guardrails": {
                    "event_append_attempted": False,
                    "broker_action_enabled": False,
                },
            },
        },
        "dry_run_no_side_effect_evidence": {
            "status": SOURCE_2369_READY,
            "no_side_effect_evidence": {
                "status": "PASS",
                "event_append_attempted": False,
                "event_append_performed": False,
                "outcome_binding_attempted": False,
                "outcome_store_mutated": False,
                "paper_shadow_enabled": False,
                "paper_trade_created": False,
                "shadow_position_created": False,
                "scheduler_enabled": False,
                "daily_report_generated": False,
                "production_enabled": False,
                "broker_action_enabled": False,
                "broker_action_attempted": False,
                "order_generated": False,
            },
        },
        "observation_protocol": {
            "status": SOURCE_2368_READY,
            "as_of": "2026-07-05",
            "next_route": (
                "TRADING-2369_Dynamic_Strategy_Research_Only_Shadow_"
                "Observation_Dry_Run"
            ),
            "primary_observation_candidate": PRIMARY_CANDIDATE,
            "research_only_shadow_observation_allowed": True,
        },
        "owner_review_gate": {
            "status": SOURCE_2367_READY,
            "as_of": "2026-07-05",
            "recommended_gate_decision": OWNER_REVIEW_REQUIRED,
            "research_only_shadow_observation_allowed": True,
        },
    }
    paths = {
        "dry_run_result": source_root / "observation_dry_run_result.json",
        "dry_run_record": source_root / "observation_dry_run_record.json",
        "dry_run_no_side_effect_evidence": source_root / "no_side_effect_evidence.json",
        "observation_protocol": source_root / "observation_protocol.json",
        "owner_review_gate": source_root / "owner_review_gate_result.json",
    }
    for key, path in paths.items():
        path.write_text(
            json.dumps(documents[key], ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    return paths


def _dry_run_result() -> dict[str, object]:
    return {
        "task_id": "TRADING-2369",
        "status": SOURCE_2369_READY,
        "as_of": "2026-07-05",
        "next_route": SOURCE_2369_ROUTE,
        "recommended_next_research_task": SOURCE_2369_ROUTE,
        "source_tasks": ["TRADING-2365", "TRADING-2366", "TRADING-2367", "TRADING-2368"],
        "observation_mode": "RESEARCH_ONLY_DRY_RUN",
        "primary_observation_candidate": PRIMARY_CANDIDATE,
        "ranking_top_from_2365": RANKING_TOP,
        "robustness_top_from_2366": PRIMARY_CANDIDATE,
        "execution_cadence": "valid_until_window",
        "observation_protocol_loaded": True,
        "observation_field_schema_loaded": True,
        "review_thresholds_loaded": True,
        "observation_decision": OWNER_REVIEW_REQUIRED,
        "owner_review_required": True,
        "research_only_shadow_observation_allowed": True,
        "requested_date_range": {"start": "2022-12-01", "end": "2026-07-05"},
        "data_quality": {
            "status": "PASS_WITH_WARNINGS",
            "error_count": 0,
            "warning_count": 1,
        },
        "paper_shadow_enabled": False,
        "paper_trade_created": False,
        "shadow_position_created": False,
        "event_append_enabled": False,
        "event_append_attempted": False,
        "outcome_binding_enabled": False,
        "outcome_binding_attempted": False,
        "scheduler_enabled": False,
        "production_enabled": False,
        "broker_action_enabled": False,
        "broker_action_attempted": False,
        "order_generated": False,
        "daily_report_generated": False,
        "generated_at": "2026-07-05T00:00:00Z",
        "runtime_artifact_path": "/tmp/volatile/runtime.json",
        "duration_ms": 123,
    }
