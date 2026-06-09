from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pytest
import yaml
from pydantic import ValidationError
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.etf_portfolio.strategy_evidence_dashboard import (
    DEFAULT_STRATEGY_EVIDENCE_CONFIG_PATH,
    STRATEGY_EVIDENCE_DASHBOARD_SCHEMA_VERSION,
    STRATEGY_EVIDENCE_REGISTRY_SCHEMA_VERSION,
    STRATEGY_EVIDENCE_SAFETY,
    StrategyCandidateEvidenceRanking,
    StrategyEvidenceCard,
    StrategyEvidenceDashboard,
    StrategyEvidenceDashboardError,
    StrategyEvidenceItem,
    StrategyEvidenceSafety,
    StrategyManualReviewPriority,
    build_candidate_evidence_rankings,
    build_evidence_conflicts,
    build_manual_review_priorities,
    build_strategy_evidence_aggregation,
    build_strategy_evidence_cards,
    build_strategy_evidence_dashboard,
    build_strategy_evidence_validation_report,
    load_strategy_evidence_dashboard_config,
)
from ai_trading_system.reports import reader_brief

RUN_DATE = date(2026, 6, 3)
GENERATED_AT = datetime(2026, 6, 3, 12, 0, tzinfo=UTC)


def test_strategy_evidence_config_loads_default_registry() -> None:
    config = load_strategy_evidence_dashboard_config()

    assert config.schema_version == STRATEGY_EVIDENCE_REGISTRY_SCHEMA_VERSION
    assert config.safety.model_dump(mode="json") == STRATEGY_EVIDENCE_SAFETY
    assert "forward_dashboard" in config.sources
    assert set(config.categories) >= {
        "baseline_allocation",
        "weight_calibration",
        "forward_simulation",
        "ai_attribution",
        "satellite_attribution",
        "data_quality",
        "operations_health",
        "validation_gates",
    }
    assert all(source.report_id for source in config.sources.values() if source.required)
    assert all(source.max_age_days >= 0 for source in config.sources.values())


def test_strategy_evidence_config_rejects_unsafe_production_effect(tmp_path: Path) -> None:
    raw = yaml.safe_load(DEFAULT_STRATEGY_EVIDENCE_CONFIG_PATH.read_text(encoding="utf-8"))
    raw["safety"]["production_effect"] = "mutate_weights"
    config_path = tmp_path / "unsafe_evidence_dashboard.yaml"
    config_path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")

    with pytest.raises(StrategyEvidenceDashboardError):
        load_strategy_evidence_dashboard_config(config_path)


def test_strategy_evidence_schema_validates_complete_dashboard(tmp_path: Path) -> None:
    config = load_strategy_evidence_dashboard_config()
    report_index = _complete_report_index(tmp_path, config)

    dashboard = build_strategy_evidence_dashboard(
        as_of=RUN_DATE,
        config=config,
        report_index=report_index,
        generated_at=GENERATED_AT,
    )

    payload = dashboard.model_dump(mode="json")
    restored = StrategyEvidenceDashboard.model_validate(payload)
    assert restored.schema_version == STRATEGY_EVIDENCE_DASHBOARD_SCHEMA_VERSION
    assert restored.safety.model_dump(mode="json") == STRATEGY_EVIDENCE_SAFETY
    assert restored.commands_executed is False
    assert restored.production_state_mutated is False
    assert restored.evidence_cards
    assert all(card.source_report_paths for card in restored.evidence_cards)
    assert json.dumps(payload, ensure_ascii=False, sort_keys=True)


def test_strategy_evidence_schema_rejects_missing_safety_and_traceability() -> None:
    source_path = "reports/source.json"
    item = _evidence_item("baseline_allocation", source_path)
    valid_card = _card(
        "baseline_allocation",
        "supportive",
        source_paths=[source_path],
        supporting=[item],
    )
    payload = {
        "dashboard_id": "dashboard:test",
        "as_of_date": RUN_DATE,
        "generated_at": GENERATED_AT,
        "model_version": "test",
        "config_hash": "test",
        "overall_status": "supportive",
        "evidence_cards": [valid_card.model_dump(mode="json")],
        "safety": STRATEGY_EVIDENCE_SAFETY,
    }

    unsafe_payload = dict(payload)
    unsafe_payload.pop("safety")
    with pytest.raises(ValidationError):
        StrategyEvidenceDashboard.model_validate(unsafe_payload)

    with pytest.raises(ValidationError):
        StrategyEvidenceSafety.model_validate(
            {**STRATEGY_EVIDENCE_SAFETY, "production_effect": "apply_weight_change"}
        )

    with pytest.raises(ValidationError):
        StrategyEvidenceCard(
            card_id="bad:missing_source",
            category="baseline_allocation",
            title="Bad",
            status="supportive",
            confidence="high",
            summary="bad",
            supporting_evidence=[item],
            blocking_evidence=[],
            metrics={},
            sample_count=1,
            freshness_status="FRESH",
            data_quality_status="PASS",
            validation_status="PASS",
            source_report_paths=[],
            manual_review_action="continue_observation",
        )

    invalid_status = valid_card.model_dump(mode="json")
    invalid_status["status"] = "good"
    with pytest.raises(ValidationError):
        StrategyEvidenceCard.model_validate(invalid_status)

    with pytest.raises(ValidationError):
        StrategyManualReviewPriority(
            priority_id="unsafe",
            priority_level="critical",
            source_component="baseline_allocation",
            issue="unsafe",
            recommended_review_action="place_order",
            evidence_links=["reports/source.json"],
            created_at=GENERATED_AT,
        )


def test_strategy_evidence_aggregation_tracks_loaded_missing_stale_and_blocked(
    tmp_path: Path,
) -> None:
    config = load_strategy_evidence_dashboard_config()
    report_index = _complete_report_index(tmp_path, config)
    reports = {item["report_id"]: item for item in report_index["reports"]}
    reports["etf_ai_attribution_report"]["exists"] = False
    reports["etf_ai_attribution_report"]["latest_artifact_path"] = ""
    reports["etf_forward_dashboard"]["freshness_status"] = "STALE"
    reports["etf_forward_dashboard"]["age_days"] = 99
    reports["etf_data_quality_governance_report"]["artifact_status"] = "FAIL"

    payload = build_strategy_evidence_aggregation(
        as_of=RUN_DATE,
        config=config,
        report_index=report_index,
        generated_at=GENERATED_AT,
    )

    assert "etf_baseline_brief" in payload["loaded_sources"]
    assert "ai_attribution_report" in payload["missing_sources"]
    assert "forward_dashboard" in payload["stale_sources"]
    assert "data_quality_governance_report" in payload["blocked_sources"]
    assert payload["safety"] == STRATEGY_EVIDENCE_SAFETY
    assert all(payload["source_report_paths"])


def test_strategy_evidence_cards_include_required_categories_and_quality_context(
    tmp_path: Path,
) -> None:
    config = load_strategy_evidence_dashboard_config()
    report_index = _complete_report_index(tmp_path, config)
    aggregation = build_strategy_evidence_aggregation(
        as_of=RUN_DATE,
        config=config,
        report_index=report_index,
        generated_at=GENERATED_AT,
    )

    cards = build_strategy_evidence_cards(aggregation, config=config)
    by_category = {card.category: card for card in cards}

    for category in (
        "baseline_allocation",
        "ai_attribution",
        "satellite_attribution",
        "data_quality",
        "operations_health",
    ):
        card = by_category[category]
        assert card.source_report_paths
        assert card.data_quality_status
        assert card.freshness_status
        assert card.validation_status

    reports = {item["report_id"]: item for item in report_index["reports"]}
    reports["etf_data_quality_governance_report"]["artifact_status"] = "FAIL"
    cards = build_strategy_evidence_cards(
        build_strategy_evidence_aggregation(
            as_of=RUN_DATE,
            config=config,
            report_index=report_index,
            generated_at=GENERATED_AT,
        ),
        config=config,
    )
    assert {card.category: card for card in cards}["data_quality"].status == "blocked"


def test_candidate_evidence_ranking_is_deterministic_and_quality_aware() -> None:
    forward = _card("forward_simulation", "strong_support", sample_count=12)
    journal = _card("decision_journal", "supportive", sample_count=4)
    blocked_ai = _card(
        "ai_confirmation",
        "supportive",
        data_quality_status="FAIL",
        sample_count=8,
    )
    weak_sample = _card("parameter_review", "supportive", sample_count=0)

    first = build_candidate_evidence_rankings([forward, journal, blocked_ai, weak_sample])
    second = build_candidate_evidence_rankings([forward, journal, blocked_ai, weak_sample])

    assert [item.model_dump(mode="json") for item in first] == [
        item.model_dump(mode="json") for item in second
    ]
    by_candidate = {item.candidate_id: item for item in first}
    assert by_candidate["forward_simulation:aggregate"].rank == 1
    assert by_candidate["ai_confirmation:aggregate"].status == "blocked"
    assert by_candidate["ai_confirmation:aggregate"].evidence_score == 0
    assert by_candidate["parameter_review:aggregate"].status == "needs_more_data"
    assert all(
        item.manual_review_priority != "critical" for item in first if item.status != "blocked"
    )


def test_conflicts_and_manual_review_priorities_are_generated() -> None:
    cards = [
        _card("weight_calibration", "strong_support", source_paths=["reports/weight.json"]),
        _card(
            "forward_simulation",
            "needs_more_data",
            sample_count=0,
            source_paths=["reports/forward.json"],
        ),
        _card("ai_attribution", "supportive", source_paths=["reports/ai_attr.json"]),
        _card(
            "data_quality",
            "blocked",
            data_quality_status="FAIL",
            source_paths=["reports/data_quality.json"],
        ),
        _card("satellite_replacement", "supportive", source_paths=["reports/satellite.json"]),
        _card(
            "satellite_attribution",
            "mixed",
            source_paths=["reports/satellite_attr.json"],
        ),
        _card("parameter_review", "supportive", source_paths=["reports/parameter.json"]),
        _card("decision_journal", "mixed", source_paths=["reports/journal.json"]),
        _card("operations_health", "supportive", source_paths=["reports/ops.json"]),
        _card("validation_gates", "stale", source_paths=["reports/validation.json"]),
    ]

    conflicts = build_evidence_conflicts(cards)
    rankings = [
        StrategyCandidateEvidenceRanking(
            candidate_id="forward_simulation:aggregate",
            candidate_type="forward_shadow_candidate",
            rank=1,
            evidence_score=35,
            status="needs_more_data",
            supporting_evidence=cards[1].supporting_evidence,
            blocking_evidence=cards[1].blocking_evidence,
            manual_review_priority="medium",
        )
    ]
    priorities = build_manual_review_priorities(
        cards=cards,
        conflicts=conflicts,
        rankings=rankings,
        generated_at=GENERATED_AT,
    )

    assert {
        "historical_backtest_strong_forward_weak",
        "ai_attribution_support_data_quality_blocked",
        "satellite_candidate_positive_attribution_negative",
        "parameter_proposal_journal_rejected_or_deferred",
        "operations_health_pass_validation_gate_stale",
    } <= {conflict.conflict_type for conflict in conflicts}
    assert any(item.priority_level == "critical" for item in priorities)
    assert any(item.recommended_review_action == "request_more_data" for item in priorities)
    assert all(
        item.recommended_review_action
        not in {"place_order", "promote_to_production", "change_production_weights"}
        for item in priorities
    )


def test_strategy_evidence_report_and_validation_cli_generate_json_and_markdown(
    tmp_path: Path,
) -> None:
    config = load_strategy_evidence_dashboard_config()
    report_index = _complete_report_index(tmp_path, config)
    report_index_path = tmp_path / "report_index_2026-06-03.json"
    report_index_path.write_text(json.dumps(report_index, indent=2), encoding="utf-8")

    runner = CliRunner()
    report_result = runner.invoke(
        app,
        [
            "etf",
            "evidence-dashboard",
            "report",
            "--as-of",
            RUN_DATE.isoformat(),
            "--report-index-path",
            str(report_index_path),
            "--output-dir",
            str(tmp_path / "dashboard"),
        ],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )

    assert report_result.exit_code == 0, report_result.output
    dashboard_json = tmp_path / "dashboard" / "strategy_evidence_dashboard_2026-06-03.json"
    dashboard_md = tmp_path / "dashboard" / "strategy_evidence_dashboard_2026-06-03.md"
    assert dashboard_json.exists()
    assert dashboard_md.exists()
    payload = json.loads(dashboard_json.read_text(encoding="utf-8"))
    assert payload["safety"] == STRATEGY_EVIDENCE_SAFETY
    assert payload["evidence_cards"]
    assert payload["candidate_rankings"]
    assert "broker_action" not in dashboard_md.read_text(encoding="utf-8").lower().replace(
        "broker_action | none",
        "",
    )

    validate_result = runner.invoke(
        app,
        [
            "etf",
            "evidence-dashboard",
            "validate",
            "--as-of",
            RUN_DATE.isoformat(),
            "--output-dir",
            str(tmp_path / "validation"),
        ],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )
    assert validate_result.exit_code == 0, validate_result.output
    validation_json = tmp_path / "validation" / "strategy_evidence_validation_2026-06-03.json"
    validation_md = tmp_path / "validation" / "strategy_evidence_validation_2026-06-03.md"
    validation = json.loads(validation_json.read_text(encoding="utf-8"))
    assert validation["status"] == "PASS"
    assert validation_md.exists()


def test_strategy_evidence_validation_fails_closed_for_bad_registry(tmp_path: Path) -> None:
    raw = yaml.safe_load(DEFAULT_STRATEGY_EVIDENCE_CONFIG_PATH.read_text(encoding="utf-8"))
    raw["safety"]["broker_action"] = "place_order"
    config_path = tmp_path / "bad_registry.yaml"
    config_path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")

    payload = build_strategy_evidence_validation_report(
        as_of=RUN_DATE,
        config_path=config_path,
        generated_at=GENERATED_AT,
    )

    assert payload["status"] == "FAIL"
    assert any(check["check_id"] == "source_registry_valid" for check in payload["checks"])


def test_reader_brief_strategy_evidence_section_summarizes_latest_report(
    tmp_path: Path,
) -> None:
    config = load_strategy_evidence_dashboard_config()
    report_index = _complete_report_index(tmp_path, config)
    dashboard = build_strategy_evidence_dashboard(
        as_of=RUN_DATE,
        config=config,
        report_index=report_index,
        generated_at=GENERATED_AT,
    )
    dashboard_path = tmp_path / "strategy_evidence_dashboard_2026-06-03.json"
    dashboard_path.write_text(
        json.dumps(dashboard.model_dump(mode="json"), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    summary = reader_brief._etf_strategy_evidence_summary(
        {
            "reports": [
                {
                    "report_id": "etf_strategy_evidence_dashboard",
                    "exists": True,
                    "latest_artifact_path": str(dashboard_path),
                    "freshness_status": "FRESH",
                    "artifact_status": "PASS",
                }
            ]
        }
    )

    assert summary["availability"] == "AVAILABLE"
    assert summary["overall_status"] == dashboard.overall_status
    assert summary["manual_review_priority_count"] == len(dashboard.manual_review_priorities)
    assert summary["data_quality_status"] == dashboard.data_quality_overlay["status"]
    assert summary["detail_report"].endswith("strategy_evidence_dashboard_2026-06-03.json")
    assert summary["safety_status"] == (
        "observe_only=true; candidate_only=true; production_effect=none; "
        "broker_action=none; manual_review_required=true; commands_executed=false; "
        "production_state_mutated=false"
    )

    missing = reader_brief._etf_strategy_evidence_summary({"reports": []})
    assert missing["availability"] == "MISSING"
    assert missing["detail_report"] == ""


def _complete_report_index(
    tmp_path: Path,
    config: Any,
) -> dict[str, Any]:
    records: dict[str, dict[str, Any]] = {}
    for source_id, source in sorted(config.sources.items()):
        path = tmp_path / f"{source.report_id}_{RUN_DATE.isoformat()}.json"
        payload = _source_payload(source_id, source.category)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        records[source.report_id] = _report_record(source.report_id, path)
        if source.validation_report_id and source.validation_report_id not in records:
            validation_path = (
                tmp_path / f"{source.validation_report_id}_{RUN_DATE.isoformat()}.json"
            )
            validation_path.write_text(
                json.dumps(
                    {
                        "report_type": source.validation_report_id,
                        "status": "PASS",
                        "data_quality_status": "PASS",
                        "production_effect": "none",
                        "broker_action": "none",
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            records[source.validation_report_id] = _report_record(
                source.validation_report_id,
                validation_path,
            )
    return {"reports": list(records.values())}


def _report_record(report_id: str, path: Path) -> dict[str, Any]:
    return {
        "report_id": report_id,
        "latest_artifact_path": str(path),
        "latest_artifact_name": path.name,
        "artifact_date": RUN_DATE.isoformat(),
        "freshness_status": "FRESH",
        "artifact_status": "PASS",
        "exists": True,
        "age_days": 0,
    }


def _source_payload(source_id: str, category: str) -> dict[str, Any]:
    status_by_category = {
        "baseline_allocation": "supportive",
        "weight_calibration": "strong_support",
        "forward_simulation": "needs_more_data",
        "ai_confirmation": "supportive",
        "ai_attribution": "supportive",
        "satellite_replacement": "supportive",
        "satellite_attribution": "mixed",
        "parameter_review": "supportive",
        "weekly_review": "supportive",
        "decision_journal": "supportive",
        "data_quality": "PASS",
        "operations_health": "PASS",
        "validation_gates": "PASS",
    }
    sample_by_category = {
        "forward_simulation": 2,
        "ai_attribution": 4,
        "satellite_attribution": 3,
        "validation_gates": 1,
    }
    return {
        "report_type": source_id,
        "status": status_by_category.get(category, "supportive"),
        "overall_status": status_by_category.get(category, "supportive"),
        "evidence_status": status_by_category.get(category, "supportive"),
        "sample_count": sample_by_category.get(category, 6),
        "data_quality_status": "PASS",
        "validation_status": "PASS",
        "production_effect": "none",
        "broker_action": "none",
        "manual_review_required": True,
        "safety": STRATEGY_EVIDENCE_SAFETY,
        "summary": f"{source_id} fixture",
    }


def _evidence_item(category: str, source_path: str) -> StrategyEvidenceItem:
    return StrategyEvidenceItem(
        evidence_id=f"{category}:fixture",
        source_module=f"fixture.{category}",
        source_report_path=source_path,
        source_metric="status",
        as_of_date=RUN_DATE,
        freshness_status="FRESH",
        data_quality_status="PASS",
        sample_count_if_applicable=4,
        summary=f"{category} fixture evidence",
        value="supportive",
    )


def _card(
    category: str,
    status: str,
    *,
    source_paths: list[str] | None = None,
    supporting: list[StrategyEvidenceItem] | None = None,
    data_quality_status: str = "PASS",
    freshness_status: str = "FRESH",
    validation_status: str = "PASS",
    sample_count: int = 4,
) -> StrategyEvidenceCard:
    paths = source_paths or [f"reports/{category}.json"]
    support = supporting or [_evidence_item(category, paths[0])]
    return StrategyEvidenceCard(
        card_id=f"{category}:card",
        category=category,  # type: ignore[arg-type]
        title=category.replace("_", " ").title(),
        status=status,  # type: ignore[arg-type]
        confidence="high" if status in {"supportive", "strong_support"} else "medium",
        summary=f"{category} status={status}",
        supporting_evidence=support,
        blocking_evidence=[],
        metrics={"sample_count": sample_count},
        sample_count=sample_count,
        freshness_status=freshness_status,
        data_quality_status=data_quality_status,
        validation_status=validation_status,
        source_report_paths=paths,
        manual_review_action="continue_observation",
    )
