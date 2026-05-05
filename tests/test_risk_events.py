from __future__ import annotations

from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.config import (
    RiskEventRuleConfig,
    RiskEventsConfig,
    load_industry_chain,
    load_risk_events,
    load_universe,
    load_watchlist,
)
from ai_trading_system.risk_events import (
    build_risk_event_occurrence_review_report,
    build_risk_event_review_attestation,
    load_risk_event_occurrence_store,
    render_risk_event_occurrence_review_report,
    render_risk_events_validation_report,
    validate_risk_event_occurrence_store,
    validate_risk_events_config,
    write_risk_event_occurrence_review_report,
    write_risk_event_review_attestation,
    write_risk_events_validation_report,
)


def test_validate_risk_events_config_passes_default_config() -> None:
    report = validate_risk_events_config(
        risk_events=load_risk_events(),
        industry_chain=load_industry_chain(),
        watchlist=load_watchlist(),
        universe=load_universe(),
        as_of=date(2026, 5, 2),
    )

    assert report.status == "PASS"
    assert report.error_count == 0
    assert report.active_rule_count >= 3


def test_validate_risk_events_config_rejects_unknown_node() -> None:
    config = load_risk_events()
    first_rule = config.event_rules[0]
    broken_rule = first_rule.model_copy(update={"affected_nodes": ["unknown_node"]})
    broken = RiskEventsConfig(
        levels=config.levels,
        event_rules=[broken_rule, *config.event_rules[1:]],
    )

    report = validate_risk_events_config(
        risk_events=broken,
        industry_chain=load_industry_chain(),
        watchlist=load_watchlist(),
        universe=load_universe(),
        as_of=date(2026, 5, 2),
    )

    assert report.passed is False
    assert "unknown_affected_node" in {issue.code for issue in report.issues}


def test_validate_risk_events_config_rejects_unknown_ticker() -> None:
    config = load_risk_events()
    first_rule = config.event_rules[0]
    broken_rule = first_rule.model_copy(update={"related_tickers": ["UNKNOWN"]})
    broken = RiskEventsConfig(
        levels=config.levels,
        event_rules=[broken_rule, *config.event_rules[1:]],
    )

    report = validate_risk_events_config(
        risk_events=broken,
        industry_chain=load_industry_chain(),
        watchlist=load_watchlist(),
        universe=load_universe(),
        as_of=date(2026, 5, 2),
    )

    assert report.passed is False
    assert "unknown_related_ticker" in {issue.code for issue in report.issues}


def test_validate_risk_events_config_warns_on_missing_escalation_conditions() -> None:
    config = load_risk_events()
    broken_rule = RiskEventRuleConfig(
        event_id="test_l2_without_escalation",
        name="测试风险",
        level="L2",
        description="测试风险事件",
        affected_nodes=["export_controls"],
        related_tickers=["NVDA"],
        trigger_examples=["测试触发"],
        recommended_actions=["人工复核"],
        escalation_conditions=[],
        deescalation_conditions=[],
        active=True,
    )
    broken = RiskEventsConfig(levels=config.levels, event_rules=[broken_rule])

    report = validate_risk_events_config(
        risk_events=broken,
        industry_chain=load_industry_chain(),
        watchlist=load_watchlist(),
        universe=load_universe(),
        as_of=date(2026, 5, 2),
    )

    assert report.passed is True
    assert report.warning_count == 2
    assert "missing_escalation_conditions" in {issue.code for issue in report.issues}
    assert "missing_deescalation_conditions" in {issue.code for issue in report.issues}


def test_render_and_write_risk_events_report(tmp_path: Path) -> None:
    report = validate_risk_events_config(
        risk_events=load_risk_events(),
        industry_chain=load_industry_chain(),
        watchlist=load_watchlist(),
        universe=load_universe(),
        as_of=date(2026, 5, 2),
    )

    markdown = render_risk_events_validation_report(report)
    output_path = write_risk_events_validation_report(report, tmp_path / "risk_events.md")

    assert "- 状态：PASS" in markdown
    assert "ai_chip_export_control_upgrade" in markdown
    assert output_path.read_text(encoding="utf-8") == markdown


def test_validate_risk_event_occurrence_store_passes_active_manual_record(
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "occurrence.yaml"
    _write_risk_event_occurrence(input_path)

    validation_report = validate_risk_event_occurrence_store(
        store=load_risk_event_occurrence_store(input_path),
        risk_events=load_risk_events(),
        as_of=date(2026, 5, 2),
    )
    review_report = build_risk_event_occurrence_review_report(validation_report)

    assert validation_report.status == "PASS"
    assert validation_report.occurrence_count == 1
    assert review_report.status == "PASS_WITH_WARNINGS"
    assert len(review_report.score_eligible_active_items) == 1
    assert len(review_report.position_gate_eligible_active_items) == 1
    assert review_report.score_eligible_active_items[0].event_id == (
        "ai_chip_export_control_upgrade"
    )


def test_b_grade_risk_event_scores_but_does_not_trigger_position_gate(
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "occurrence.yaml"
    _write_risk_event_occurrence(
        input_path,
        evidence_grade="B",
        action_class="position_gate_eligible",
    )

    validation_report = validate_risk_event_occurrence_store(
        store=load_risk_event_occurrence_store(input_path),
        risk_events=load_risk_events(),
        as_of=date(2026, 5, 2),
    )
    review_report = build_risk_event_occurrence_review_report(validation_report)

    assert validation_report.passed is True
    assert "b_grade_risk_event_not_position_gate_eligible" in {
        issue.code for issue in validation_report.issues
    }
    assert len(review_report.score_eligible_active_items) == 1
    assert review_report.position_gate_eligible_active_items == ()
    assert review_report.items[0].health.endswith("_SCORE_ONLY")


def test_watch_risk_event_occurrence_requires_review_not_scoring(
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "occurrence.yaml"
    _write_risk_event_occurrence(
        input_path,
        status="watch",
        action_class="position_gate_eligible",
    )

    validation_report = validate_risk_event_occurrence_store(
        store=load_risk_event_occurrence_store(input_path),
        risk_events=load_risk_events(),
        as_of=date(2026, 5, 2),
    )
    review_report = build_risk_event_occurrence_review_report(validation_report)

    assert validation_report.passed is True
    assert "watch_risk_event_not_auto_scored" in {
        issue.code for issue in validation_report.issues
    }
    assert review_report.score_eligible_active_items == ()
    assert review_report.items[0].health == "WATCH"
    assert review_report.items[0].score_eligible is False


def test_risk_event_lifecycle_fields_and_expiry_are_audited(tmp_path: Path) -> None:
    input_path = tmp_path / "occurrence.yaml"
    _write_risk_event_occurrence(
        input_path,
        extra_fields="""
lifecycle_state: confirmed_high
dedup_group: export_control_2026_may
primary_channel: risk_gate
used_in_alpha: false
used_in_gate: true
decay_half_life_days: 30
expiry_time: 2026-05-01
resolution_reason: ""
""",
    )

    validation_report = validate_risk_event_occurrence_store(
        store=load_risk_event_occurrence_store(input_path),
        risk_events=load_risk_events(),
        as_of=date(2026, 5, 2),
    )
    review_report = build_risk_event_occurrence_review_report(validation_report)
    markdown = render_risk_event_occurrence_review_report(review_report)
    item = review_report.items[0]

    assert "risk_event_occurrence_expired" in {
        issue.code for issue in validation_report.issues
    }
    assert item.lifecycle_state == "expired"
    assert item.dedup_group == "export_control_2026_may"
    assert item.used_in_gate is True
    assert item.score_eligible is False
    assert item.position_gate_eligible is False
    assert item.health == "EXPIRED"
    assert "Dedup group" in markdown
    assert "export_control_2026_may" in markdown


def test_low_grade_risk_event_occurrence_never_auto_scores(
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "occurrence.yaml"
    _write_risk_event_occurrence(
        input_path,
        evidence_grade="D",
        action_class="position_gate_eligible",
    )

    validation_report = validate_risk_event_occurrence_store(
        store=load_risk_event_occurrence_store(input_path),
        risk_events=load_risk_events(),
        as_of=date(2026, 5, 2),
    )
    review_report = build_risk_event_occurrence_review_report(validation_report)

    assert validation_report.passed is True
    assert "low_grade_risk_event_not_auto_scored" in {
        issue.code for issue in validation_report.issues
    }
    assert review_report.score_eligible_active_items == ()
    assert review_report.items[0].health == "LOW_EVIDENCE_GRADE"
    assert review_report.items[0].score_eligible is False


def test_public_convenience_risk_event_occurrence_is_not_scoreable(
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "occurrence.yaml"
    _write_risk_event_occurrence(
        input_path,
        source_type="public_convenience",
        source_url="https://example.test/public-risk-note",
    )

    validation_report = validate_risk_event_occurrence_store(
        store=load_risk_event_occurrence_store(input_path),
        risk_events=load_risk_events(),
        as_of=date(2026, 5, 2),
    )
    review_report = build_risk_event_occurrence_review_report(validation_report)

    assert validation_report.passed is True
    assert "public_convenience_risk_event_source" in {
        issue.code for issue in validation_report.issues
    }
    assert review_report.score_eligible_active_items == ()
    assert review_report.items[0].health == "INELIGIBLE_SOURCE"
    assert review_report.items[0].score_eligible is False


def test_active_risk_event_occurrence_requires_human_review_metadata(
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "occurrence.yaml"
    input_path.write_text(
        """
occurrence_id: ai_chip_export_control_upgrade_2026_05_01
event_id: ai_chip_export_control_upgrade
status: active
triggered_at: 2026-05-01
last_confirmed_at: 2026-05-02
evidence_grade: A
severity: high
probability: confirmed
scope: ai_bucket
time_sensitivity: high
reversibility: partly_reversible
action_class: position_gate_eligible
evidence_sources:
  - source_name: manual_policy_review
    source_type: manual_input
    captured_at: 2026-05-02
summary: 缺少人工复核元数据的测试风险事件。
""",
        encoding="utf-8",
    )

    validation_report = validate_risk_event_occurrence_store(
        store=load_risk_event_occurrence_store(input_path),
        risk_events=load_risk_events(),
        as_of=date(2026, 5, 2),
    )

    assert validation_report.passed is False
    assert "risk_event_occurrence_missing_review_metadata" in {
        issue.code for issue in validation_report.issues
    }


def test_validate_risk_event_occurrence_store_rejects_unknown_event_id(
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "occurrence.yaml"
    _write_risk_event_occurrence(input_path, event_id="unknown_policy_event")

    validation_report = validate_risk_event_occurrence_store(
        store=load_risk_event_occurrence_store(input_path),
        risk_events=load_risk_events(),
        as_of=date(2026, 5, 2),
    )

    assert validation_report.passed is False
    assert "unknown_risk_event_id" in {issue.code for issue in validation_report.issues}


def test_current_review_attestation_allows_empty_occurrence_store(
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "occurrences"
    attestation = build_risk_event_review_attestation(
        as_of=date(2026, 5, 2),
        reviewer="policy_owner",
        rationale="人工复核官方来源和预审队列，未发现未记录重大风险事件。",
        checked_source_names=("official_policy_sources", "openai_prereview_queue"),
        next_review_due=date(2026, 5, 3),
    )
    write_risk_event_review_attestation(attestation, input_path)

    validation_report = validate_risk_event_occurrence_store(
        store=load_risk_event_occurrence_store(input_path),
        risk_events=load_risk_events(),
        as_of=date(2026, 5, 2),
    )
    review_report = build_risk_event_occurrence_review_report(validation_report)
    markdown = render_risk_event_occurrence_review_report(review_report)

    assert validation_report.status == "PASS"
    assert validation_report.occurrence_count == 0
    assert validation_report.review_attestation_count == 1
    assert validation_report.current_review_attestation_count == 1
    assert review_report.status == "PASS"
    assert "当前有效复核声明数：1" in markdown
    assert "确认无未记录重大事件" in markdown


def test_stale_review_attestation_does_not_remove_empty_store_warning(
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "occurrences"
    attestation = build_risk_event_review_attestation(
        as_of=date(2026, 5, 1),
        reviewer="policy_owner",
        rationale="前一日复核声明。",
        checked_source_names=("official_policy_sources",),
        next_review_due=date(2026, 5, 1),
    )
    write_risk_event_review_attestation(attestation, input_path)

    validation_report = validate_risk_event_occurrence_store(
        store=load_risk_event_occurrence_store(input_path),
        risk_events=load_risk_events(),
        as_of=date(2026, 5, 2),
    )

    assert validation_report.passed is True
    assert validation_report.current_review_attestation_count == 0
    assert {
        "risk_event_current_review_attestation_missing",
        "risk_event_review_attestation_stale",
    }.issubset({issue.code for issue in validation_report.issues})


def test_render_and_write_risk_event_occurrence_report(tmp_path: Path) -> None:
    input_path = tmp_path / "occurrence.yaml"
    _write_risk_event_occurrence(input_path)
    validation_report = validate_risk_event_occurrence_store(
        store=load_risk_event_occurrence_store(input_path),
        risk_events=load_risk_events(),
        as_of=date(2026, 5, 2),
    )
    review_report = build_risk_event_occurrence_review_report(validation_report)

    markdown = render_risk_event_occurrence_review_report(review_report)
    output_path = write_risk_event_occurrence_review_report(
        review_report,
        tmp_path / "risk_event_occurrences.md",
    )

    assert "- 状态：PASS_WITH_WARNINGS" in markdown
    assert "ai_chip_export_control_upgrade" in markdown
    assert output_path.read_text(encoding="utf-8") == markdown


def test_risk_events_cli_validate_and_list(tmp_path: Path) -> None:
    output_path = tmp_path / "risk_events_validation.md"

    validate_result = CliRunner().invoke(
        app,
        [
            "risk-events",
            "validate",
            "--as-of",
            "2026-05-02",
            "--output-path",
            str(output_path),
        ],
    )
    list_result = CliRunner().invoke(app, ["risk-events", "list"])

    assert validate_result.exit_code == 0
    assert list_result.exit_code == 0
    assert output_path.exists()
    assert "风险事件校验状态：PASS" in validate_result.output
    assert "风险事件规则" in list_result.output
    assert "L2" in list_result.output


def test_risk_events_cli_validate_occurrences(tmp_path: Path) -> None:
    input_path = tmp_path / "occurrences"
    input_path.mkdir()
    _write_risk_event_occurrence(input_path / "occurrence.yaml")
    output_path = tmp_path / "risk_event_occurrences.md"

    validate_result = CliRunner().invoke(
        app,
        [
            "risk-events",
            "validate-occurrences",
            "--input-path",
            str(input_path),
            "--as-of",
            "2026-05-02",
            "--output-path",
            str(output_path),
        ],
    )
    list_result = CliRunner().invoke(
        app,
        [
            "risk-events",
            "list-occurrences",
            "--input-path",
            str(input_path),
        ],
    )

    assert validate_result.exit_code == 0
    assert list_result.exit_code == 0
    assert output_path.exists()
    assert "风险事件发生记录状态：PASS_WITH_WARNINGS" in validate_result.output
    assert "风险事件发生记录" in list_result.output


def test_risk_events_cli_record_review_attestation(tmp_path: Path) -> None:
    input_path = tmp_path / "occurrences"
    output_path = tmp_path / "risk_event_occurrences.md"

    result = CliRunner().invoke(
        app,
        [
            "risk-events",
            "record-review-attestation",
            "--output-dir",
            str(input_path),
            "--as-of",
            "2026-05-02",
            "--reviewer",
            "policy_owner",
            "--rationale",
            "人工复核官方来源和预审队列，未发现未记录重大风险事件。",
            "--checked-sources",
            "official_policy_sources,openai_prereview_queue",
            "--output-path",
            str(output_path),
        ],
    )

    assert result.exit_code == 0
    assert "风险事件复核声明" in result.output
    assert "当前有效：1" in result.output
    assert output_path.exists()
    assert len(list(input_path.glob("*.yaml"))) == 1


def _write_risk_event_occurrence(
    output_path: Path,
    event_id: str = "ai_chip_export_control_upgrade",
    status: str = "active",
    source_type: str = "manual_input",
    source_url: str = "",
    evidence_grade: str = "A",
    action_class: str = "position_gate_eligible",
    extra_fields: str = "",
) -> None:
    output_path.write_text(
        f"""
occurrence_id: {event_id}_2026_05_01
event_id: {event_id}
status: {status}
triggered_at: 2026-05-01
last_confirmed_at: 2026-05-02
evidence_grade: {evidence_grade}
severity: high
probability: confirmed
scope: ai_bucket
time_sensitivity: high
reversibility: partly_reversible
action_class: {action_class}
{extra_fields.strip()}
reviewer: policy_owner
reviewed_at: 2026-05-02
review_decision: confirmed_active
rationale: 一手来源和人工复核均确认该测试风险事件。
next_review_due: 2026-05-09
evidence_sources:
  - source_name: manual_policy_review
    source_type: {source_type}
    source_url: "{source_url}"
    published_at: 2026-05-01
    captured_at: 2026-05-02
summary: 人工确认的测试风险事件。
""",
        encoding="utf-8",
    )
