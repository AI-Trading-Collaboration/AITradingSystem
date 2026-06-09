from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

import pytest
import yaml
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.etf_portfolio import baseline_review
from ai_trading_system.etf_portfolio.baseline_review import (
    BASELINE_REVIEW_SAFETY,
    DEFAULT_BASELINE_REVIEW_POLICY_CONFIG_PATH,
    BaselineReviewError,
    build_baseline_change_proposal_draft,
    build_baseline_review_eligibility,
    build_baseline_review_evidence_matrix,
    build_baseline_review_package,
    build_baseline_review_validation_report,
    build_candidate_review_outcome,
    build_owner_review_decision,
    link_baseline_review_decision_to_journal,
    load_baseline_review_policy_config,
    write_baseline_review_package,
    write_owner_review_decision,
)
from ai_trading_system.reports import reader_brief

RUN_DATE = date(2026, 6, 3)
GENERATED_AT = datetime(2026, 6, 3, 12, 0, tzinfo=UTC)
CANDIDATE_ID = "candidate_weight_set_003"


def test_baseline_review_policy_config_loads_and_requires_safety(tmp_path: Path) -> None:
    config = load_baseline_review_policy_config()

    assert config.safety.model_dump(mode="json") == BASELINE_REVIEW_SAFETY
    assert set(config.required_evidence) >= set(baseline_review.REQUIRED_EVIDENCE_IDS)
    assert config.eligibility_thresholds.minimum_forward_days == 20
    assert "EVIDENCE_DASHBOARD_BLOCKED" in config.blocking_conditions

    raw = yaml.safe_load(DEFAULT_BASELINE_REVIEW_POLICY_CONFIG_PATH.read_text(encoding="utf-8"))
    raw["safety"]["production_effect"] = "mutate_config"
    unsafe_path = tmp_path / "unsafe.yaml"
    unsafe_path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")
    with pytest.raises(BaselineReviewError):
        load_baseline_review_policy_config(unsafe_path)

    raw = yaml.safe_load(DEFAULT_BASELINE_REVIEW_POLICY_CONFIG_PATH.read_text(encoding="utf-8"))
    raw["blocking_conditions"]["EVIDENCE_DASHBOARD_BLOCKED"]["blocks_review"] = False
    invalid_path = tmp_path / "invalid_blocker.yaml"
    invalid_path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")
    with pytest.raises(BaselineReviewError):
        load_baseline_review_policy_config(invalid_path)


def test_baseline_review_eligibility_passes_and_fails_closed(tmp_path: Path) -> None:
    report_index = baseline_review._sample_report_index(tmp_path, RUN_DATE)

    eligible = build_baseline_review_eligibility(
        candidate_id=CANDIDATE_ID,
        as_of=RUN_DATE,
        report_index=report_index,
        root_path=tmp_path,
        generated_at=GENERATED_AT,
    )
    assert eligible["eligibility_status"] == "eligible_for_owner_review"
    assert not eligible["blockers"]
    assert eligible["manual_review_required"] is True
    assert eligible["safety"] == BASELINE_REVIEW_SAFETY

    _patch_source_payload(
        report_index,
        "etf_data_quality_governance_report",
        {"status": "FAIL", "data_quality_status": "FAIL"},
    )
    blocked = build_baseline_review_eligibility(
        candidate_id=CANDIDATE_ID,
        as_of=RUN_DATE,
        report_index=report_index,
        root_path=tmp_path,
        generated_at=GENERATED_AT,
    )
    assert blocked["eligibility_status"] == "blocked"
    assert {"EVIDENCE_DASHBOARD_BLOCKED", "DATA_QUALITY_CRITICAL"} <= {
        item["blocker_id"] for item in blocked["blockers"]
    }


def test_baseline_review_forward_sample_and_safety_blockers(tmp_path: Path) -> None:
    report_index = baseline_review._sample_report_index(tmp_path, RUN_DATE)
    _patch_candidate(
        report_index,
        "etf_forward_dashboard",
        {"forward_days": 5, "sample_count": 5},
    )
    needs_more = build_baseline_review_eligibility(
        candidate_id=CANDIDATE_ID,
        as_of=RUN_DATE,
        report_index=report_index,
        root_path=tmp_path,
        generated_at=GENERATED_AT,
    )
    assert needs_more["eligibility_status"] == "needs_more_data"
    assert "FORWARD_SAMPLE_TOO_SMALL" in {item["blocker_id"] for item in needs_more["blockers"]}

    report_index = baseline_review._sample_report_index(tmp_path / "unsafe", RUN_DATE)
    _patch_source_payload(
        report_index,
        "etf_forward_dashboard",
        {"production_effect": "overwrite_config"},
    )
    unsafe = build_baseline_review_eligibility(
        candidate_id=CANDIDATE_ID,
        as_of=RUN_DATE,
        report_index=report_index,
        root_path=tmp_path,
        generated_at=GENERATED_AT,
    )
    assert unsafe["eligibility_status"] == "blocked"
    assert "UNSAFE_PRODUCTION_EFFECT" in {item["blocker_id"] for item in unsafe["blockers"]}

    report_index = baseline_review._sample_report_index(tmp_path / "unsupported", RUN_DATE)
    _patch_all_candidates(report_index, {"candidate_type": "unsupported_candidate"})
    unsupported = build_baseline_review_eligibility(
        candidate_id=CANDIDATE_ID,
        as_of=RUN_DATE,
        report_index=report_index,
        root_path=tmp_path,
        generated_at=GENERATED_AT,
    )
    assert unsupported["eligibility_status"] == "blocked"
    assert "UNSUPPORTED_CANDIDATE_TYPE" in {item["blocker_id"] for item in unsupported["blockers"]}


def test_baseline_review_does_not_label_missing_forward_drawdown_as_high_drawdown(
    tmp_path: Path,
) -> None:
    report_index = baseline_review._sample_report_index(tmp_path, RUN_DATE)
    _patch_source_payload(report_index, "etf_forward_dashboard", {"sample_count": 0})
    _patch_candidate(
        report_index,
        "etf_forward_dashboard",
        {"forward_days": 0, "sample_count": 0},
    )

    blocked = build_baseline_review_eligibility(
        candidate_id=CANDIDATE_ID,
        as_of=RUN_DATE,
        report_index=report_index,
        root_path=tmp_path,
        generated_at=GENERATED_AT,
    )
    blocker_ids = {item["blocker_id"] for item in blocked["blockers"]}
    assert "FORWARD_SAMPLE_TOO_SMALL" in blocker_ids
    assert "HIGH_DRAWDOWN" not in blocker_ids


def test_baseline_review_reads_markdown_json_sidecar_source_payloads(tmp_path: Path) -> None:
    report_index = baseline_review._sample_report_index(tmp_path, RUN_DATE)
    sidecar_candidate = "candidate_weight_set_sidecar"
    forward_path = _path_for_report(report_index, "etf_forward_dashboard")
    payload = json.loads(forward_path.read_text(encoding="utf-8"))
    payload["candidates"] = [
        {
            "candidate_id": sidecar_candidate,
            "weight_set_id": sidecar_candidate,
            "candidate_type": "weight_calibration_candidate",
            "status": "supportive",
            "forward_days": 25,
            "sample_count": 25,
            "turnover_delta": 0.05,
            "drawdown_delta": 0.01,
            "weights": {"SPY": 0.25, "QQQ": 0.35, "SMH": 0.25, "CASH": 0.15},
        }
    ]
    forward_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    markdown_path = forward_path.with_suffix(".md")
    markdown_path.write_text("# Forward dashboard\n", encoding="utf-8")
    _point_report_to_path(report_index, "etf_forward_dashboard", markdown_path)

    eligibility = build_baseline_review_eligibility(
        candidate_id=sidecar_candidate,
        as_of=RUN_DATE,
        report_index=report_index,
        root_path=tmp_path,
        generated_at=GENERATED_AT,
    )

    assert eligibility["candidate_context"]["candidate_exists"] is True
    assert "CANDIDATE_NOT_FOUND" not in {item["blocker_id"] for item in eligibility["blockers"]}
    assert str(markdown_path) in eligibility["candidate_context"]["source_paths"]


def test_baseline_review_normalizes_weight_calibration_proposal_types(tmp_path: Path) -> None:
    report_index = baseline_review._sample_report_index(tmp_path, RUN_DATE)
    proposal_candidate = "weight_set_proposal_only"
    calibration_path = _path_for_report(report_index, "etf_weight_dual_track_calibration_report")
    payload = json.loads(calibration_path.read_text(encoding="utf-8"))
    payload["proposals"] = [
        {
            "weight_set_id": proposal_candidate,
            "proposal_type": "defer_until_more_forward_data",
            "status": "needs_more_forward_data",
            "production_effect": "none",
            "broker_action": "none",
            "manual_review_required": True,
            "weights": {"SPY": 0.25, "QQQ": 0.35, "SMH": 0.25, "CASH": 0.15},
        }
    ]
    calibration_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    eligibility = build_baseline_review_eligibility(
        candidate_id=proposal_candidate,
        as_of=RUN_DATE,
        report_index=report_index,
        root_path=tmp_path,
        generated_at=GENERATED_AT,
    )

    assert eligibility["candidate_context"]["candidate_type"] == "weight_calibration_candidate"
    assert eligibility["candidate_type"] == "weight_calibration_candidate"
    assert "UNSUPPORTED_CANDIDATE_TYPE" not in {
        item["blocker_id"] for item in eligibility["blockers"]
    }


def test_evidence_matrix_and_review_package_preserve_source_links(tmp_path: Path) -> None:
    report_index = baseline_review._sample_report_index(tmp_path, RUN_DATE)

    matrix = build_baseline_review_evidence_matrix(
        candidate_id=CANDIDATE_ID,
        as_of=RUN_DATE,
        report_index=report_index,
        root_path=tmp_path,
    )
    assert len(matrix.rows) == len(baseline_review.REQUIRED_EVIDENCE_IDS)
    assert all(row.source_report for row in matrix.rows)
    assert {row.evidence_id for row in matrix.rows} >= {"forward_performance", "data_quality"}

    package = build_baseline_review_package(
        candidate_id=CANDIDATE_ID,
        as_of=RUN_DATE,
        report_index=report_index,
        root_path=tmp_path,
        generated_at=GENERATED_AT,
    )
    assert package["report_type"] == "etf_baseline_review_package"
    assert package["safety_banner"] == BASELINE_REVIEW_SAFETY
    assert package["evidence_requirement_matrix"]["rows"]
    assert package["recommended_decision_options"]
    assert package["source_report_links"]

    paths = write_baseline_review_package(
        package,
        json_path=tmp_path / "package.json",
        markdown_path=tmp_path / "package.md",
    )
    assert paths["json"].exists()
    assert "Safety Banner" in paths["markdown"].read_text(encoding="utf-8")


def test_owner_decision_journal_link_and_proposal_preconditions(tmp_path: Path) -> None:
    report_index = baseline_review._sample_report_index(tmp_path, RUN_DATE)
    package = build_baseline_review_package(
        candidate_id=CANDIDATE_ID,
        as_of=RUN_DATE,
        report_index=report_index,
        root_path=tmp_path,
        generated_at=GENERATED_AT,
    )
    package_path = tmp_path / "package.json"
    package_path.write_text(json.dumps(package, ensure_ascii=False, indent=2), encoding="utf-8")

    with pytest.raises(BaselineReviewError):
        build_owner_review_decision(
            review_package=package,
            owner_decision="approve_for_proposal_draft",
            rationale="",
            confidence=0.8,
            created_at=GENERATED_AT,
        )
    with pytest.raises(BaselineReviewError):
        build_owner_review_decision(
            review_package=package,
            owner_decision="place_order",
            rationale="unsafe",
            confidence=0.8,
            created_at=GENERATED_AT,
        )

    decision = build_owner_review_decision(
        review_package=package,
        owner_decision="approve_for_proposal_draft",
        rationale="Evidence is sufficient for proposal draft only.",
        confidence=0.82,
        conditions=["Owner rechecks source reports."],
        follow_up_tasks=["Keep forward shadow running."],
        created_at=GENERATED_AT,
    )
    linked = link_baseline_review_decision_to_journal(
        decision,
        review_package_path=package_path,
        journal_path=tmp_path / "journal.json",
        updated_at=GENERATED_AT,
    )
    assert linked["decision_journal_linkage"]["status"] == "linked"
    assert (tmp_path / "journal.json").exists()

    proposal = build_baseline_change_proposal_draft(
        review_package=package,
        owner_decision=linked,
        created_at=GENERATED_AT,
    )
    assert proposal["proposal_is_draft_only"] is True
    assert proposal["baseline_config_mutated"] is False
    assert proposal["target_weights_mutated"] is False

    continue_decision = build_owner_review_decision(
        review_package=package,
        owner_decision="continue_shadow",
        rationale="Continue observation.",
        confidence=0.6,
        created_at=GENERATED_AT,
    )
    with pytest.raises(BaselineReviewError):
        build_baseline_change_proposal_draft(
            review_package=package,
            owner_decision=continue_decision,
            created_at=GENERATED_AT,
        )


def test_outcome_tracker_and_reader_brief_summary(tmp_path: Path) -> None:
    report_index = baseline_review._sample_report_index(tmp_path, RUN_DATE)
    package = build_baseline_review_package(
        candidate_id=CANDIDATE_ID,
        as_of=RUN_DATE,
        report_index=report_index,
        root_path=tmp_path,
        generated_at=GENERATED_AT,
    )
    package_path = tmp_path / "baseline_review_package_2026-06-03.json"
    package_path.write_text(json.dumps(package, ensure_ascii=False, indent=2), encoding="utf-8")
    decision = build_owner_review_decision(
        review_package=package,
        owner_decision="continue_shadow",
        rationale="Continue observation.",
        confidence=0.7,
        created_at=GENERATED_AT,
    )
    decision_path = tmp_path / "decision.json"
    write_owner_review_decision(
        decision,
        json_path=decision_path,
        markdown_path=tmp_path / "decision.md",
    )
    outcome = build_candidate_review_outcome(
        candidate_id=CANDIDATE_ID,
        decision=decision,
        created_at=GENERATED_AT,
    )
    assert outcome["latest_review_status"] == "continue_shadow"
    assert outcome["next_review_due"] == "2026-06-17"
    outcome_path = tmp_path / "outcome.json"
    outcome_path.write_text(json.dumps(outcome, ensure_ascii=False, indent=2), encoding="utf-8")

    summary = reader_brief._etf_baseline_review_summary(
        {
            "reports": [
                _report_record("etf_baseline_review_package", package_path),
                _report_record("etf_baseline_review_decision", decision_path),
                _report_record("etf_baseline_review_outcome", outcome_path),
            ]
        }
    )
    assert summary["availability"] == "AVAILABLE"
    assert summary["eligible_count"] == 1
    assert summary["latest_owner_decision"] == "continue_shadow"
    assert summary["latest_outcome_status"] == "continue_shadow"
    assert summary["safety_status"].startswith("observe_only=true")

    missing = reader_brief._etf_baseline_review_summary({"reports": []})
    assert missing["availability"] == "MISSING"


def test_baseline_review_validation_and_cli(tmp_path: Path) -> None:
    validation = build_baseline_review_validation_report(
        as_of=RUN_DATE,
        report_registry_path=Path("config/report_registry.yaml"),
        generated_at=GENERATED_AT,
    )
    assert validation["status"] == "PASS"

    report_index = baseline_review._sample_report_index(tmp_path, RUN_DATE)
    report_index_path = tmp_path / "report_index.json"
    report_index_path.write_text(
        json.dumps(report_index, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "etf",
            "baseline-review",
            "package",
            "--candidate",
            CANDIDATE_ID,
            "--as-of",
            RUN_DATE.isoformat(),
            "--report-index-path",
            str(report_index_path),
            "--output-dir",
            str(tmp_path / "packages"),
        ],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )
    assert result.exit_code == 0, result.output
    assert (tmp_path / "packages" / "baseline_review_package_2026-06-03.json").exists()

    validate_result = runner.invoke(
        app,
        [
            "etf",
            "baseline-review",
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


def _patch_source_payload(
    report_index: dict[str, object],
    report_id: str,
    updates: dict[str, object],
) -> None:
    path = _path_for_report(report_index, report_id)
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload.update(updates)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _patch_candidate(
    report_index: dict[str, object],
    report_id: str,
    updates: dict[str, object],
) -> None:
    path = _path_for_report(report_index, report_id)
    payload = json.loads(path.read_text(encoding="utf-8"))
    for candidate in payload["candidates"]:
        if candidate["candidate_id"] == CANDIDATE_ID:
            candidate.update(updates)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _patch_all_candidates(
    report_index: dict[str, object],
    updates: dict[str, object],
) -> None:
    for record in report_index["reports"]:  # type: ignore[index]
        path = Path(record["latest_artifact_path"])
        payload = json.loads(path.read_text(encoding="utf-8"))
        if "candidates" not in payload:
            continue
        for candidate in payload["candidates"]:
            if candidate["candidate_id"] == CANDIDATE_ID:
                candidate.update(updates)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _path_for_report(report_index: dict[str, object], report_id: str) -> Path:
    for record in report_index["reports"]:  # type: ignore[index]
        if record["report_id"] == report_id:
            return Path(record["latest_artifact_path"])
    raise AssertionError(f"missing report_id: {report_id}")


def _point_report_to_path(report_index: dict[str, object], report_id: str, path: Path) -> None:
    for record in report_index["reports"]:  # type: ignore[index]
        if record["report_id"] == report_id:
            record["latest_artifact_path"] = str(path)
            record["latest_artifact_name"] = path.name
            return
    raise AssertionError(f"missing report_id: {report_id}")


def _report_record(report_id: str, path: Path) -> dict[str, object]:
    return {
        "report_id": report_id,
        "latest_artifact_path": str(path),
        "freshness_status": "FRESH",
        "artifact_status": "PASS",
        "exists": True,
    }
