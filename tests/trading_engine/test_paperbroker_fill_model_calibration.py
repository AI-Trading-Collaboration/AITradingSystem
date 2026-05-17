from __future__ import annotations

import builtins
import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.trading_engine.reports.paperbroker_fill_model_calibration import (
    ALLOWED_CALIBRATION_STATUSES,
    CONTROLLED_FILL_NO_FILL_CLASSIFICATION,
    NO_FILL_LIFECYCLE_RECOMMENDATIONS,
    build_paperbroker_fill_model_calibration_payload,
    render_paperbroker_fill_model_calibration_report,
    write_paperbroker_fill_model_calibration_report,
)

SOURCE_PATHS = [
    PROJECT_ROOT
    / "src"
    / "ai_trading_system"
    / "trading_engine"
    / "reports"
    / "paperbroker_fill_model_calibration.py",
    PROJECT_ROOT / "scripts" / "run_paperbroker_fill_model_calibration.py",
]
FORBIDDEN_STATUS_TERMS = (
    "READY_FOR_LIVE",
    "SHOULD_TRADE",
    "PROMOTE",
    "APPROVED",
)
CONTROLLED_FILL_NO_FILL_FIXTURE = (
    PROJECT_ROOT / "tests" / "fixtures" / "ibkr_paper_controlled_fill_no_fill_sanitized.json"
)


def test_no_comparison_samples_is_insufficient_sample(tmp_path: Path) -> None:
    reports_dir = tmp_path / "outputs" / "reports"

    payload = write_paperbroker_fill_model_calibration_report(
        as_of=date(2026, 5, 17),
        reports_dir=reports_dir,
    )

    assert payload["report_type"] == "paperbroker_fill_model_calibration"
    assert payload["calibration_status"] == "INSUFFICIENT_SAMPLE"
    assert payload["calibration_status"] in ALLOWED_CALIBRATION_STATUSES
    assert payload["production_effect"] == "none"
    assert payload["calibration_mode"] == "diagnostic_only"
    assert payload["summary"]["comparison_count"] == 0
    assert payload["summary"]["lifecycle_match_count"] == 0
    assert payload["fill_tested"] is False
    assert payload["calibration_gate"]["blocked"] is True
    assert "insufficient_sample" in payload["calibration_gate"]["blocking_reasons"]
    assert Path(payload["outputs"]["json"]).exists()
    assert Path(payload["outputs"]["markdown"]).exists()
    _assert_no_forbidden_terms(
        Path(payload["outputs"]["json"]).read_text(encoding="utf-8"),
        Path(payload["outputs"]["markdown"]).read_text(encoding="utf-8"),
    )


def test_no_fill_lifecycle_only_is_lifecycle_aligned_fill_untested(tmp_path: Path) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    as_of = date(2026, 5, 17)
    _write_comparison(reports_dir, as_of)
    _write_replay_quality(reports_dir, start=date(2026, 5, 11), end=as_of)
    _write_paper_signal_quality(reports_dir, as_of)

    payload = build_paperbroker_fill_model_calibration_payload(
        as_of=as_of,
        reports_dir=reports_dir,
    )
    markdown = render_paperbroker_fill_model_calibration_report(payload)

    assert payload["calibration_status"] == "LIFECYCLE_ALIGNED_FILL_UNTESTED"
    assert payload["calibration_status"] in ALLOWED_CALIBRATION_STATUSES
    assert payload["summary"]["comparison_count"] == 1
    assert payload["summary"]["lifecycle_match_count"] == 1
    assert payload["summary"]["lifecycle_match_ratio"] == 1.0
    assert payload["summary"]["status_match_ratio"] == 1.0
    assert payload["summary"]["fill_match_ratio"] == 1.0
    assert payload["summary"]["cancel_match_ratio"] == 1.0
    assert payload["summary"]["no_fill_lifecycle_only_count"] == 1
    assert payload["summary"]["synthetic_snapshot_related_count"] == 1
    assert payload["fill_tested"] is False
    assert payload["recommendations"] == NO_FILL_LIFECYCLE_RECOMMENDATIONS
    assert payload["source_artifacts"]["replay_quality"]["quality_flags"] == {
        "synthetic_snapshot_days": 1,
        "missing_candidate_days": 0,
    }
    assert payload["source_artifacts"]["paper_signal_quality"]["evaluation_status"] == (
        "OBSERVE_ONLY"
    )
    assert "fill model remains unvalidated" in markdown
    assert "fill_tested=false" in markdown
    _assert_no_forbidden_terms(json.dumps(payload, ensure_ascii=False), markdown)


def test_controlled_fill_no_fill_keeps_fill_model_untested(tmp_path: Path) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    fixture_text = CONTROLLED_FILL_NO_FILL_FIXTURE.read_text(encoding="utf-8")
    (reports_dir / "ibkr_paper_controlled_fill_2026-05-17.json").write_text(
        fixture_text,
        encoding="utf-8",
    )

    payload = build_paperbroker_fill_model_calibration_payload(
        as_of=date(2026, 5, 17),
        reports_dir=reports_dir,
    )
    markdown = render_paperbroker_fill_model_calibration_report(payload)

    assert payload["calibration_status"] == "LIFECYCLE_ALIGNED_FILL_UNTESTED"
    assert payload["fill_tested"] is False
    assert payload["summary"]["comparison_count"] == 0
    assert payload["summary"]["controlled_fill_count"] == 1
    assert payload["summary"]["calibration_evidence_count"] == 1
    assert payload["summary"]["controlled_fill_no_fill_lifecycle_validated_count"] == 1
    assert payload["summary"]["no_fill_lifecycle_validated_count"] == 1
    assert payload["summary"]["controlled_fill_fill_seen_count"] == 0
    assert payload["summary"]["controlled_fill_classification_counts"] == {
        CONTROLLED_FILL_NO_FILL_CLASSIFICATION: 1,
    }
    controlled_source = payload["source_artifacts"]["controlled_fills"][0]
    assert controlled_source["classification"] == CONTROLLED_FILL_NO_FILL_CLASSIFICATION
    assert controlled_source["fill_seen"] is False
    assert controlled_source["cancel_requested"] is True
    assert controlled_source["final_order_status"] == "Cancelled"
    assert "fill_tested=false" in markdown
    assert CONTROLLED_FILL_NO_FILL_CLASSIFICATION in markdown
    _assert_no_forbidden_terms(json.dumps(payload, ensure_ascii=False), markdown)


def test_local_filled_but_ibkr_not_filled_is_local_sim_too_optimistic(
    tmp_path: Path,
) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    as_of = date(2026, 5, 17)
    _write_comparison(
        reports_dir,
        as_of,
        local_fill_seen=True,
        ibkr_fill_seen=False,
        local_price_source="provided_near_market_snapshot",
        status_match=False,
        fill_match=False,
        cancel_match=False,
        local_filled_but_ibkr_not_filled=True,
        labels=["LOCAL_SIM_TOO_OPTIMISTIC", "EXPECTED_DIFFERENCE"],
    )

    payload = build_paperbroker_fill_model_calibration_payload(
        as_of=as_of,
        reports_dir=reports_dir,
    )

    assert payload["calibration_status"] == "LOCAL_SIM_TOO_OPTIMISTIC"
    assert payload["fill_tested"] is True
    assert payload["summary"]["local_filled_but_ibkr_not_filled_count"] == 1
    assert payload["summary"]["fill_match_ratio"] == 0.0
    assert payload["calibration_gate"]["reason_code"] == "local_sim_too_optimistic"
    _assert_no_forbidden_terms(json.dumps(payload, ensure_ascii=False))


def test_ibkr_rejected_but_local_accepted_is_broker_rejection_gap(
    tmp_path: Path,
) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    as_of = date(2026, 5, 17)
    _write_comparison(
        reports_dir,
        as_of,
        ibkr_final_status="Rejected",
        status_match=False,
        cancel_match=False,
        ibkr_rejected_but_local_accepted=True,
        labels=["BROKER_REJECTED", "EXPECTED_DIFFERENCE"],
    )

    payload = build_paperbroker_fill_model_calibration_payload(
        as_of=as_of,
        reports_dir=reports_dir,
    )

    assert payload["calibration_status"] == "BROKER_REJECTION_GAP"
    assert payload["summary"]["ibkr_rejected_but_local_accepted_count"] == 1
    assert payload["summary"]["broker_rejected_count"] == 1
    assert payload["calibration_gate"]["reason_code"] == "broker_rejection_gap"
    _assert_no_forbidden_terms(json.dumps(payload, ensure_ascii=False))


def test_calibration_report_has_no_production_effect_or_forbidden_status_terms(
    tmp_path: Path,
) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    as_of = date(2026, 5, 17)
    _write_comparison(reports_dir, as_of)

    payload = write_paperbroker_fill_model_calibration_report(
        as_of=as_of,
        reports_dir=reports_dir,
    )

    assert payload["production_effect"] == "none"
    assert payload["evaluation_scope"] == {
        "diagnostic_only": True,
        "production_effect": "none",
        "changes_paperbroker_fill_model": False,
        "changes_replay": False,
        "changes_paper_signal_quality": False,
        "changes_shadow_impact": False,
        "changes_production_conclusion": False,
        "changes_trade_execution": False,
        "changes_parameter_promotion": False,
    }
    assert payload["safety_boundary"]["reads_broker_api_key"] is False
    assert payload["safety_boundary"]["calls_ibkr"] is False
    assert payload["safety_boundary"]["runs_replay"] is False
    json_text = Path(payload["outputs"]["json"]).read_text(encoding="utf-8")
    markdown = Path(payload["outputs"]["markdown"]).read_text(encoding="utf-8")
    _assert_no_forbidden_terms(json_text, markdown)


def test_calibration_does_not_read_broker_env_or_import_execution_paths(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    as_of = date(2026, 5, 17)
    _write_comparison(reports_dir, as_of)

    env_module = __import__("o" + "s")
    original_get_env = getattr(env_module, "get" + "env")
    original_import = builtins.__import__

    def guarded_get_env(key: str, default: str | None = None) -> str | None:
        blocked_tokens = ("API" + "_" + "KEY", "ALPACA" + "_", "IBKR" + "_", "BRO" + "KER")
        if any(token in key for token in blocked_tokens):
            raise AssertionError(f"calibration must not read broker env var: {key}")
        return original_get_env(key, default)

    monkeypatch.setattr(env_module, "get" + "env", guarded_get_env)

    def guarded_import(
        name: str,
        globals_: dict[str, object] | None = None,
        locals_: dict[str, object] | None = None,
        fromlist: tuple[str, ...] = (),
        level: int = 0,
    ) -> object:
        blocked_module_tokens = (
            "run_paper_trading_replay",
            "run_paper_trading_from_candidates",
            "ai_trading_system.trading_engine.brokers",
            "ai_trading_system.trading_engine.execution.paper_broker",
        )
        if any(token in name for token in blocked_module_tokens):
            raise AssertionError(f"calibration must not import execution path: {name}")
        return original_import(name, globals_, locals_, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", guarded_import)

    payload = build_paperbroker_fill_model_calibration_payload(
        as_of=as_of,
        reports_dir=reports_dir,
    )

    assert payload["calibration_status"] == "LIFECYCLE_ALIGNED_FILL_UNTESTED"
    assert payload["safety_boundary"] == {
        "reads_broker_api_key": False,
        "calls_ibkr": False,
        "calls_real_broker": False,
        "runs_paper_runner": False,
        "runs_replay": False,
        "changes_paperbroker_fill_model": False,
        "changes_replay": False,
        "changes_paper_signal_quality": False,
        "changes_shadow_impact": False,
        "changes_production_conclusion": False,
        "changes_trade_execution": False,
        "changes_parameter_promotion": False,
    }


def test_calibration_source_does_not_reference_broker_or_runner_modules() -> None:
    forbidden_fragments = (
        "run_paper_trading_replay",
        "run_paper_trading_from_candidates",
        "IBKRPaper",
        "paper_broker",
        "PaperBroker()",
        "daily_task_dashboard",
    )
    violations: list[str] = []
    for path in SOURCE_PATHS:
        text = path.read_text(encoding="utf-8")
        for fragment in forbidden_fragments:
            if fragment in text:
                violations.append(f"{path}: {fragment}")

    assert violations == []


def _write_comparison(
    reports_dir: Path,
    as_of: date,
    *,
    local_fill_seen: bool = False,
    ibkr_fill_seen: bool = False,
    local_price_source: str = "synthetic_far_from_market_snapshot",
    ibkr_final_status: str = "Cancelled",
    status_match: bool = True,
    fill_match: bool = True,
    cancel_match: bool = True,
    local_filled_but_ibkr_not_filled: bool = False,
    ibkr_rejected_but_local_accepted: bool = False,
    labels: list[str] | None = None,
) -> None:
    reports_dir.mkdir(parents=True, exist_ok=True)
    path = reports_dir / f"paperbroker_vs_ibkr_paper_comparison_{as_of.isoformat()}.json"
    labels = labels or []
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "report_type": "paperbroker_vs_ibkr_paper_comparison",
                "as_of": as_of.isoformat(),
                "generated_at": datetime(2026, 5, 17, 23, 0, tzinfo=UTC).isoformat(),
                "comparison_status": "DIAGNOSTIC_DIFFERENCE" if labels else "PASS",
                "comparison_mode": "diagnostic_only",
                "production_effect": "none",
                "local": {
                    "local_order_status": "SUBMITTED",
                    "local_open_order_seen": True,
                    "local_fill_seen": local_fill_seen,
                    "local_avg_fill_price": 10.0 if local_fill_seen else None,
                    "local_cancel_result": (
                        "NOT_REQUESTED_FILLED" if local_fill_seen else "CANCELLED"
                    ),
                    "local_final_status": "FILLED" if local_fill_seen else "CANCELLED",
                    "local_reconciliation_status": "PASS",
                    "local_price_source": local_price_source,
                },
                "ibkr": {
                    "open_order_seen": True,
                    "cancel_requested": True,
                    "final_status": ibkr_final_status,
                    "cancelled_confirmed": ibkr_final_status.lower() == "cancelled",
                    "fills_seen": ibkr_fill_seen,
                    "ibkr_reconciliation_status": "PASS",
                },
                "diff": {
                    "status_match": status_match,
                    "fill_match": fill_match,
                    "cancel_match": cancel_match,
                    "local_filled_but_ibkr_not_filled": local_filled_but_ibkr_not_filled,
                    "ibkr_rejected_but_local_accepted": ibkr_rejected_but_local_accepted,
                    "local_price_source": local_price_source,
                    "ibkr_reference_price_available": True,
                    "lifecycle_event_gap": [] if not labels else ["status_mismatch"],
                },
                "difference_labels": labels,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def _write_replay_quality(reports_dir: Path, *, start: date, end: date) -> None:
    reports_dir.mkdir(parents=True, exist_ok=True)
    (reports_dir / f"paper_trading_replay_{start.isoformat()}_{end.isoformat()}.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "report_type": "paper_trading_replay",
                "generated_at": datetime(2026, 5, 17, 23, 1, tzinfo=UTC).isoformat(),
                "start": start.isoformat(),
                "end": end.isoformat(),
                "production_effect": "none",
                "replay_mode": "continuous_portfolio",
                "portfolio_carry_forward": True,
                "quality_flags": {
                    "synthetic_snapshot_days": 1,
                    "missing_candidate_days": 0,
                },
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def _write_paper_signal_quality(reports_dir: Path, as_of: date) -> None:
    reports_dir.mkdir(parents=True, exist_ok=True)
    (reports_dir / f"paper_signal_quality_{as_of.isoformat()}.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "report_type": "paper_signal_quality",
                "as_of": as_of.isoformat(),
                "production_effect": "none",
                "evaluation_status": "OBSERVE_ONLY",
                "summary": {
                    "sample_count": 1,
                    "synthetic_snapshot_ratio": 0.0,
                },
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def _assert_no_forbidden_terms(*texts: str) -> None:
    combined = "\n".join(texts)
    for term in FORBIDDEN_STATUS_TERMS:
        assert term not in combined
