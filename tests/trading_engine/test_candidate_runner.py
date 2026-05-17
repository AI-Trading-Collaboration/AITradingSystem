from __future__ import annotations

import importlib.util
import json
from datetime import UTC, date, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
RUNNER_PATH = REPO_ROOT / "scripts" / "run_paper_trading_from_candidates.py"
_RUNNER_SPEC = importlib.util.spec_from_file_location(
    "run_paper_trading_from_candidates",
    RUNNER_PATH,
)
assert _RUNNER_SPEC is not None
_RUNNER_MODULE = importlib.util.module_from_spec(_RUNNER_SPEC)
assert _RUNNER_SPEC.loader is not None
_RUNNER_SPEC.loader.exec_module(_RUNNER_MODULE)
run_from_candidates = _RUNNER_MODULE.run_from_candidates


def test_run_paper_trading_from_candidates_generates_report_audit_and_summary(
    tmp_path: Path,
) -> None:
    as_of = date(2026, 5, 17)
    candidates_path = _write_candidates(tmp_path, as_of)
    summary_output_path = tmp_path / "outputs" / "paper_trading_summary_2026-05-17.json"

    summary = run_from_candidates(
        as_of=as_of,
        candidates_path=candidates_path,
        audit_root=tmp_path / "audit",
        report_dir=tmp_path / "reports" / "trading_daily",
        summary_output_path=summary_output_path,
    )

    assert summary["candidate_count"] == 2
    assert summary["blocked_candidates"] == 1
    assert summary["status"] == "LIMITED"
    assert summary["generated_intents"] == 1
    assert summary["approved"] == 1
    assert summary["rejected"] == 0
    assert summary["submitted"] == 1
    assert summary["filled"] == 1
    assert summary["open"] == 0
    assert summary["reconciliation_status"] == "PASS"
    assert summary["production_effect"] == "none"
    assert summary["market_snapshot_source"] == "synthetic_limit_price"
    assert summary["market_snapshot_source_counts"] == {
        "historical_ohlc": 0,
        "candidate_metadata": 0,
        "synthetic_limit_price": 1,
    }
    assert Path(summary["report_path"]).exists()
    assert summary_output_path.exists()
    assert (tmp_path / "audit" / "order_intent_log" / "2026-05-17.jsonl").exists()
    summary_json = json.loads(summary_output_path.read_text(encoding="utf-8"))
    assert summary_json["production_effect"] == "none"
    assert summary_json["market_snapshot_source"] == "synthetic_limit_price"
    assert summary_json["candidate_count"] == 2
    assert summary_json["blocked_candidates"] == 1
    assert summary_json["reconciliation_status"] == "PASS"


def test_run_paper_trading_from_candidates_default_generates_limited_upstream_artifacts(
    tmp_path: Path,
) -> None:
    as_of = date(2026, 5, 17)
    reports_dir = tmp_path / "outputs" / "reports"
    candidates_path = reports_dir / "order_intent_candidates_2026-05-17.json"
    summary_output_path = reports_dir / "paper_trading_summary_2026-05-17.json"

    summary = run_from_candidates(
        as_of=as_of,
        candidates_path=candidates_path,
        audit_root=tmp_path / "data" / "trading_engine" / "audit",
        report_dir=tmp_path / "reports" / "trading_daily",
        summary_output_path=summary_output_path,
        project_root=tmp_path,
        ensure_upstream_artifacts=True,
    )

    daily_summary_path = reports_dir / "daily_decision_summary_2026-05-17.json"
    assert daily_summary_path.exists()
    assert candidates_path.exists()
    assert summary_output_path.exists()
    assert (tmp_path / "reports" / "trading_daily" / "2026-05-17.md").exists()
    assert (tmp_path / "data" / "trading_engine" / "audit").exists()

    daily_summary = json.loads(daily_summary_path.read_text(encoding="utf-8"))
    assert daily_summary["production_effect"] == "none"
    assert daily_summary["status"] == "limited"
    assert daily_summary["data_gate"]["status"] == "MISSING"
    assert daily_summary["investment_conclusion"]["availability"] == "missing"
    assert "补造投资动作" in daily_summary["investment_conclusion"]["major_risks"][0]

    candidates = json.loads(candidates_path.read_text(encoding="utf-8"))
    assert candidates["production_effect"] == "none"
    assert candidates["candidate_count"] == 1
    candidate = candidates["candidates"][0]
    assert candidate["blocked"] is True
    assert {
        "manual_approval_required",
        "trading_engine_not_enabled",
        "data_gate_blocked",
    }.issubset(set(candidate["blocked_by"]))

    assert summary["status"] == "LIMITED"
    assert summary["candidate_count"] == 1
    assert summary["blocked_candidates"] == 1
    assert summary["generated_intents"] == 0
    assert summary["production_effect"] == "none"


def _write_candidates(tmp_path: Path, as_of: date) -> Path:
    path = tmp_path / f"order_intent_candidates_{as_of.isoformat()}.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "report_type": "order_intent_candidates",
                "as_of": as_of.isoformat(),
                "run_id": "candidate_runner_test",
                "production_effect": "none",
                "source_inputs": {"daily_decision_summary": {"exists": True}},
                "candidates": [
                    {
                        "schema_version": "1.0",
                        "candidate_id": "candidate_unblocked_tsm",
                        "created_at": datetime(2026, 5, 17, 14, 0, tzinfo=UTC).isoformat(),
                        "strategy_id": "candidate_runner_test",
                        "strategy_version": "v1",
                        "run_id": "candidate_runner_test",
                        "symbol": "TSM",
                        "asset_type": "stock",
                        "side": "BUY",
                        "target_notional_usd": 1000.0,
                        "limit_price": 100.0,
                        "confidence": 0.75,
                        "score_snapshot_id": "score_snapshot_tsm",
                        "blocked": False,
                        "blocked_by": [],
                        "mode": "paper",
                        "production_effect": "none",
                    },
                    {
                        "schema_version": "1.0",
                        "candidate_id": "candidate_blocked_nvda",
                        "strategy_id": "candidate_runner_test",
                        "strategy_version": "v1",
                        "run_id": "candidate_runner_test",
                        "symbol": "NVDA",
                        "asset_type": "stock",
                        "side": "BUY",
                        "target_notional_usd": 1000.0,
                        "limit_price": 900.0,
                        "confidence": 0.75,
                        "score_snapshot_id": "score_snapshot_nvda",
                        "blocked": True,
                        "blocked_by": [
                            "trading_engine_not_enabled",
                            "manual_approval_required",
                        ],
                        "mode": "paper",
                        "production_effect": "none",
                    },
                ],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return path
