from __future__ import annotations

import inspect
import json
from datetime import date, datetime
from pathlib import Path

import ai_trading_system.order_intent_candidates as module
from ai_trading_system.order_intent_candidates import (
    build_order_intent_candidates_payload,
    write_order_intent_candidates_json,
)


def test_order_intent_candidates_are_blocked_only_and_no_trading_engine(
    tmp_path: Path,
) -> None:
    as_of = date(2026, 5, 4)
    daily_summary_path = _write_daily_decision_summary(tmp_path, as_of)
    snapshot_path = _write_decision_snapshot(tmp_path, as_of)

    payload = build_order_intent_candidates_payload(
        as_of=as_of,
        daily_decision_summary_path=daily_summary_path,
        decision_snapshot_path=snapshot_path,
        project_root=tmp_path,
        generated_at=datetime.fromisoformat("2026-05-05T01:00:00+00:00"),
    )

    assert payload["report_type"] == "order_intent_candidates"
    assert payload["production_effect"] == "none"
    assert set(payload) == {
        "schema_version",
        "report_type",
        "as_of",
        "generated_at",
        "run_id",
        "production_effect",
        "status",
        "execution_boundary",
        "source_inputs",
        "source_artifacts",
        "candidate_count",
        "candidates",
    }
    assert payload["execution_boundary"] == {
        "creates_order_intent": False,
        "creates_execution_action": False,
        "broker_api_allowed": False,
        "paper_broker_allowed": False,
        "account_state_required": False,
        "trading_engine_connected": False,
    }
    assert payload["candidate_count"] == 1
    candidate = payload["candidates"][0]
    assert set(candidate) == {
        "schema_version",
        "candidate_id",
        "strategy_id",
        "strategy_version",
        "run_id",
        "mode",
        "candidate_type",
        "candidate_action",
        "candidate_action_label",
        "scope",
        "statement",
        "blocked",
        "blocked_by",
        "production_effect",
        "execution_action",
        "would_create_order_intent",
        "would_submit_order",
        "manual_approval_required",
        "trading_engine_connected",
        "account_state_dependency",
        "score_snapshot_id",
        "confidence",
        "reason_codes",
        "metadata",
        "source_decision",
        "score_snapshot",
        "position_context",
        "non_execution_policy",
    }
    assert candidate["blocked"] is True
    assert {"trading_engine_not_enabled", "manual_approval_required"}.issubset(
        set(candidate["blocked_by"])
    )
    assert candidate["candidate_action"] == "HOLD_OR_OBSERVE_BLOCKED"
    assert candidate["execution_action"] == "none"
    assert candidate["would_create_order_intent"] is False
    assert candidate["would_submit_order"] is False
    assert candidate["account_state_dependency"] is False
    assert candidate["mode"] == "paper"
    assert candidate["production_effect"] == "none"
    assert candidate["score_snapshot_id"] == "decision_snapshot:2026-05-04"
    assert candidate["score_snapshot"]["snapshot_id"] == "decision_snapshot:2026-05-04"
    assert candidate["position_context"]["binding_position_gates"][0]["gate_id"] == (
        "valuation"
    )
    assert candidate["non_execution_policy"] == {
        "no_symbol_level_order": True,
        "no_quantity": True,
        "no_notional": True,
        "no_limit_price": True,
        "no_broker_route": True,
    }
    candidate_text = json.dumps(candidate, ensure_ascii=False)
    assert '"target_quantity"' not in candidate_text
    assert '"target_notional_usd"' not in candidate_text
    assert '"limit_price"' not in candidate_text
    assert "OrderIntent" not in candidate_text
    assert "from ai_trading_system.trading_engine" not in inspect.getsource(module)
    assert payload["source_artifacts"][0]["id"] == "daily_decision_summary"
    assert payload["source_artifacts"][1]["id"] == "decision_snapshot"


def test_order_intent_candidates_missing_snapshot_stays_limited_and_blocked(
    tmp_path: Path,
) -> None:
    as_of = date(2026, 5, 4)
    daily_summary_path = _write_daily_decision_summary(tmp_path, as_of)
    missing_snapshot_path = tmp_path / "missing_snapshot.json"

    output_path = tmp_path / "order_intent_candidates_2026-05-04.json"
    write_order_intent_candidates_json(
        as_of=as_of,
        daily_decision_summary_path=daily_summary_path,
        decision_snapshot_path=missing_snapshot_path,
        output_path=output_path,
        project_root=tmp_path,
    )
    payload = json.loads(output_path.read_text(encoding="utf-8"))

    candidate = payload["candidates"][0]
    assert candidate["blocked"] is True
    assert "score_snapshot_missing" in candidate["blocked_by"]
    assert {"trading_engine_not_enabled", "manual_approval_required"}.issubset(
        set(candidate["blocked_by"])
    )
    assert candidate["score_snapshot"]["exists"] is False
    assert candidate["execution_action"] == "none"
    assert payload["source_inputs"]["decision_snapshot"]["exists"] is False


def test_order_intent_candidates_mark_data_gate_blocked(tmp_path: Path) -> None:
    as_of = date(2026, 5, 4)
    daily_summary_path = _write_daily_decision_summary(
        tmp_path,
        as_of,
        data_gate={"status": "MISSING"},
    )
    snapshot_path = _write_decision_snapshot(tmp_path, as_of)

    payload = build_order_intent_candidates_payload(
        as_of=as_of,
        daily_decision_summary_path=daily_summary_path,
        decision_snapshot_path=snapshot_path,
        project_root=tmp_path,
    )

    candidate = payload["candidates"][0]
    assert candidate["blocked"] is True
    assert "data_gate_blocked" in candidate["blocked_by"]
    assert {"trading_engine_not_enabled", "manual_approval_required"}.issubset(
        set(candidate["blocked_by"])
    )


def test_order_intent_candidates_missing_decision_stays_missing(
    tmp_path: Path,
) -> None:
    as_of = date(2026, 5, 4)
    daily_summary_path = _write_daily_decision_summary(
        tmp_path,
        as_of,
        investment_conclusion={
            "availability": "missing",
            "action_bias": "missing",
            "confidence": "missing",
            "position_band": "missing",
            "major_risks": ["未合成投资动作。"],
        },
    )
    snapshot_path = _write_decision_snapshot(tmp_path, as_of)

    payload = build_order_intent_candidates_payload(
        as_of=as_of,
        daily_decision_summary_path=daily_summary_path,
        decision_snapshot_path=snapshot_path,
        project_root=tmp_path,
    )

    candidate = payload["candidates"][0]
    assert candidate["blocked"] is True
    assert "investment_conclusion_missing" in candidate["blocked_by"]
    assert candidate["source_decision"]["action_bias"] == "missing"
    assert candidate["source_decision"]["confidence"] == "missing"
    assert candidate["source_decision"]["position_band"] == "missing"
    assert candidate["execution_action"] == "none"


def _write_daily_decision_summary(
    tmp_path: Path,
    as_of: date,
    *,
    investment_conclusion: dict[str, object] | None = None,
    data_gate: dict[str, object] | None = None,
) -> Path:
    path = tmp_path / f"daily_decision_summary_{as_of.isoformat()}.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "report_type": "daily_decision_summary",
                "as_of": as_of.isoformat(),
                "run_id": f"daily_ops_run:{as_of.isoformat()}:test",
                "production_effect": "none",
                "status": "available",
                "data_gate": data_gate or {"status": "PASS"},
                "investment_conclusion": investment_conclusion or {
                    "availability": "available",
                    "action_bias": "观察",
                    "confidence": "0.71",
                    "position_band": "40%-60%",
                    "major_risks": ["估值 gate 是主要限制。"],
                },
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return path


def _write_decision_snapshot(tmp_path: Path, as_of: date) -> Path:
    path = tmp_path / f"decision_snapshot_{as_of.isoformat()}.json"
    path.write_text(
        json.dumps(
            {
                "snapshot_id": f"decision_snapshot:{as_of.isoformat()}",
                "scores": {
                    "overall_score": 73.0,
                    "confidence_score": 71.0,
                },
                "positions": {
                    "final_risk_asset_ai_band": {
                        "min_position": 0.4,
                        "max_position": 0.6,
                        "label": "AI bucket",
                    },
                    "final_total_risk_asset_band": {
                        "min_position": 0.2,
                        "max_position": 0.3,
                    },
                    "position_gates": [
                        {
                            "gate_id": "valuation",
                            "label": "valuation crowded",
                            "source": "valuation_review",
                            "triggered": True,
                            "max_position": 0.4,
                            "reason": "valuation cap binds",
                        }
                    ],
                },
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return path
