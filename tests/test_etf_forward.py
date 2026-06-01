from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd
import pytest

from ai_trading_system.etf_portfolio.forward import (
    build_forward_dashboard_payload,
    build_forward_validation_report,
    build_forward_watchlist_payload,
    build_forward_weekly_review_payload,
    load_forward_simulation_config,
    run_forward_update,
)
from ai_trading_system.etf_portfolio.forward_state import (
    load_shadow_candidate_registry,
    validate_shadow_candidate_record,
    validate_shadow_candidate_registry,
    write_shadow_candidate_registry,
)


def test_etf_shadow_state_round_trip_is_valid_and_deterministic(tmp_path: Path) -> None:
    path = tmp_path / "etf_shadow_candidates.json"
    payload = _shadow_registry()

    first = write_shadow_candidate_registry(payload, path)
    first_text = path.read_text(encoding="utf-8")
    second = write_shadow_candidate_registry(first, path)
    second_text = path.read_text(encoding="utf-8")

    assert first_text == second_text
    assert second == load_shadow_candidate_registry(path)
    validate_shadow_candidate_registry(second)
    assert second["candidates"][0]["status"] == "active"


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("observe_only", False, "observe_only=true"),
        ("production_effect", "target_weights", "production_effect=none"),
        ("broker_action", "submit_order", "broker_action=none"),
        ("status", "production", "prohibited status"),
    ],
)
def test_etf_shadow_state_rejects_unsafe_candidate_fields(
    field: str,
    value: object,
    message: str,
) -> None:
    candidate = _shadow_candidate()
    candidate[field] = value

    with pytest.raises(ValueError, match=message):
        validate_shadow_candidate_record(candidate)


def test_etf_shadow_state_rejects_missing_safety_field() -> None:
    candidate = _shadow_candidate()
    del candidate["broker_action"]

    with pytest.raises(ValueError, match="missing required field"):
        validate_shadow_candidate_record(candidate)


def test_etf_shadow_state_rejects_duplicate_shadow_id() -> None:
    candidate = _shadow_candidate()
    registry = _shadow_registry(candidates=[candidate, {**candidate}])

    with pytest.raises(ValueError, match="duplicate shadow_id"):
        validate_shadow_candidate_registry(registry)


def test_etf_shadow_state_load_rejects_top_level_unsafe_value(tmp_path: Path) -> None:
    path = tmp_path / "unsafe_registry.json"
    payload = _shadow_registry()
    payload["production_effect"] = "target_weights"
    path.write_text(json_dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="production_effect=none"):
        load_shadow_candidate_registry(path)


def test_etf_forward_update_writes_evaluation_only_metrics_and_decisions(
    tmp_path: Path,
) -> None:
    registry_path = tmp_path / "etf_shadow_candidates.json"
    prices_path = tmp_path / "prices.csv"
    decision_path = tmp_path / "etf_forward_decisions.csv"
    output_dir = tmp_path / "reports" / "updates"
    write_shadow_candidate_registry(_shadow_registry(), registry_path)
    _write_prices(prices_path, days=95)

    payload = run_forward_update(
        as_of=date(2023, 3, 31),
        registry_path=registry_path,
        decision_ledger_path=decision_path,
        prices_path=prices_path,
        output_dir=output_dir,
        generated_at=datetime(2026, 6, 1, tzinfo=UTC),
    )

    assert payload["observe_only"] is True
    assert payload["production_effect"] == "none"
    assert payload["broker_action"] == "none"
    assert payload["manual_review_required"] is True
    [evaluation] = payload["candidate_evaluations"]
    assert evaluation["evaluation_only"] is True
    assert evaluation["evaluation_as_of_date"] == "2023-03-31"
    assert evaluation["excess_return_vs_baseline"] is not None
    assert evaluation["excess_return_vs_QQQ"] is not None
    assert "5d" in evaluation["rolling_metrics"]
    assert output_dir.joinpath("forward_update_2023-03-31.json").exists()

    ledger = pd.read_csv(decision_path)
    assert not ledger.empty
    assert set(ledger["record_type"]) == {"decision"}
    assert set(ledger["evaluation_only"]) == {False}
    assert not any(column.startswith("forward_") for column in ledger.columns)

    registry = load_shadow_candidate_registry(registry_path)
    updated = registry["candidates"][0]
    assert updated["last_evaluated_date"] == "2023-03-31"
    assert updated["production_effect"] == "none"
    assert updated["broker_action"] == "none"


def test_etf_forward_update_skips_inactive_candidate(tmp_path: Path) -> None:
    registry_path = tmp_path / "etf_shadow_candidates.json"
    prices_path = tmp_path / "prices.csv"
    candidate = _shadow_candidate(status="rejected")
    write_shadow_candidate_registry(_shadow_registry(candidates=[candidate]), registry_path)
    _write_prices(prices_path, days=95)

    payload = run_forward_update(
        as_of=date(2023, 3, 31),
        registry_path=registry_path,
        decision_ledger_path=tmp_path / "decisions.csv",
        prices_path=prices_path,
        output_dir=tmp_path / "reports",
        generated_at=datetime(2026, 6, 1, tzinfo=UTC),
    )

    assert payload["status"] == "NO_ACTIVE_SHADOW_CANDIDATES"
    assert payload["candidate_evaluations"] == []
    assert payload["skipped_candidates"][0]["reason"] == "inactive_candidate"


def test_etf_forward_dashboard_weekly_and_watchlist_outputs(tmp_path: Path) -> None:
    registry_path = tmp_path / "registry.json"
    write_shadow_candidate_registry(_shadow_registry(), registry_path)
    update_payload = _update_payload(
        status="WATCH",
        candidate_status="watch",
        excess_return_vs_baseline=-0.03,
    )
    dashboard = build_forward_dashboard_payload(
        as_of=date(2023, 3, 31),
        update_payload=update_payload,
        registry_path=registry_path,
        generated_at=datetime(2026, 6, 1, tzinfo=UTC),
    )
    weekly = build_forward_weekly_review_payload(
        as_of=date(2023, 3, 31),
        dashboard_payload=dashboard,
        generated_at=datetime(2026, 6, 1, tzinfo=UTC),
    )
    watchlist = build_forward_watchlist_payload(
        as_of=date(2023, 3, 31),
        dashboard_payload=dashboard,
        generated_at=datetime(2026, 6, 1, tzinfo=UTC),
    )

    row = dashboard["candidate_summary_table"][0]
    assert row["status"] == "watch"
    assert dashboard["baseline_comparison"]["available_count"] == 1
    assert dashboard["benchmark_comparison"]["QQQ"]["available_count"] == 1
    assert "observe_only=true" in dashboard["safety_banner"]
    assert weekly["recommended_next_actions"] == ["watch"]
    assert watchlist["status"] == "ATTENTION_REQUIRED"
    assert watchlist["attention_required"][0]["recommended_action"] == "needs_manual_review"


def test_etf_forward_validation_fails_on_decision_leak(tmp_path: Path) -> None:
    ledger_path = tmp_path / "decisions.csv"
    pd.DataFrame(
        [
            {
                "signal_date": "2023-03-30",
                "execution_date": "2023-03-31",
                "symbol": "QQQ",
                "target_weight": 0.5,
                "forward_return_5d": 0.01,
                "evaluation_only": False,
            }
        ]
    ).to_csv(ledger_path, index=False)

    payload = build_forward_validation_report(
        registry_path=tmp_path / "missing_registry.json",
        decision_ledger_path=ledger_path,
        report_registry_path=Path("config/report_registry.yaml"),
        generated_at=datetime(2026, 6, 1, tzinfo=UTC),
    )

    assert payload["status"] == "FAIL"
    check = next(
        item for item in payload["checks"] if item["check_id"] == "decision_evaluation_separation"
    )
    assert check["status"] == "FAIL"


def test_etf_forward_config_exposes_lifecycle_thresholds() -> None:
    config = load_forward_simulation_config()

    assert config["safety"]["observe_only"] is True
    assert config["safety"]["production_effect"] == "none"
    assert config["safety"]["broker_action"] == "none"
    assert config["safety"]["manual_review_required"] is True
    assert config["lifecycle_thresholds"]["minimum_forward_days"] == 20


def _shadow_registry(
    *,
    candidates: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    selected = candidates if candidates is not None else [_shadow_candidate()]
    return {
        "schema_version": "etf_shadow_candidate_registry_v2",
        "registry_type": "etf_shadow_candidates",
        "updated_at": "2026-06-01T00:00:00+00:00",
        "candidate_count": len(selected),
        "candidates": selected,
        "observe_only": True,
        "production_effect": "none",
        "broker_action": "none",
        "manual_review_required": True,
        "production_promotion_allowed": False,
    }


def _shadow_candidate(status: str = "active") -> dict[str, object]:
    return {
        "shadow_id": "etf_shadow_unit_base_ai_growth",
        "candidate_id": "unit_run:base_ai_growth",
        "experiment_id": "base_ai_growth",
        "source_run_id": "unit_run",
        "source_pack_id": "etf_calibration_v1",
        "enrolled_at": "2022-12-01T00:00:00+00:00",
        "enrollment_date": "2022-12-01",
        "model_version": "etf_model_base_ai_growth",
        "config_hash": "config_hash_base_ai_growth",
        "data_hash": "price_hash",
        "ranking_score": 1.25,
        "ranking_summary": {"selection_reasons": ["fixture"]},
        "selection_gate_status": "eligible_for_shadow",
        "status": status,
        "observe_only": True,
        "production_effect": "none",
        "broker_action": "none",
        "manual_review_required": True,
        "production_promotion_allowed": False,
        "evaluation_schedule": {
            "cadence": "daily",
            "start_date": "2022-12-01",
            "weekly_review_task": "TRADING-065F",
        },
        "last_evaluated_at": None,
        "last_evaluated_date": None,
        "notes": [],
    }


def json_dumps(payload: object) -> str:
    return json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"


def _write_prices(path: Path, *, days: int) -> None:
    dates = pd.bdate_range("2022-12-01", periods=days)
    rows = []
    for symbol_index, symbol in enumerate(["SPY", "QQQ", "SMH", "SOXX"]):
        for index, current_date in enumerate(dates):
            drift = 0.45 + symbol_index * 0.04
            price = 100.0 + index * drift + symbol_index
            rows.append(
                {
                    "date": current_date.date().isoformat(),
                    "symbol": symbol,
                    "open": price,
                    "high": price * 1.01,
                    "low": price * 0.99,
                    "close": price,
                    "adj_close": price,
                    "volume": 1_000_000,
                    "source": "fixture",
                    "created_at": "2026-06-01T00:00:00+00:00",
                }
            )
    pd.DataFrame(rows).to_csv(path, index=False)


def _update_payload(
    *,
    status: str,
    candidate_status: str,
    excess_return_vs_baseline: float,
) -> dict[str, object]:
    return {
        "schema_version": "etf_forward_update_v1",
        "report_type": "etf_forward_update",
        "status": status,
        "as_of": "2023-03-31",
        "candidate_evaluations": [
            {
                "shadow_id": "etf_shadow_unit_base_ai_growth",
                "candidate_id": "unit_run:base_ai_growth",
                "experiment_id": "base_ai_growth",
                "status": "active",
                "evaluation_as_of_date": "2023-03-31",
                "days_since_enrollment": 60,
                "return_since_enrollment": 0.04,
                "excess_return_vs_baseline": excess_return_vs_baseline,
                "excess_return_vs_QQQ": -0.01,
                "excess_return_vs_SPY": 0.02,
                "excess_return_vs_SMH": -0.015,
                "max_drawdown_since_enrollment": -0.04,
                "turnover_since_enrollment": 0.3,
                "constraint_hits_since_enrollment": 0,
                "recommended_status": candidate_status,
                "recommended_action": candidate_status,
                "status_reasons": ["fixture"],
                "rolling_metrics": {"5d": {"window_return": 0.01}},
                "metric_null_reasons": {},
                "evaluation_only": True,
            }
        ],
        "observe_only": True,
        "production_effect": "none",
        "broker_action": "none",
        "manual_review_required": True,
    }
