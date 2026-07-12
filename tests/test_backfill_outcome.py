from __future__ import annotations

import json
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pandas as pd
import pytest
import yaml
from dynamic_v3_historical_replay_helpers import (
    build_replay_inventory,
    prepare_replay_test_environment,
    write_owner_reviews,
    write_replay_daily_advisory,
)

from ai_trading_system.etf_portfolio import dynamic_v3_historical_replay as replay_module
from ai_trading_system.etf_portfolio.dynamic_v3_historical_replay import (
    DynamicV3HistoricalReplayError,
    run_backfill_outcome,
    run_historical_replay,
    validate_backfill_outcome_artifact,
)


def _remove_symbol_prices_on_or_after(prices_path: Path, *, symbol: str, start: str) -> None:
    prices = pd.read_csv(prices_path)
    symbol_column = "symbol" if "symbol" in prices.columns else "ticker"
    dates = pd.to_datetime(prices["date"], errors="coerce")
    missing_window_mask = prices[symbol_column].astype(str).eq(symbol) & (
        dates >= pd.Timestamp(start)
    )
    prices.loc[~missing_window_mask].to_csv(prices_path, index=False)


def test_backfill_outcome_distinguishes_available_pending_and_insufficient_data(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    paths = prepare_replay_test_environment(tmp_path, monkeypatch)
    target = {"QQQ": 0.45, "SMH": 0.30, "SOXX": 0.10, "CASH": 0.15}
    write_replay_daily_advisory(
        paths["daily_advisory_dir"],
        daily_advisory_id="available",
        as_of="2026-06-03",
        target_weights=target,
    )
    write_replay_daily_advisory(
        paths["daily_advisory_dir"],
        daily_advisory_id="insufficient-history",
        as_of="2026-06-04",
        current_weights={"QQQ": 0.50, "SMH": 0.20, "SOXX": 0.10, "CASH": 0.20},
        target_weights={"QQQ": 0.45, "SMH": 0.30, "SOXX": 0.10, "CASH": 0.15},
    )
    write_replay_daily_advisory(
        paths["daily_advisory_dir"],
        daily_advisory_id="pending",
        as_of="2026-07-30",
        current_weights={"QQQ": 0.80, "CASH": 0.20},
        target_weights={"QQQ": 0.75, "CASH": 0.25},
    )
    write_owner_reviews(
        paths["owner_review_dir"],
        ["available", "insufficient-history", "pending"],
    )
    _remove_symbol_prices_on_or_after(
        paths["prices_path"],
        symbol="SMH",
        start="2026-06-11",
    )
    inventory = build_replay_inventory(
        paths,
        start=date(2026, 6, 1),
        end=date(2026, 7, 31),
        generated_at=datetime(2026, 7, 31, tzinfo=UTC),
    )
    replay = run_historical_replay(
        inventory_id=inventory["inventory_id"],
        include_pit_warning=True,
        inventory_dir=paths["inventory_dir"],
        output_dir=paths["historical_replay_dir"],
        generated_at=datetime(2026, 7, 31, tzinfo=UTC),
    )
    assert {event["daily_advisory_id"] for event in replay["events"]} >= {
        "available",
        "insufficient-history",
        "pending",
    }
    backfill = run_backfill_outcome(
        replay_id=replay["replay_id"],
        replay_dir=paths["historical_replay_dir"],
        output_dir=paths["backfill_dir"],
        prices_path=paths["prices_path"],
        rates_path=paths["rates_path"],
        config_path=paths["config_path"],
        enforce_data_quality_gate=False,
        generated_at=datetime(2026, 7, 31, tzinfo=UTC),
    )
    statuses = {row["outcome_status"] for row in backfill["outcome_rows"]}

    assert {"AVAILABLE", "PENDING", "INSUFFICIENT_DATA"} <= statuses
    assert backfill["manifest"]["replay_event_count"] == replay["manifest"]["replay_event_count"]
    assert backfill["manifest"]["data_quality_status"] == "SKIPPED_EXPLICIT_TEST_FIXTURE"
    assert backfill["manifest"]["available_count"] > 0
    assert backfill["manifest"]["pending_count"] > 0
    assert backfill["manifest"]["insufficient_data_count"] > 0
    assert backfill["variant_performance_summary"]["best_variant"] != ""
    assert all(row["outcome_mode"] == "HISTORICAL_REPLAY" for row in backfill["outcome_rows"])
    metric_fields = (
        "gross_return",
        "estimated_cost",
        "return",
        "relative_to_no_trade",
        "relative_to_consensus_target",
        "relative_to_limited_adjustment",
        "max_drawdown",
        "realized_volatility",
    )
    assert all(
        row[field] is None
        for row in backfill["outcome_rows"]
        if row["outcome_status"] != "AVAILABLE"
        for field in metric_fields
    )

    validation = validate_backfill_outcome_artifact(
        backfill_id=backfill["backfill_id"],
        output_dir=paths["backfill_dir"],
    )
    assert validation["status"] == "PASS"


def test_backfill_applies_configured_initial_turnover_cost_once(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    paths = prepare_replay_test_environment(tmp_path, monkeypatch)
    config = yaml.safe_load(paths["config_path"].read_text(encoding="utf-8"))
    config["simulation"]["transaction_cost_bps"] = 8
    config["simulation"]["slippage_bps"] = 2
    paths["config_path"].write_text(
        yaml.safe_dump(config, sort_keys=False),
        encoding="utf-8",
    )
    replay = _build_valid_replay(paths)
    backfill = run_backfill_outcome(
        replay_id=replay["replay_id"],
        replay_dir=paths["historical_replay_dir"],
        output_dir=paths["backfill_dir"],
        prices_path=paths["prices_path"],
        rates_path=paths["rates_path"],
        config_path=paths["config_path"],
        enforce_data_quality_gate=False,
        generated_at=datetime(2026, 6, 10, tzinfo=UTC),
    )
    available = [
        row for row in backfill["outcome_rows"] if row["outcome_status"] == "AVAILABLE"
    ]

    assert available
    assert backfill["manifest"]["cost_rate"] == 0.001
    assert all(
        row["return"] == round(row["gross_return"] - row["turnover"] * 0.001, 6)
        for row in available
    )
    assert all(row["turnover_convention"] == "one_way_l1_weight_change" for row in available)
    assert all(
        row["risk_metric_cost_role"] == "gross_price_path_cost_not_applied"
        for row in available
    )
    assert (
        validate_backfill_outcome_artifact(
            backfill_id=backfill["backfill_id"],
            output_dir=paths["backfill_dir"],
        )["status"]
        == "PASS"
    )


def _build_valid_replay(paths: dict[str, Path]) -> dict[str, Any]:
    target = {"QQQ": 0.45, "SMH": 0.30, "SOXX": 0.10, "CASH": 0.15}
    write_replay_daily_advisory(
        paths["daily_advisory_dir"],
        daily_advisory_id="safe",
        as_of="2026-06-03",
        target_weights=target,
    )
    write_owner_reviews(paths["owner_review_dir"], ["safe"])
    inventory = build_replay_inventory(paths, start=date(2026, 6, 1), end=date(2026, 6, 10))
    return run_historical_replay(
        inventory_id=inventory["inventory_id"],
        inventory_dir=paths["inventory_dir"],
        output_dir=paths["historical_replay_dir"],
        generated_at=datetime(2026, 6, 10, tzinfo=UTC),
    )


def test_backfill_fails_before_output_for_time_travel_source_or_dq_failure(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    paths = prepare_replay_test_environment(tmp_path, monkeypatch)
    replay = _build_valid_replay(paths)
    with pytest.raises(DynamicV3HistoricalReplayError, match="cannot precede"):
        run_backfill_outcome(
            replay_id=replay["replay_id"],
            replay_dir=paths["historical_replay_dir"],
            output_dir=paths["backfill_dir"],
            prices_path=paths["prices_path"],
            rates_path=paths["rates_path"],
            config_path=paths["config_path"],
            enforce_data_quality_gate=False,
            generated_at=datetime(2026, 6, 9, tzinfo=UTC),
        )
    assert not paths["backfill_dir"].exists()

    monkeypatch.setattr(
        replay_module,
        "_validate_cached_data_quality",
        lambda **_: SimpleNamespace(passed=False, status="FAIL"),
    )
    with pytest.raises(DynamicV3HistoricalReplayError, match="data quality gate failed"):
        run_backfill_outcome(
            replay_id=replay["replay_id"],
            replay_dir=paths["historical_replay_dir"],
            output_dir=paths["backfill_dir"],
            prices_path=paths["prices_path"],
            rates_path=paths["rates_path"],
            config_path=paths["config_path"],
            generated_at=datetime(2026, 6, 10, tzinfo=UTC),
        )
    assert not paths["backfill_dir"].exists()


@pytest.mark.parametrize("tamper_target", ["snapshot", "rows", "report", "source"])
def test_backfill_validator_recomputes_views_and_source_binding(
    tmp_path: Path,
    monkeypatch: Any,
    tamper_target: str,
) -> None:
    paths = prepare_replay_test_environment(tmp_path, monkeypatch)
    replay = _build_valid_replay(paths)
    backfill = run_backfill_outcome(
        replay_id=replay["replay_id"],
        replay_dir=paths["historical_replay_dir"],
        output_dir=paths["backfill_dir"],
        prices_path=paths["prices_path"],
        rates_path=paths["rates_path"],
        config_path=paths["config_path"],
        enforce_data_quality_gate=False,
        generated_at=datetime(2026, 6, 10, tzinfo=UTC),
    )
    backfill_dir = Path(backfill["backfill_dir"])
    if tamper_target == "snapshot":
        snapshot_path = backfill_dir / "backfilled_outcome_source_snapshot.json"
        snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
        snapshot["price_rows"][0]["adj_close"] += 1.0
        snapshot_path.write_text(
            json.dumps(snapshot, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        manifest_path = backfill_dir / "backfill_manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["source_snapshot_checksum"] = sha256(snapshot_path.read_bytes()).hexdigest()
        manifest_path.write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    elif tamper_target == "rows":
        rows_path = backfill_dir / "replay_outcome_windows.jsonl"
        rows = [json.loads(line) for line in rows_path.read_text(encoding="utf-8").splitlines()]
        rows[0]["outcome_status"] = "PENDING"
        rows_path.write_text(
            "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
            encoding="utf-8",
        )
    elif tamper_target == "report":
        report_path = backfill_dir / "backfill_outcome_report.md"
        report_path.write_text(
            report_path.read_text(encoding="utf-8") + "tampered",
            encoding="utf-8",
        )
    else:
        source_path = Path(replay["replay_dir"]) / "historical_replay_manifest.json"
        source_path.write_text(source_path.read_text(encoding="utf-8") + " ", encoding="utf-8")

    validation = validate_backfill_outcome_artifact(
        backfill_id=backfill["backfill_id"],
        output_dir=paths["backfill_dir"],
    )
    assert validation["status"] == "FAIL"
