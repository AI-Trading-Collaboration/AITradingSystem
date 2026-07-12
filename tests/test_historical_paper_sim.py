from __future__ import annotations

import json
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace
from typing import Any

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
    run_historical_paper_sim,
    run_historical_replay,
    validate_historical_paper_sim_artifact,
)


def test_historical_paper_sim_rebuilds_state_and_ledger_without_broker_action(
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
    first_target = {"QQQ": 0.45, "SMH": 0.30, "SOXX": 0.10, "CASH": 0.15}
    second_target = {"QQQ": 0.40, "SMH": 0.35, "SOXX": 0.10, "CASH": 0.15}
    write_replay_daily_advisory(
        paths["daily_advisory_dir"],
        daily_advisory_id="first",
        as_of="2026-06-03",
        target_weights=first_target,
    )
    write_replay_daily_advisory(
        paths["daily_advisory_dir"],
        daily_advisory_id="second",
        as_of="2026-06-10",
        target_weights=second_target,
    )
    write_owner_reviews(paths["owner_review_dir"], ["first", "second"])
    inventory = build_replay_inventory(
        paths,
        start=date(2026, 6, 1),
        end=date(2026, 6, 30),
        generated_at=datetime(2026, 6, 30, tzinfo=UTC),
    )
    replay = run_historical_replay(
        inventory_id=inventory["inventory_id"],
        include_pit_warning=True,
        inventory_dir=paths["inventory_dir"],
        output_dir=paths["historical_replay_dir"],
        generated_at=datetime(2026, 6, 30, tzinfo=UTC),
    )

    sim = run_historical_paper_sim(
        replay_id=replay["replay_id"],
        variant="limited_adjustment",
        replay_dir=paths["historical_replay_dir"],
        output_dir=paths["paper_sim_dir"],
        prices_path=paths["prices_path"],
        rates_path=paths["rates_path"],
        config_path=paths["config_path"],
        enforce_data_quality_gate=False,
        generated_at=datetime(2026, 7, 1, tzinfo=UTC),
    )

    assert sim["performance_summary"]["simulation_status"] == "AVAILABLE", sim[
        "performance_summary"
    ]["simulation_reason"]
    assert sim["performance_summary"]["variant"] == "limited_adjustment"
    assert sim["state_history"]
    assert sim["trade_ledger"]
    assert sim["performance_summary"]["turnover"] > 0
    assert sim["performance_summary"]["estimated_cost"] > 0
    assert sim["manifest"]["cost_rate"] == 0.001
    assert all(
        row["estimated_cost"] == round(row["turnover"] * 0.001, 8) for row in sim["trade_ledger"]
    )
    assert sim["manifest"]["broker_action_taken"] is False
    assert all(row["broker_action_taken"] is False for row in sim["trade_ledger"])

    validation = validate_historical_paper_sim_artifact(
        sim_id=sim["sim_id"],
        output_dir=paths["paper_sim_dir"],
    )
    assert validation["status"] == "PASS"


def _build_valid_replay(paths: dict[str, Path]) -> dict[str, Any]:
    for daily_id, as_of, target in (
        (
            "first",
            "2026-06-03",
            {"QQQ": 0.45, "SMH": 0.30, "SOXX": 0.10, "CASH": 0.15},
        ),
        (
            "second",
            "2026-06-10",
            {"QQQ": 0.40, "SMH": 0.35, "SOXX": 0.10, "CASH": 0.15},
        ),
    ):
        write_replay_daily_advisory(
            paths["daily_advisory_dir"],
            daily_advisory_id=daily_id,
            as_of=as_of,
            target_weights=target,
        )
    write_owner_reviews(paths["owner_review_dir"], ["first", "second"])
    inventory = build_replay_inventory(
        paths,
        start=date(2026, 6, 1),
        end=date(2026, 6, 30),
        generated_at=datetime(2026, 6, 30, tzinfo=UTC),
    )
    return run_historical_replay(
        inventory_id=inventory["inventory_id"],
        include_pit_warning=True,
        inventory_dir=paths["inventory_dir"],
        output_dir=paths["historical_replay_dir"],
        generated_at=datetime(2026, 6, 30, tzinfo=UTC),
    )


def test_historical_paper_sim_fails_before_output_for_time_or_dq(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    paths = prepare_replay_test_environment(tmp_path, monkeypatch)
    replay = _build_valid_replay(paths)
    with pytest.raises(DynamicV3HistoricalReplayError, match="cannot precede"):
        run_historical_paper_sim(
            replay_id=replay["replay_id"],
            replay_dir=paths["historical_replay_dir"],
            output_dir=paths["paper_sim_dir"],
            prices_path=paths["prices_path"],
            rates_path=paths["rates_path"],
            config_path=paths["config_path"],
            enforce_data_quality_gate=False,
            generated_at=datetime(2026, 6, 29, tzinfo=UTC),
        )
    assert not paths["paper_sim_dir"].exists()

    monkeypatch.setattr(
        replay_module,
        "_validate_cached_data_quality",
        lambda **_: SimpleNamespace(passed=False, status="FAIL"),
    )
    with pytest.raises(DynamicV3HistoricalReplayError, match="data quality gate failed"):
        run_historical_paper_sim(
            replay_id=replay["replay_id"],
            replay_dir=paths["historical_replay_dir"],
            output_dir=paths["paper_sim_dir"],
            prices_path=paths["prices_path"],
            rates_path=paths["rates_path"],
            config_path=paths["config_path"],
            generated_at=datetime(2026, 7, 1, tzinfo=UTC),
        )
    assert not paths["paper_sim_dir"].exists()


@pytest.mark.parametrize("tamper_target", ["snapshot", "history", "report", "source"])
def test_historical_paper_sim_validator_recomputes_all_views(
    tmp_path: Path,
    monkeypatch: Any,
    tamper_target: str,
) -> None:
    paths = prepare_replay_test_environment(tmp_path, monkeypatch)
    replay = _build_valid_replay(paths)
    sim = run_historical_paper_sim(
        replay_id=replay["replay_id"],
        replay_dir=paths["historical_replay_dir"],
        output_dir=paths["paper_sim_dir"],
        prices_path=paths["prices_path"],
        rates_path=paths["rates_path"],
        config_path=paths["config_path"],
        enforce_data_quality_gate=False,
        generated_at=datetime(2026, 7, 1, tzinfo=UTC),
    )
    sim_dir = Path(sim["sim_dir"])
    if tamper_target == "snapshot":
        snapshot_path = sim_dir / "historical_paper_sim_source_snapshot.json"
        snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
        snapshot["price_rows"][0]["adj_close"] += 1.0
        snapshot_path.write_text(
            json.dumps(snapshot, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        manifest_path = sim_dir / "historical_paper_sim_manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["source_snapshot_checksum"] = sha256(snapshot_path.read_bytes()).hexdigest()
        manifest_path.write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    elif tamper_target == "history":
        history_path = sim_dir / "simulated_paper_state_history.jsonl"
        rows = [json.loads(line) for line in history_path.read_text(encoding="utf-8").splitlines()]
        rows[0]["portfolio_value"] += 0.1
        history_path.write_text(
            "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
            encoding="utf-8",
        )
    elif tamper_target == "report":
        report_path = sim_dir / "historical_paper_sim_report.md"
        report_path.write_text(report_path.read_text(encoding="utf-8") + "tamper", encoding="utf-8")
    else:
        source_path = Path(replay["replay_dir"]) / "historical_replay_manifest.json"
        source_path.write_text(source_path.read_text(encoding="utf-8") + " ", encoding="utf-8")

    assert (
        validate_historical_paper_sim_artifact(
            sim_id=sim["sim_id"],
            output_dir=paths["paper_sim_dir"],
        )["status"]
        == "FAIL"
    )
