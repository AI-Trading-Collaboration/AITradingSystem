from __future__ import annotations

import json
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from dynamic_v3_historical_replay_helpers import (
    build_replay_review_chain,
    prepare_replay_test_environment,
)

from ai_trading_system.etf_portfolio import dynamic_v3_historical_replay as replay_module
from ai_trading_system.etf_portfolio.dynamic_v3_historical_replay import (
    DynamicV3HistoricalReplayError,
    run_backfill_repair,
    run_replay_diagnosis,
    validate_backfill_repair_artifact,
)


def _build_repair_chain(paths: dict[str, Path]) -> dict[str, Any]:
    chain = build_replay_review_chain(
        paths,
        backfill_generated_at=datetime(2026, 6, 30, tzinfo=UTC),
    )
    diagnosis = run_replay_diagnosis(
        inventory_id=chain["inventory"]["inventory_id"],
        replay_id=chain["replay"]["replay_id"],
        backfill_id=chain["backfill"]["backfill_id"],
        sim_id=chain["sim"]["sim_id"],
        review_id=chain["review"]["review_id"],
        inventory_dir=paths["inventory_dir"],
        replay_dir=paths["historical_replay_dir"],
        backfill_dir=paths["backfill_dir"],
        sim_dir=paths["paper_sim_dir"],
        review_dir=paths["performance_review_dir"],
        output_dir=paths["diagnosis_dir"],
        generated_at=datetime(2026, 7, 21, tzinfo=UTC),
    )
    return {**chain, "diagnosis": diagnosis}


def _run_repair(paths: dict[str, Path], chain: dict[str, Any]) -> dict[str, Any]:
    return run_backfill_repair(
        backfill_id=chain["backfill"]["backfill_id"],
        diagnosis_id=chain["diagnosis"]["diagnosis_id"],
        backfill_dir=paths["backfill_dir"],
        diagnosis_dir=paths["diagnosis_dir"],
        replay_dir=paths["historical_replay_dir"],
        output_dir=paths["backfill_repair_dir"],
        prices_path=paths["prices_path"],
        rates_path=paths["rates_path"],
        enforce_data_quality_gate=False,
        generated_at=datetime(2026, 7, 21, tzinfo=UTC),
    )


def test_backfill_repair_recomputes_available_windows_without_future_decision_leakage(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    paths = prepare_replay_test_environment(tmp_path, monkeypatch)
    chain = _build_repair_chain(paths)
    diagnosis = chain["diagnosis"]

    repair = run_backfill_repair(
        backfill_id=chain["backfill"]["backfill_id"],
        diagnosis_id=diagnosis["diagnosis_id"],
        backfill_dir=paths["backfill_dir"],
        diagnosis_dir=paths["diagnosis_dir"],
        replay_dir=paths["historical_replay_dir"],
        output_dir=paths["backfill_repair_dir"],
        prices_path=paths["prices_path"],
        rates_path=paths["rates_path"],
        enforce_data_quality_gate=False,
        generated_at=datetime(2026, 7, 21, tzinfo=UTC),
    )

    delta = repair["backfill_availability_delta"]
    assert delta["repaired_count"] > 0
    assert delta["sample_unit"] == "event_variant_window"
    assert delta["after"]["available"] > delta["before"]["available"]
    assert delta["still_pending_count"] < delta["before"]["pending"]
    assert repair["manifest"]["future_data_used_in_decision"] is False
    assert repair["manifest"]["repair_count_unit"] == "event_variant_window"
    assert (repair["repair_dir"] / "backfill_repair_source_snapshot.json").is_file()
    assert all(
        action["future_data_used_in_decision"] is False for action in repair["repair_actions"]
    )

    validation = validate_backfill_repair_artifact(
        repair_id=repair["repair_id"],
        output_dir=paths["backfill_repair_dir"],
    )
    assert validation["status"] == "PASS"


def test_backfill_repair_fails_before_output_for_time_or_data_quality(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    paths = prepare_replay_test_environment(tmp_path, monkeypatch)
    chain = _build_repair_chain(paths)
    with pytest.raises(DynamicV3HistoricalReplayError, match="must not precede"):
        run_backfill_repair(
            backfill_id=chain["backfill"]["backfill_id"],
            diagnosis_id=chain["diagnosis"]["diagnosis_id"],
            backfill_dir=paths["backfill_dir"],
            diagnosis_dir=paths["diagnosis_dir"],
            replay_dir=paths["historical_replay_dir"],
            output_dir=paths["backfill_repair_dir"],
            prices_path=paths["prices_path"],
            rates_path=paths["rates_path"],
            enforce_data_quality_gate=False,
            generated_at=datetime(2026, 7, 20, tzinfo=UTC),
        )
    assert not paths["backfill_repair_dir"].exists()

    monkeypatch.setattr(
        replay_module,
        "_validate_cached_data_quality",
        lambda **_: SimpleNamespace(passed=False, status="FAIL"),
    )
    with pytest.raises(DynamicV3HistoricalReplayError, match="data quality gate failed"):
        run_backfill_repair(
            backfill_id=chain["backfill"]["backfill_id"],
            diagnosis_id=chain["diagnosis"]["diagnosis_id"],
            backfill_dir=paths["backfill_dir"],
            diagnosis_dir=paths["diagnosis_dir"],
            replay_dir=paths["historical_replay_dir"],
            output_dir=paths["backfill_repair_dir"],
            prices_path=paths["prices_path"],
            rates_path=paths["rates_path"],
            generated_at=datetime(2026, 7, 22, tzinfo=UTC),
        )
    assert not paths["backfill_repair_dir"].exists()


@pytest.mark.parametrize("tamper_target", ["snapshot", "actions", "report", "source"])
def test_backfill_repair_validator_recomputes_all_views(
    tmp_path: Path,
    monkeypatch: Any,
    tamper_target: str,
) -> None:
    paths = prepare_replay_test_environment(tmp_path, monkeypatch)
    chain = _build_repair_chain(paths)
    repair = _run_repair(paths, chain)
    repair_dir = Path(repair["repair_dir"])
    if tamper_target == "snapshot":
        snapshot_path = repair_dir / "backfill_repair_source_snapshot.json"
        snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
        snapshot["cost_rate"] = 0.99
        snapshot_path.write_text(
            json.dumps(snapshot, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        manifest_path = repair_dir / "backfill_repair_manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["source_snapshot_checksum"] = sha256(snapshot_path.read_bytes()).hexdigest()
        manifest_path.write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    elif tamper_target == "actions":
        path = repair_dir / "repair_actions.jsonl"
        records = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
        records[0]["repair_action"] = "tampered"
        path.write_text(
            "".join(json.dumps(row, sort_keys=True) + "\n" for row in records),
            encoding="utf-8",
        )
    elif tamper_target == "report":
        path = repair_dir / "backfill_repair_report.md"
        path.write_text(path.read_text(encoding="utf-8") + "tamper", encoding="utf-8")
    else:
        path = Path(chain["backfill"]["backfill_dir"]) / "backfill_manifest.json"
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["available_count"] += 1
        path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    assert (
        validate_backfill_repair_artifact(
            repair_id=repair["repair_id"],
            output_dir=paths["backfill_repair_dir"],
        )["status"]
        == "FAIL"
    )
