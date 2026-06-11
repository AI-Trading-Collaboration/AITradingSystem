from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from dynamic_v3_pressure_validation_helpers import run_pressure_backfill_fixture

from ai_trading_system.etf_portfolio.dynamic_v3_defensive_evidence import (
    build_forward_pressure_capture_plan,
    run_defensive_failure_study,
    run_defensive_hypothesis_deep_dive,
    run_defensive_label_review,
    run_defensive_owner_pack,
    run_defensive_research_note,
    run_pressure_capture_workflow,
    run_pressure_trigger_scan,
    run_weekly_defensive_evidence_update,
    update_pressure_sample_ledger,
)
from ai_trading_system.etf_portfolio.dynamic_v3_pressure_validation import (
    run_defensive_pressure_compare,
)

GENERATED_AT = datetime(2026, 6, 30, tzinfo=UTC)


def run_defensive_deep_dive_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_pressure_backfill_fixture(tmp_path)
    backfill_root = fixture["pressure_backfill_dir"]
    backfill_id = fixture["pressure_backfill"]["pressure_backfill_id"]
    inventory_path = backfill_root / backfill_id / "pressure_outcome_inventory.jsonl"
    rows = _read_jsonl(inventory_path)
    rows.append(_contradicting_pressure_row())
    _write_jsonl(inventory_path, rows)
    compare_root = tmp_path / "defensive_pressure_compare"
    comparison = run_defensive_pressure_compare(
        pressure_backfill_id=backfill_id,
        backfill_dir=backfill_root,
        output_dir=compare_root,
        generated_at=GENERATED_AT,
    )
    deep_root = tmp_path / "defensive_hypothesis_deep_dive"
    deep_dive = run_defensive_hypothesis_deep_dive(
        pressure_backfill_id=backfill_id,
        comparison_id=comparison["comparison_id"],
        backfill_dir=backfill_root,
        comparison_dir=compare_root,
        output_dir=deep_root,
        generated_at=GENERATED_AT,
    )
    return {
        **fixture,
        "defensive_pressure_compare_dir": compare_root,
        "defensive_pressure_compare": comparison,
        "defensive_hypothesis_deep_dive_dir": deep_root,
        "defensive_hypothesis_deep_dive": deep_dive,
    }


def run_label_review_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_defensive_deep_dive_fixture(tmp_path)
    label_root = tmp_path / "defensive_label_review"
    label = run_defensive_label_review(
        deep_dive_id=fixture["defensive_hypothesis_deep_dive"]["deep_dive_id"],
        deep_dive_dir=fixture["defensive_hypothesis_deep_dive_dir"],
        output_dir=label_root,
        generated_at=GENERATED_AT,
    )
    return {**fixture, "defensive_label_review_dir": label_root, "defensive_label_review": label}


def run_failure_study_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_defensive_deep_dive_fixture(tmp_path)
    failure_root = tmp_path / "defensive_failure_study"
    failure = run_defensive_failure_study(
        deep_dive_id=fixture["defensive_hypothesis_deep_dive"]["deep_dive_id"],
        deep_dive_dir=fixture["defensive_hypothesis_deep_dive_dir"],
        output_dir=failure_root,
        generated_at=GENERATED_AT,
    )
    return {
        **fixture,
        "defensive_failure_study_dir": failure_root,
        "defensive_failure_study": failure,
    }


def run_research_note_fixture(tmp_path: Path) -> dict[str, Any]:
    label_fixture = run_label_review_fixture(tmp_path)
    failure_root = tmp_path / "defensive_failure_study"
    failure = run_defensive_failure_study(
        deep_dive_id=label_fixture["defensive_hypothesis_deep_dive"]["deep_dive_id"],
        deep_dive_dir=label_fixture["defensive_hypothesis_deep_dive_dir"],
        output_dir=failure_root,
        generated_at=GENERATED_AT,
    )
    note_root = tmp_path / "defensive_research_note"
    note = run_defensive_research_note(
        deep_dive_id=label_fixture["defensive_hypothesis_deep_dive"]["deep_dive_id"],
        label_review_id=label_fixture["defensive_label_review"]["label_review_id"],
        failure_study_id=failure["failure_study_id"],
        deep_dive_dir=label_fixture["defensive_hypothesis_deep_dive_dir"],
        label_review_dir=label_fixture["defensive_label_review_dir"],
        failure_study_dir=failure_root,
        output_dir=note_root,
        generated_at=GENERATED_AT,
    )
    return {
        **label_fixture,
        "defensive_failure_study_dir": failure_root,
        "defensive_failure_study": failure,
        "defensive_research_note_dir": note_root,
        "defensive_research_note": note,
    }


def run_owner_pack_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_research_note_fixture(tmp_path)
    pack_root = tmp_path / "defensive_owner_pack"
    pack = run_defensive_owner_pack(
        note_id=fixture["defensive_research_note"]["note_id"],
        note_dir=fixture["defensive_research_note_dir"],
        output_dir=pack_root,
        generated_at=GENERATED_AT,
    )
    return {**fixture, "defensive_owner_pack_dir": pack_root, "defensive_owner_pack": pack}


def run_capture_plan_fixture(tmp_path: Path) -> dict[str, Any]:
    plan_root = tmp_path / "forward_pressure_capture"
    plan = build_forward_pressure_capture_plan(
        config_path=_write_capture_config(tmp_path),
        output_dir=plan_root,
        generated_at=GENERATED_AT,
    )
    return {"capture_plan_dir": plan_root, "capture_plan": plan}


def run_pressure_trigger_fixture(tmp_path: Path, *, triggered: bool = False) -> dict[str, Any]:
    trigger_root = tmp_path / "pressure_trigger"
    trigger = run_pressure_trigger_scan(
        as_of=date(2026, 6, 21),
        config_path=_write_capture_config(tmp_path),
        output_dir=trigger_root,
        prices_path=_write_price_fixture(tmp_path, triggered=triggered),
        enforce_data_quality_gate=False,
        generated_at=GENERATED_AT,
    )
    return {"pressure_trigger_dir": trigger_root, "pressure_trigger": trigger}


def run_pressure_capture_skip_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_pressure_trigger_fixture(tmp_path, triggered=False)
    capture_root = tmp_path / "pressure_capture"
    capture = run_pressure_capture_workflow(
        trigger_id=fixture["pressure_trigger"]["trigger_id"],
        trigger_dir=fixture["pressure_trigger_dir"],
        output_dir=capture_root,
        enforce_data_quality_gate=False,
        generated_at=GENERATED_AT,
    )
    return {**fixture, "pressure_capture_dir": capture_root, "pressure_capture": capture}


def run_pressure_capture_force_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_pressure_trigger_fixture(tmp_path, triggered=True)
    capture_root = tmp_path / "pressure_capture"
    capture = run_pressure_capture_workflow(
        trigger_id=fixture["pressure_trigger"]["trigger_id"],
        force=True,
        trigger_dir=fixture["pressure_trigger_dir"],
        output_dir=capture_root,
        pressure_tag_dir=tmp_path / "pressure_regime_tag",
        pressure_backfill_dir=tmp_path / "pressure_outcome_backfill_for_capture",
        comparison_dir=tmp_path / "defensive_pressure_compare_for_capture",
        prices_path=_write_price_fixture(tmp_path, triggered=True),
        enforce_data_quality_gate=False,
        generated_at=GENERATED_AT,
    )
    return {**fixture, "pressure_capture_dir": capture_root, "pressure_capture": capture}


def run_pressure_sample_ledger_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_defensive_deep_dive_fixture(tmp_path)
    ledger_root = tmp_path / "pressure_sample_ledger"
    ledger = update_pressure_sample_ledger(
        output_dir=ledger_root,
        pressure_backfill_dir=fixture["pressure_backfill_dir"],
        config_path=_write_capture_config(tmp_path),
        generated_at=GENERATED_AT,
    )
    return {**fixture, "pressure_sample_ledger_dir": ledger_root, "pressure_sample_ledger": ledger}


def run_weekly_defensive_evidence_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_pressure_sample_ledger_fixture(tmp_path)
    weekly_root = tmp_path / "weekly_defensive_evidence"
    weekly = run_weekly_defensive_evidence_update(
        week_ending=date(2026, 6, 30),
        output_dir=weekly_root,
        ledger_dir=fixture["pressure_sample_ledger_dir"],
        generated_at=GENERATED_AT,
    )
    return {**fixture, "weekly_defensive_evidence_dir": weekly_root, "weekly_defensive": weekly}


def _contradicting_pressure_row() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "pressure_outcome_id": "pressure-outcome-failure",
        "source_mode": "BACKTEST_SIMULATION",
        "source_artifact_id": "sim-outcome-fixture",
        "source_event_id": "sim-event-failure",
        "as_of": "2026-06-24",
        "window_days": 20,
        "regime_tags": ["tech_drawdown"],
        "pressure_regime": True,
        "defensive_validation_relevant": True,
        "outcome_status": "AVAILABLE",
        "variant_results": {
            "no_trade": {"return": -0.010, "max_drawdown": -0.040, "turnover": 0.0},
            "defensive_limited_adjustment": {
                "return": -0.024,
                "max_drawdown": -0.065,
                "turnover": 0.025,
                "risk_asset_exposure": 0.70,
            },
            "limited_adjustment": {"return": -0.012, "max_drawdown": -0.050, "turnover": 0.02},
            "consensus_target": {"return": -0.014, "max_drawdown": -0.052, "turnover": 0.02},
        },
        "evidence_quality": "SIMULATION_NOT_PIT",
        "can_support_production": False,
        "production_effect": "none",
        "broker_action_allowed": False,
    }


def _write_capture_config(tmp_path: Path) -> Path:
    tmp_path.mkdir(parents=True, exist_ok=True)
    path = tmp_path / "forward_pressure_capture_v1.yaml"
    path.write_text(
        "\n".join(
            [
                "schema_version: 1",
                "policy_metadata:",
                "  owner: project_owner",
                "  version: test_forward_pressure_capture_v1",
                "daily:",
                "  enabled: true",
                "  commands:",
                "    - shadow-monitor run",
                "    - outcome-due scan",
                "weekly:",
                "  enabled: true",
                "  commands:",
                "    - pressure-regime-tag run",
                "    - pressure-sample-ledger update",
                "event_driven:",
                "  enabled: true",
                "  triggers:",
                "    qqq_1w_drawdown_pct: -0.05",
                "    smh_1w_drawdown_pct: -0.07",
                "    qqq_1d_drawdown_pct: -0.03",
                "    smh_1d_drawdown_pct: -0.04",
                "  commands:",
                "    - pressure-regime-tag run",
                "    - pressure-outcome-backfill run",
                "    - defensive-pressure-compare run",
                "validation:",
                "  required_forward_pressure_samples: 5",
                "safety:",
                "  broker_action_allowed: false",
                "  production_effect: none",
                "  auto_apply_policy: false",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return path


def _write_price_fixture(tmp_path: Path, *, triggered: bool) -> Path:
    tmp_path.mkdir(parents=True, exist_ok=True)
    path = tmp_path / ("prices_triggered.csv" if triggered else "prices_no_trigger.csv")
    qqq_prices = [100, 101, 102, 103, 104, 105]
    smh_prices = [200, 201, 202, 203, 204, 205]
    if triggered:
        qqq_prices = [100, 99, 98, 97, 96, 94]
        smh_prices = [200, 198, 196, 194, 192, 190]
    dates = [
        "2026-06-15",
        "2026-06-16",
        "2026-06-17",
        "2026-06-18",
        "2026-06-19",
        "2026-06-21",
    ]
    rows = ["date,symbol,adj_close"]
    for day, price in zip(dates, qqq_prices, strict=True):
        rows.append(f"{day},QQQ,{price}")
    for day, price in zip(dates, smh_prices, strict=True):
        rows.append(f"{day},SMH,{price}")
    path.write_text("\n".join(rows) + "\n", encoding="utf-8")
    return path


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
        encoding="utf-8",
    )
