from __future__ import annotations

import json
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path

import pandas as pd

from ai_trading_system.config import (
    PROJECT_ROOT,
    configured_price_tickers,
    configured_rate_series,
    load_universe,
)
from ai_trading_system.limited_non_pit_reconstruction import (
    build_limited_non_pit_reconstruction,
    validate_limited_non_pit_reconstruction,
)
from ai_trading_system.platform.artifacts import write_json_atomic

AS_OF = date(2026, 7, 21)
OWNER_DECISION_ID = (
    "owner_decision:OPS-068:2026-07-24:approve_limited_non_pit_reconstruction_v1"
)


def test_build_and_validate_limited_non_pit_bundle(tmp_path: Path) -> None:
    inventory = _write_inventory(tmp_path / "inventory")
    guard = tmp_path / "canonical_guard.json"
    guard.write_text('{"status":"FAILED"}\n', encoding="utf-8")

    result = build_limited_non_pit_reconstruction(
        inventory_bundle=inventory,
        owner_decision_id=OWNER_DECISION_ID,
        bundle_id="limited_non_pit_reconstruction_2026-07-21_test",
        project_root=PROJECT_ROOT,
        output_root=tmp_path / "output",
        guard_paths=[guard],
        generated_at=datetime(2026, 7, 24, 12, 0, tzinfo=UTC),
    )

    assert result.validation.passed
    assert guard.read_text(encoding="utf-8") == '{"status":"FAILED"}\n'
    payload = json.loads(result.payload_path.read_text(encoding="utf-8"))
    assert payload["schema_version"] == "limited_non_pit_reconstruction.v2"
    assert payload["canonical_daily_evidence_status"] == "MISSING"
    assert payload["reconstruction_conclusion_status"] == "INSUFFICIENT_DATA"
    assert set(payload["strict_missing_inputs"].values()) == {None}
    assert set(payload["conclusion_outputs"].values()) == {None}
    assert payload["safety"]["production_effect"] == "none"
    assert payload["data_quality"]["canonical_gate"] is False

    validation = validate_limited_non_pit_reconstruction(
        result.bundle_path,
        project_root=PROJECT_ROOT,
        expected_as_of=AS_OF,
        expected_owner_decision_id=OWNER_DECISION_ID,
        expected_inventory_bundle=inventory,
    )
    assert validation.passed
    assert "content_derived_data_quality" in validation.checks
    assert "null_exclusion_and_safety_contract" in validation.checks


def test_validator_rejects_market_fact_tamper(tmp_path: Path) -> None:
    result, inventory = _build_fixture_bundle(tmp_path)
    payload = json.loads(result.payload_path.read_text(encoding="utf-8"))
    payload["market_snapshot"][0]["close"] += 1
    write_json_atomic(result.payload_path, payload)

    validation = validate_limited_non_pit_reconstruction(
        result.bundle_path,
        project_root=PROJECT_ROOT,
        expected_as_of=AS_OF,
        expected_owner_decision_id=OWNER_DECISION_ID,
        expected_inventory_bundle=inventory,
    )

    assert not validation.passed
    assert "LIMITED_NON_PIT_MARKET_SNAPSHOT_DRIFT" in validation.errors[0]


def test_validator_rejects_frozen_input_tamper(tmp_path: Path) -> None:
    result, inventory = _build_fixture_bundle(tmp_path)
    prices_path = result.bundle_path / "input/data/raw/prices_daily.csv"
    prices_path.write_bytes(prices_path.read_bytes() + b"\n")

    validation = validate_limited_non_pit_reconstruction(
        result.bundle_path,
        project_root=PROJECT_ROOT,
        expected_as_of=AS_OF,
        expected_owner_decision_id=OWNER_DECISION_ID,
        expected_inventory_bundle=inventory,
    )

    assert not validation.passed
    assert "LIMITED_NON_PIT_ARTIFACT_POINTER_DRIFT" in validation.errors[0]


def test_validator_rejects_owner_decision_tamper(tmp_path: Path) -> None:
    result, inventory = _build_fixture_bundle(tmp_path)
    payload = json.loads(result.payload_path.read_text(encoding="utf-8"))
    payload["owner_decision_id"] = "owner_decision:OPS-068:tampered"
    write_json_atomic(result.payload_path, payload)

    validation = validate_limited_non_pit_reconstruction(
        result.bundle_path,
        project_root=PROJECT_ROOT,
        expected_as_of=AS_OF,
        expected_owner_decision_id=OWNER_DECISION_ID,
        expected_inventory_bundle=inventory,
    )

    assert not validation.passed
    assert "LIMITED_NON_PIT_PAYLOAD_CONSTANT_DRIFT:owner_decision_id" in (
        validation.errors[0]
    )


def _build_fixture_bundle(
    tmp_path: Path,
) -> tuple[object, Path]:
    inventory = _write_inventory(tmp_path / "inventory")
    guard = tmp_path / "canonical_guard.json"
    guard.write_text('{"status":"FAILED"}\n', encoding="utf-8")
    result = build_limited_non_pit_reconstruction(
        inventory_bundle=inventory,
        owner_decision_id=OWNER_DECISION_ID,
        bundle_id="limited_non_pit_reconstruction_2026-07-21_test",
        project_root=PROJECT_ROOT,
        output_root=tmp_path / "output",
        guard_paths=[guard],
        generated_at=datetime(2026, 7, 24, 12, 0, tzinfo=UTC),
    )
    return result, inventory


def _write_inventory(root: Path) -> Path:
    raw_root = root / "input/data/raw"
    raw_root.mkdir(parents=True)
    universe = load_universe()
    tickers = configured_price_tickers(universe, include_full_ai_chain=False)
    series = configured_rate_series(universe)
    price_rows: list[dict[str, object]] = []
    for ticker_index, ticker in enumerate(tickers):
        for day_index, day in enumerate(("2026-07-20", "2026-07-21")):
            close = 100.0 + ticker_index + day_index
            price_rows.append(
                {
                    "date": day,
                    "ticker": ticker,
                    "open": close - 0.5,
                    "high": close + 1.0,
                    "low": close - 1.0,
                    "close": close,
                    "adj_close": close,
                    "volume": 1000.0,
                }
            )
    rate_rows: list[dict[str, object]] = []
    for series_index, series_id in enumerate(series):
        for day_index, day in enumerate(("2026-07-20", "2026-07-21")):
            rate_rows.append(
                {
                    "date": day,
                    "series": series_id,
                    "value": 2.0 + series_index + (day_index * 0.01),
                }
            )
    primary_path = raw_root / "prices_daily.csv"
    secondary_path = raw_root / "prices_marketstack_daily.csv"
    rates_path = raw_root / "rates_daily.csv"
    pd.DataFrame(price_rows).to_csv(primary_path, index=False)
    pd.DataFrame(price_rows).to_csv(secondary_path, index=False)
    pd.DataFrame(rate_rows).to_csv(rates_path, index=False)

    manifest_path = raw_root / "download_manifest.csv"
    manifest_rows = []
    for source_id, path in (
        ("replay_filtered_prices_daily", primary_path),
        ("replay_filtered_prices_marketstack_daily", secondary_path),
        ("replay_filtered_rates_daily", rates_path),
    ):
        manifest_rows.append(
            {
                "downloaded_at": "2026-07-24T10:47:07+00:00",
                "source_id": source_id,
                "provider": "cache-only replay filter",
                "endpoint": "local raw cache as-of filter",
                "request_parameters": json.dumps(
                    {"as_of": AS_OF.isoformat(), "filter": "date <= as_of"}
                ),
                "output_path": str(path),
                "row_count": len(pd.read_csv(path)),
                "checksum_sha256": _sha(path),
            }
        )
    pd.DataFrame(manifest_rows).to_csv(manifest_path, index=False)

    input_records: list[dict[str, object]] = []
    for artifact_id, path in (
        ("prices_daily", primary_path),
        ("prices_marketstack_daily", secondary_path),
        ("rates_daily", rates_path),
        ("download_manifest", manifest_path),
    ):
        rows = len(pd.read_csv(path))
        input_records.append(
            {
                "artifact_id": artifact_id,
                "artifact_class": "fixture",
                "source_path": str(path),
                "replay_path": str(path),
                "status": "PASS",
                "row_count": rows,
                "included_count": rows,
                "excluded_count": 0,
                "sha256": _sha(path),
                "min_timestamp": None,
                "max_timestamp": None,
                "reason": "test fixture",
            }
        )
    for artifact_id in (
        "fmp_forward_pit_normalized",
        "pit_validation_report",
        "fmp_forward_pit_fetch_report",
        "sec_fundamentals",
        "risk_event_openai_prereview_report",
    ):
        input_records.append(
            {
                "artifact_id": artifact_id,
                "artifact_class": "fixture",
                "source_path": "",
                "replay_path": "",
                "status": "MISSING",
                "row_count": None,
                "included_count": None,
                "excluded_count": None,
                "sha256": None,
                "min_timestamp": None,
                "max_timestamp": None,
                "reason": "test fixture required missing input",
            }
        )
    replay_payload = {
        "status": "INCOMPLETE_REPLAY",
        "as_of": AS_OF.isoformat(),
        "mode": "cache-only",
        "run_id": "test_inventory_2026-07-21",
        "generated_at": "2026-07-24T10:47:06+00:00",
        "visible_at": "2026-07-21T23:59:59.999999+00:00",
        "cutoff_policy": "end_of_asof_utc",
        "inventory_only": True,
        "allow_incomplete": True,
        "label": "test",
        "openai_replay_policy": "disabled",
        "paths": {"root": str(root)},
        "errors": ["required inputs missing"],
        "input_records": input_records,
        "command_results": [],
        "production_diff": None,
    }
    (root / "replay_run.json").write_text(
        json.dumps(replay_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (root / "input_freeze_manifest.json").write_text(
        json.dumps(input_records, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return root


def _sha(path: Path) -> str:
    return sha256(path.read_bytes()).hexdigest()
