from __future__ import annotations

import json
import shutil
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path

import pytest

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.daily_input_capture import (
    build_daily_input_capture_recovery_queue,
    validate_daily_input_capture_recovery_queue,
)
from ai_trading_system.historical_gap_recovery import (
    HistoricalGapRecoveryError,
    build_historical_gap_recovery,
    validate_historical_gap_recovery,
)
from ai_trading_system.platform.artifacts import write_json_atomic

AS_OF = "2026-09-02"
OWNER_DECISION_ID = "owner_decision:OPS-079:2026-09-05:test"


def test_build_and_validate_market_macro_recovery(tmp_path: Path) -> None:
    fixture = _write_project_fixture(tmp_path)
    inventory = _write_market_inventory(tmp_path / "inventory")
    recovery_id = _recovery_id(fixture["queue"], "market_macro")

    result = build_historical_gap_recovery(
        queue_path=fixture["queue"],
        queue_validation_path=fixture["queue_validation"],
        recovery_id=recovery_id,
        owner_decision_id=OWNER_DECISION_ID,
        project_root=fixture["project"],
        guard_paths=[fixture["guard"]],
        inventory_bundle=inventory,
        output_root=tmp_path / "output",
        generated_at=datetime(2026, 9, 5, 6, 0, tzinfo=UTC),
    )

    assert result.validation.passed
    payload = json.loads(result.payload_path.read_text(encoding="utf-8"))
    assert payload["component_id"] == "market_macro"
    assert payload["result_classification"] == "IMMUTABLE_RAW_BACKFILL_EVIDENCE"
    assert payload["evidence"]["primary_exact_date_rows"] == 2
    assert payload["evidence"]["secondary_exact_date_rows"] == 2
    assert payload["evidence"]["rates_exact_date_rows"] == 2
    assert set(payload["consumer_outputs"].values()) == {None}
    assert payload["canonical_history_status"]["unchanged"] is True
    assert payload["safety"]["production_effect"] == "none"

    validation = _validate(
        result.bundle_path,
        fixture=fixture,
        recovery_id=recovery_id,
        inventory=inventory,
    )
    assert validation.passed
    assert "branch_source_and_derived_evidence_recomputed" in validation.checks


def test_build_and_validate_sec_non_pit_review(tmp_path: Path) -> None:
    fixture = _write_project_fixture(tmp_path)
    before, after = _write_sec_capture_pair(fixture["project"])
    recovery_id = _recovery_id(fixture["queue"], "sec_companyfacts")

    result = build_historical_gap_recovery(
        queue_path=fixture["queue"],
        queue_validation_path=fixture["queue_validation"],
        recovery_id=recovery_id,
        owner_decision_id=OWNER_DECISION_ID,
        project_root=fixture["project"],
        guard_paths=[fixture["guard"]],
        sec_before_manifest=before,
        sec_after_manifest=after,
        output_root=tmp_path / "output",
        generated_at=datetime(2026, 9, 5, 6, 0, tzinfo=UTC),
    )

    assert result.validation.passed
    payload = json.loads(result.payload_path.read_text(encoding="utf-8"))
    assert payload["component_id"] == "sec_companyfacts"
    assert payload["result_classification"] == "MANUAL_NON_PIT_RAW_REVIEW"
    assert payload["evidence"]["ticker_count"] == 2
    assert payload["evidence"]["identical_payload_sha256_count"] == 2
    assert payload["evidence"]["contemporaneous_evidence_status"] == "MISSING"
    assert payload["strict_pit_eligible"] is False

    validation = _validate(
        result.bundle_path,
        fixture=fixture,
        recovery_id=recovery_id,
        sec_before=before,
        sec_after=after,
    )
    assert validation.passed


def test_validator_rejects_frozen_source_tamper(tmp_path: Path) -> None:
    fixture = _write_project_fixture(tmp_path)
    inventory = _write_market_inventory(tmp_path / "inventory")
    recovery_id = _recovery_id(fixture["queue"], "market_macro")
    result = _build_market(tmp_path, fixture, inventory, recovery_id)
    frozen = result.bundle_path / "source/market/input/data/raw/prices_daily.csv"
    frozen.write_bytes(frozen.read_bytes() + b"\n")

    validation = _validate(
        result.bundle_path,
        fixture=fixture,
        recovery_id=recovery_id,
        inventory=inventory,
    )

    assert not validation.passed
    assert "HISTORICAL_GAP_HASH_SIZE_DRIFT" in validation.errors[0]


def test_validator_rejects_live_canonical_guard_drift(tmp_path: Path) -> None:
    fixture = _write_project_fixture(tmp_path)
    inventory = _write_market_inventory(tmp_path / "inventory")
    recovery_id = _recovery_id(fixture["queue"], "market_macro")
    result = _build_market(tmp_path, fixture, inventory, recovery_id)
    fixture["guard"].write_text('{"status":"CHANGED"}\n', encoding="utf-8")

    validation = _validate(
        result.bundle_path,
        fixture=fixture,
        recovery_id=recovery_id,
        inventory=inventory,
    )

    assert not validation.passed
    assert "HISTORICAL_GAP_CANONICAL_GUARD_DRIFT" in validation.errors[0]


def test_validator_rejects_unexpected_bundle_member(tmp_path: Path) -> None:
    fixture = _write_project_fixture(tmp_path)
    inventory = _write_market_inventory(tmp_path / "inventory")
    recovery_id = _recovery_id(fixture["queue"], "market_macro")
    result = _build_market(tmp_path, fixture, inventory, recovery_id)
    (result.bundle_path / "unexpected.txt").write_text("not declared\n", encoding="utf-8")

    validation = _validate(
        result.bundle_path,
        fixture=fixture,
        recovery_id=recovery_id,
        inventory=inventory,
    )

    assert not validation.passed
    assert "HISTORICAL_GAP_MEMBER_SET:unexpected.txt" in validation.errors[0]


def test_validator_rejects_forbidden_safety_promotion(tmp_path: Path) -> None:
    fixture = _write_project_fixture(tmp_path)
    inventory = _write_market_inventory(tmp_path / "inventory")
    recovery_id = _recovery_id(fixture["queue"], "market_macro")
    result = _build_market(tmp_path, fixture, inventory, recovery_id)
    payload = json.loads(result.payload_path.read_text(encoding="utf-8"))
    payload["safety"]["production_effect"] = "production"
    write_json_atomic(result.payload_path, payload)

    validation = _validate(
        result.bundle_path,
        fixture=fixture,
        recovery_id=recovery_id,
        inventory=inventory,
    )

    assert not validation.passed
    assert "HISTORICAL_GAP_SAFETY_DRIFT" in validation.errors[0]


def test_builder_rejects_existing_output_and_preserves_first_bundle(tmp_path: Path) -> None:
    fixture = _write_project_fixture(tmp_path)
    inventory = _write_market_inventory(tmp_path / "inventory")
    recovery_id = _recovery_id(fixture["queue"], "market_macro")
    first = _build_market(tmp_path, fixture, inventory, recovery_id)
    first_bytes = first.payload_path.read_bytes()

    with pytest.raises(HistoricalGapRecoveryError, match="HISTORICAL_GAP_OUTPUT_EXISTS"):
        _build_market(tmp_path, fixture, inventory, recovery_id)

    assert first.payload_path.read_bytes() == first_bytes


def test_builder_rejects_queue_item_without_reviewed_execution_contract(
    tmp_path: Path,
) -> None:
    fixture = _write_project_fixture(tmp_path)
    recovery_id = _recovery_id(fixture["queue"], "fmp_forward_pit")

    with pytest.raises(
        HistoricalGapRecoveryError,
        match="HISTORICAL_GAP_COMPONENT_NOT_ALLOWED:fmp_forward_pit",
    ):
        build_historical_gap_recovery(
            queue_path=fixture["queue"],
            queue_validation_path=fixture["queue_validation"],
            recovery_id=recovery_id,
            owner_decision_id=OWNER_DECISION_ID,
            project_root=fixture["project"],
            guard_paths=[fixture["guard"]],
            output_root=tmp_path / "output",
        )


def test_sec_review_rejects_project_path_escape(tmp_path: Path) -> None:
    fixture = _write_project_fixture(tmp_path)
    before, after = _write_sec_capture_pair(fixture["project"])
    manifest = json.loads(before.read_text(encoding="utf-8"))
    manifest["component_results"][0]["artifacts"][0]["path"] = "../AAA_companyfacts.json"
    write_json_atomic(before, manifest)
    recovery_id = _recovery_id(fixture["queue"], "sec_companyfacts")

    with pytest.raises(
        HistoricalGapRecoveryError,
        match="HISTORICAL_GAP_PROJECT_PATH_INVALID",
    ):
        build_historical_gap_recovery(
            queue_path=fixture["queue"],
            queue_validation_path=fixture["queue_validation"],
            recovery_id=recovery_id,
            owner_decision_id=OWNER_DECISION_ID,
            project_root=fixture["project"],
            guard_paths=[fixture["guard"]],
            sec_before_manifest=before,
            sec_after_manifest=after,
            output_root=tmp_path / "output",
        )


def _build_market(
    tmp_path: Path,
    fixture: dict[str, Path],
    inventory: Path,
    recovery_id: str,
):
    return build_historical_gap_recovery(
        queue_path=fixture["queue"],
        queue_validation_path=fixture["queue_validation"],
        recovery_id=recovery_id,
        owner_decision_id=OWNER_DECISION_ID,
        project_root=fixture["project"],
        guard_paths=[fixture["guard"]],
        inventory_bundle=inventory,
        output_root=tmp_path / "output",
        generated_at=datetime(2026, 9, 5, 6, 0, tzinfo=UTC),
    )


def _validate(
    bundle_path: Path,
    *,
    fixture: dict[str, Path],
    recovery_id: str,
    inventory: Path | None = None,
    sec_before: Path | None = None,
    sec_after: Path | None = None,
):
    return validate_historical_gap_recovery(
        bundle_path,
        project_root=fixture["project"],
        expected_queue_path=fixture["queue"],
        expected_queue_validation_path=fixture["queue_validation"],
        expected_recovery_id=recovery_id,
        expected_owner_decision_id=OWNER_DECISION_ID,
        expected_guard_paths=[fixture["guard"]],
        expected_inventory_bundle=inventory,
        expected_sec_before_manifest=sec_before,
        expected_sec_after_manifest=sec_after,
    )


def _write_project_fixture(tmp_path: Path) -> dict[str, Path]:
    project = tmp_path / "project"
    capture_policy = project / "config/operations/daily_input_capture.yaml"
    recovery_policy = project / "config/operations/historical_gap_recovery.yaml"
    schema = project / "docs/schema/historical_gap_recovery.v1.schema.json"
    for source, destination in (
        (PROJECT_ROOT / "config/operations/daily_input_capture.yaml", capture_policy),
        (PROJECT_ROOT / "config/operations/historical_gap_recovery.yaml", recovery_policy),
        (PROJECT_ROOT / "docs/schema/historical_gap_recovery.v1.schema.json", schema),
    ):
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, destination)
    guard = project / "outputs/daily_input_capture/daily_input_capture_gap_ledger.json"
    guard.parent.mkdir(parents=True, exist_ok=True)
    guard.write_text('{"status":"GAPS_PRESENT"}\n', encoding="utf-8")
    gap_ledger = {
        "schema_version": "daily_input_capture_gap_ledger.v1",
        "as_of": AS_OF,
        "sessions": [
            {
                "session_date": AS_OF,
                "status": "MISSED",
                "manifest_path": None,
            }
        ],
    }
    queue_payload = build_daily_input_capture_recovery_queue(
        gap_ledger=gap_ledger,
        project_root=project,
        policy_path=capture_policy,
    )
    queue = project / "outputs/daily_input_capture/recovery_queue.json"
    write_json_atomic(queue, queue_payload)
    validation_payload = validate_daily_input_capture_recovery_queue(
        queue,
        project_root=project,
        policy_path=capture_policy,
    )
    queue_validation = project / "outputs/daily_input_capture/recovery_queue_validation.json"
    write_json_atomic(queue_validation, validation_payload)
    assert validation_payload["status"] == "PASS"
    return {
        "project": project,
        "queue": queue,
        "queue_validation": queue_validation,
        "guard": guard,
    }


def _recovery_id(queue_path: Path, component_id: str) -> str:
    queue = json.loads(queue_path.read_text(encoding="utf-8"))
    return next(
        str(item["recovery_id"]) for item in queue["items"] if item["component_id"] == component_id
    )


def _write_market_inventory(root: Path) -> Path:
    raw = root / "input/data/raw"
    raw.mkdir(parents=True)
    contents = {
        "prices_daily": (
            "date,ticker,close\n"
            "2026-09-01,AAA,99\n"
            "2026-09-02,AAA,100\n"
            "2026-09-02,BBB,200\n"
        ),
        "prices_marketstack_daily": (
            "date,ticker,close\n" "2026-09-02,AAA,100\n" "2026-09-02,BBB,200\n"
        ),
        "rates_daily": ("date,series,value\n" "2026-09-02,DGS2,3.10\n" "2026-09-02,DGS10,3.70\n"),
        "download_manifest": "source_id,status\nfixture,PASS\n",
    }
    paths: dict[str, Path] = {}
    for artifact_id, content in contents.items():
        filename = {
            "prices_daily": "prices_daily.csv",
            "prices_marketstack_daily": "prices_marketstack_daily.csv",
            "rates_daily": "rates_daily.csv",
            "download_manifest": "download_manifest.csv",
        }[artifact_id]
        path = raw / filename
        path.write_text(content, encoding="utf-8")
        paths[artifact_id] = path
    records = [
        {
            "artifact_id": artifact_id,
            "status": "PASS",
            "sha256": _sha(path),
            "size_bytes": path.stat().st_size,
        }
        for artifact_id, path in paths.items()
    ]
    write_json_atomic(root / "input_freeze_manifest.json", records)
    write_json_atomic(
        root / "replay_run.json",
        {
            "status": "INCOMPLETE_REPLAY",
            "as_of": AS_OF,
            "mode": "cache-only",
            "inventory_only": True,
            "run_id": "historical_gap_inventory_test",
            "visible_at": "2026-09-02T23:59:59.999999+00:00",
            "cutoff_policy": "end_of_asof_utc",
            "input_records": records,
        },
    )
    return root


def _write_sec_capture_pair(project: Path) -> tuple[Path, Path]:
    before_records = _write_sec_payloads(project, "2026-09-01")
    after_records = _write_sec_payloads(project, "2026-09-03")
    before = project / "outputs/daily_input_capture/2026-09-01/manifest.json"
    after = project / "outputs/daily_input_capture/2026-09-03/manifest.json"
    _write_sec_manifest(
        before,
        as_of="2026-09-01",
        captured_at="2026-09-01T23:00:00+00:00",
        records=before_records,
    )
    _write_sec_manifest(
        after,
        as_of="2026-09-03",
        captured_at="2026-09-03T23:00:00+00:00",
        records=after_records,
    )
    return before, after


def _write_sec_payloads(project: Path, as_of: str) -> list[dict[str, object]]:
    result: list[dict[str, object]] = []
    for ticker in ("AAA", "BBB"):
        path = (
            project
            / "data/raw/daily_input_capture"
            / as_of
            / "sec_companyfacts"
            / f"{ticker}_companyfacts.json"
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps({"ticker": ticker, "facts": {"value": 1}}, sort_keys=True),
            encoding="utf-8",
        )
        result.append(
            {
                "path": path.relative_to(project).as_posix(),
                "sha256": _sha(path),
                "size_bytes": path.stat().st_size,
            }
        )
    return result


def _write_sec_manifest(
    path: Path,
    *,
    as_of: str,
    captured_at: str,
    records: list[dict[str, object]],
) -> None:
    write_json_atomic(
        path,
        {
            "as_of": as_of,
            "captured_at": captured_at,
            "component_results": [
                {
                    "component_id": "sec_companyfacts",
                    "status": "PASS",
                    "recovery_mode": "MANUAL_NON_PIT_RAW_REVIEW",
                    "artifacts": records,
                }
            ],
        },
    )


def _sha(path: Path) -> str:
    return sha256(path.read_bytes()).hexdigest()
