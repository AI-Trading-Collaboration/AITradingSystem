from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from dynamic_v3_system_target_helpers import run_smoothed_forward_ops_chain_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target
from ai_trading_system.etf_portfolio import (
    dynamic_v3_system_target_smoothed_promotion as smoothed_promotion,
)


def _bundles(value: Any) -> list[dict[str, Any]]:
    found: list[dict[str, Any]] = []
    if isinstance(value, dict):
        bundle = value.get("bundle")
        if isinstance(bundle, dict):
            found.append(bundle)
        for nested in value.values():
            found.extend(_bundles(nested))
    elif isinstance(value, list):
        for nested in value:
            found.extend(_bundles(nested))
    return found


@smoothed_promotion._with_validation_session
def test_operations_chain_is_bounded_and_tamper_fail_closed(tmp_path: Path) -> None:
    fixture = run_smoothed_forward_ops_chain_fixture(tmp_path)
    artifacts = [
        (
            fixture["progress"],
            "smoothed_forward_progress_input_snapshot.json",
            "smoothed_forward_progress_input_snapshot.v2",
            system_target.validate_smoothed_forward_progress_artifact,
            "progress_id",
            tmp_path / "smoothed_forward_progress",
        ),
        (
            fixture["dashboard"],
            "smoothed_weekly_dashboard_input_snapshot.json",
            "smoothed_weekly_dashboard_input_snapshot.v2",
            system_target.validate_smoothed_weekly_dashboard_artifact,
            "dashboard_id",
            tmp_path / "smoothed_weekly_dashboard",
        ),
        (
            fixture["monitor"],
            "smoothed_event_monitor_input_snapshot.json",
            "smoothed_event_monitor_input_snapshot.v2",
            system_target.validate_smoothed_event_monitor_artifact,
            "monitor_id",
            tmp_path / "smoothed_event_monitor",
        ),
        (
            fixture["recheck"],
            "smoothed_switch_readiness_input_snapshot.json",
            "smoothed_switch_readiness_input_snapshot.v2",
            system_target.validate_smoothed_switch_readiness_artifact,
            "recheck_id",
            tmp_path / "smoothed_switch_readiness",
        ),
        (
            fixture["renewal"],
            "smoothed_owner_renewal_input_snapshot.json",
            "smoothed_owner_renewal_input_snapshot.v2",
            system_target.validate_smoothed_owner_renewal_artifact,
            "renewal_id",
            tmp_path / "smoothed_owner_renewal",
        ),
    ]

    snapshot_paths: list[Path] = []
    for payload, snapshot_name, schema, validator, validator_key, output_dir in artifacts:
        artifact_id = str(payload[validator_key])
        snapshot_path = output_dir / artifact_id / snapshot_name
        snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
        snapshot_paths.append(snapshot_path)
        assert snapshot["schema_version"] == schema
        assert validator(**{validator_key: artifact_id, "output_dir": output_dir})[
            "status"
        ] == "PASS"
        for bundle in _bundles(snapshot):
            assert not any(
                name.endswith("_input_snapshot.json")
                for name in dict(bundle.get("json", {}))
            )

    assert max(path.stat().st_size for path in snapshot_paths) < 20 * 1024 * 1024

    progress_root = Path(fixture["progress"]["progress_dir"])
    progress_summary = progress_root / "smoothed_forward_progress_summary.json"
    original_progress = progress_summary.read_bytes()
    changed = json.loads(original_progress)
    changed["summary_recommendation"] = "tampered"
    progress_summary.write_text(json.dumps(changed), encoding="utf-8")
    dashboard_validation = system_target.validate_smoothed_weekly_dashboard_artifact(
        dashboard_id=fixture["dashboard"]["dashboard_id"],
        output_dir=tmp_path / "smoothed_weekly_dashboard",
    )
    assert dashboard_validation["status"] == "FAIL"
    progress_summary.write_bytes(original_progress)

    renewal_root = Path(fixture["renewal"]["renewal_dir"])
    reader = renewal_root / "reader_brief_section.md"
    original_reader = reader.read_bytes()
    reader.write_bytes(original_reader + b"\ntampered\n")
    renewal_validation = system_target.validate_smoothed_owner_renewal_artifact(
        renewal_id=fixture["renewal"]["renewal_id"],
        output_dir=tmp_path / "smoothed_owner_renewal",
    )
    assert renewal_validation["status"] == "FAIL"
    reader.write_bytes(original_reader)

    restored = system_target.validate_smoothed_owner_renewal_artifact(
        renewal_id=fixture["renewal"]["renewal_id"],
        output_dir=tmp_path / "smoothed_owner_renewal",
    )
    assert restored["status"] == "PASS"
