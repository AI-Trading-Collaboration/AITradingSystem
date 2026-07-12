from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from dynamic_v3_outcome_loop_helpers import (
    run_rolling_refresh_fixture,
    run_safe_update_fixture,
)

from ai_trading_system.etf_portfolio import dynamic_v3_outcome_accumulation as accumulation


def test_rolling_evidence_refresh_records_downstream_ids_and_delta(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    result = run_rolling_refresh_fixture(tmp_path, monkeypatch)["refresh"]
    refreshed = result["refreshed_artifacts"]
    delta = result["evidence_delta_summary"]

    assert refreshed["outcome_dashboard_id"]
    assert refreshed["limited_vs_notrade_id"]
    assert refreshed["consensus_risk_id"]
    assert refreshed["owner_attribution_id"]
    assert refreshed["shadow_aging_id"]
    assert refreshed["weekly_advisory_review_id"]
    assert refreshed["reader_brief_section_generated"] is True
    assert refreshed["reader_brief_updated"] is False
    assert refreshed["all_downstream_validations_passed"] is True
    assert delta["before"]["forward_available"] == 0
    assert delta["after"]["forward_available"] == 1
    assert delta["after"]["limited_vs_notrade_available_count"] == 1
    assert delta["after"]["limited_vs_notrade_confidence"] == "LOW"
    assert delta["after"]["consensus_target_risk"] == "INSUFFICIENT_DATA"
    assert delta["material_change"] is True
    assert (
        accumulation.validate_rolling_evidence_refresh_artifact(
            refresh_id=result["refresh_id"],
            output_dir=tmp_path / "rolling_evidence_refresh",
        )["status"]
        == "PASS"
    )


def test_rolling_evidence_refresh_requires_valid_explicit_update(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    fixture = run_safe_update_fixture(tmp_path, monkeypatch)
    update_dir = tmp_path / "outcome_update" / fixture["update"]["outcome_update_id"]
    manifest_path = update_dir / "outcome_update_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["status"] = "TAMPERED"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(accumulation.DynamicV3OutcomeAccumulationError):
        accumulation.run_rolling_evidence_refresh(
            outcome_update_id=fixture["update"]["outcome_update_id"],
            output_dir=tmp_path / "rolling_evidence_refresh",
            outcome_update_dir=tmp_path / "outcome_update",
            generated_at=datetime(2026, 6, 10, tzinfo=UTC),
        )

    assert not (tmp_path / "rolling_evidence_refresh").exists()


def test_rolling_evidence_refresh_is_single_use(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    fixture = run_rolling_refresh_fixture(tmp_path, monkeypatch)

    with pytest.raises(
        accumulation.DynamicV3OutcomeAccumulationError,
        match="already has a COMMITTED",
    ):
        accumulation.run_rolling_evidence_refresh(
            outcome_update_id=fixture["update"]["outcome_update_id"],
            output_dir=tmp_path / "rolling_evidence_refresh",
            outcome_update_dir=tmp_path / "outcome_update",
            generated_at=datetime(2026, 6, 11, tzinfo=UTC),
        )


def test_rolling_evidence_refresh_rolls_back_partial_downstream_outputs(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    fixture = run_safe_update_fixture(tmp_path, monkeypatch)

    def fail_consensus(**_: Any) -> dict[str, Any]:
        raise accumulation.DynamicV3OutcomeAccumulationError("forced consensus failure")

    monkeypatch.setattr(accumulation, "run_consensus_risk_review", fail_consensus)
    with pytest.raises(
        accumulation.DynamicV3OutcomeAccumulationError,
        match="forced consensus failure",
    ):
        accumulation.run_rolling_evidence_refresh(
            outcome_update_id=fixture["update"]["outcome_update_id"],
            output_dir=tmp_path / "rolling_evidence_refresh",
            outcome_update_dir=tmp_path / "outcome_update",
            outcome_dashboard_dir=tmp_path / "outcome_dashboard",
            limited_vs_notrade_dir=tmp_path / "limited_vs_notrade",
            consensus_risk_dir=tmp_path / "consensus_risk",
            advisory_outcome_dir=tmp_path / "advisory_outcome",
            backfill_dir=tmp_path / "backfilled_outcome",
            repair_dir=tmp_path / "backfill_repair",
            paper_sim_dir=tmp_path / "historical_paper_sim",
            diagnosis_dir=tmp_path / "replay_diagnosis",
            outcome_due_dir=tmp_path / "outcome_due",
            generated_at=datetime(2026, 6, 10, tzinfo=UTC),
        )

    assert not list((tmp_path / "outcome_dashboard").glob("*/"))
    assert not list((tmp_path / "limited_vs_notrade").glob("*/"))
    refresh_dirs = list((tmp_path / "rolling_evidence_refresh").glob("*/"))
    assert len(refresh_dirs) == 1
    transaction = json.loads(
        (refresh_dirs[0] / "rolling_refresh_transaction.json").read_text(encoding="utf-8")
    )
    assert transaction["status"] == "ROLLED_BACK"
    assert transaction["rollback_validation"]["status"] == "PASS"


@pytest.mark.parametrize(
    ("target", "mutate"),
    [
        (
            "evidence_delta_summary.json",
            lambda payload: {**payload, "material_change": not payload["material_change"]},
        ),
        (
            "rolling_refresh_source_snapshot.json",
            lambda payload: {**payload, "outcome_update_id": "tampered"},
        ),
        (
            "rolling_refresh_transaction.json",
            lambda payload: {**payload, "status": "PREPARED"},
        ),
    ],
)
def test_rolling_evidence_refresh_validator_rejects_view_snapshot_and_transaction_tamper(
    tmp_path: Path,
    monkeypatch: Any,
    target: str,
    mutate: Any,
) -> None:
    result = run_rolling_refresh_fixture(tmp_path, monkeypatch)["refresh"]
    path = result["refresh_dir"] / target
    payload = json.loads(path.read_text(encoding="utf-8"))
    path.write_text(json.dumps(mutate(payload)), encoding="utf-8")

    validation = accumulation.validate_rolling_evidence_refresh_artifact(
        refresh_id=result["refresh_id"],
        output_dir=tmp_path / "rolling_evidence_refresh",
    )
    assert validation["status"] == "FAIL"


def test_rolling_evidence_refresh_validator_rejects_live_downstream_tamper(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    result = run_rolling_refresh_fixture(tmp_path, monkeypatch)["refresh"]
    dashboard_id = result["refreshed_artifacts"]["outcome_dashboard_id"]
    report_path = (
        tmp_path
        / "outcome_dashboard"
        / dashboard_id
        / "outcome_dashboard_report.md"
    )
    report_path.write_text(report_path.read_text(encoding="utf-8") + "tampered\n", encoding="utf-8")

    validation = accumulation.validate_rolling_evidence_refresh_artifact(
        refresh_id=result["refresh_id"],
        output_dir=tmp_path / "rolling_evidence_refresh",
    )
    assert validation["status"] == "FAIL"
