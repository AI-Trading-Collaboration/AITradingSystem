from __future__ import annotations

import socket
from datetime import UTC, datetime

import pytest

from ai_trading_system.breadth_participation_feasibility_audit import (
    BreadthParticipationFeasibilityAuditError,
    build_breadth_participation_feasibility_artifacts,
    run_breadth_participation_feasibility_audit,
)


def test_loader_constructs_target_etf_inventory() -> None:
    artifacts = build_breadth_participation_feasibility_artifacts(
        target_etfs=["QQQ", "SPY", "SMH"],
        target_assets=["QQQ", "SPY", "SMH"],
        candidate_family="breadth_participation",
        generated_at=datetime(2026, 7, 1, tzinfo=UTC),
    )

    rows = artifacts["data_inventory"]["rows"]
    assert {row["target_etf"] for row in rows if row["target_etf"] != "ALL"} >= {
        "QQQ",
        "SPY",
        "SMH",
    }
    assert artifacts["summary"]["candidate_family"] == "breadth_participation"


def test_loader_fails_closed_when_required_config_is_missing(tmp_path) -> None:
    with pytest.raises(BreadthParticipationFeasibilityAuditError):
        run_breadth_participation_feasibility_audit(
            target_etfs="",
            target_assets="QQQ,SPY,SMH",
            candidate_family="breadth_participation",
            output_dir=tmp_path,
            mode="feasibility_audit",
        )


def test_loader_does_not_call_external_network(monkeypatch) -> None:
    def blocked_socket(*args, **kwargs):  # type: ignore[no-untyped-def]
        raise AssertionError("network access is not allowed")

    monkeypatch.setattr(socket, "socket", blocked_socket)

    artifacts = build_breadth_participation_feasibility_artifacts(
        target_etfs=["QQQ", "SPY", "SMH"],
        target_assets=["QQQ", "SPY", "SMH"],
        candidate_family="breadth_participation",
        generated_at=datetime(2026, 7, 1, tzinfo=UTC),
    )

    assert artifacts["summary"]["data_quality_status"] == "NOT_APPLICABLE_STATIC_FEASIBILITY_AUDIT"


def test_loader_does_not_generate_candidate_artifacts() -> None:
    artifacts = build_breadth_participation_feasibility_artifacts(
        target_etfs=["QQQ", "SPY", "SMH"],
        target_assets=["QQQ", "SPY", "SMH"],
        candidate_family="breadth_participation",
        generated_at=datetime(2026, 7, 1, tzinfo=UTC),
    )
    summary = artifacts["summary"]

    assert summary["generator_implemented"] is False
    assert summary["candidate_artifact_generated"] is False
    assert summary["candidate_signal_series_generated"] is False
    assert summary["actual_path_validation_executed"] is False
    assert summary["promotion_allowed"] is False
    assert summary["paper_shadow_allowed"] is False
    assert summary["production_allowed"] is False
    assert summary["broker_action"] == "none"
