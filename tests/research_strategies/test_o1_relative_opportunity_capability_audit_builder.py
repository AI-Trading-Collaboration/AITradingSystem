from __future__ import annotations

import copy
import hashlib
import shutil
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest

from ai_trading_system.research_framework.plugins.o1_relative_opportunity_capability_audit import (
    DATA_ROLE,
    READY_STATUS,
    O1SyntheticValidationError,
    build_synthetic_capability_dataset,
    validate_synthetic_capability_dataset,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
AUDIT_POLICY_PATH = (
    PROJECT_ROOT / "config/research/o1_relative_opportunity_capability_audit_v1.yaml"
)
HISTORICAL_MODEL_POLICY_PATH = (
    PROJECT_ROOT / "config/research/decision_target_capability_audit_model_ladder_v1.yaml"
)
SOURCE_COMMIT_SHA = "b346aaa622de1c9671527fda4b89b84f2c08ac83"


def test_synthetic_builder_is_deterministic_and_independently_reconstructs() -> None:
    panel = _synthetic_panel()
    events = _synthetic_events(panel)

    first = _build(panel=panel, events=events)
    second = _build(panel=panel.copy(), events=copy.deepcopy(events))

    assert first == second
    assert first["status"] == READY_STATUS
    assert first["data_role"] == DATA_ROLE
    assert first["dataset_contract"]["feature_count"] == 28
    assert first["dataset_contract"]["model_id"] == "M1_RIDGE_LINEAR"
    assert len(first["rows"]) > 700
    assert len(first["fold_ledger"]) >= 3
    assert {row["event_family"] for row in first["event_episodes"]} == {
        "FOMC",
        "CPI",
        "NFP",
    }
    assert first["safety"]["synthetic_fixture_only"] is True
    assert first["safety"]["real_data_accessed"] is False
    assert first["safety"]["real_coverage_read"] is False
    assert first["safety"]["model_training_executed"] is False
    assert first["safety"]["predictions_generated"] is False
    assert "predictions" not in first
    assert "metrics" not in first
    assert _validate(first, panel=panel, events=events) == ()


def test_target_feature_and_dataset_tamper_fail_independent_validation() -> None:
    panel = _synthetic_panel()
    events = _synthetic_events(panel)
    payload = _build(panel=panel, events=events)

    target_tamper = copy.deepcopy(payload)
    target_tamper["rows"][0]["target_value"] += 0.25
    target_errors = _validate(target_tamper, panel=panel, events=events)
    assert "DATASET_ROWS_MISMATCH" in target_errors
    assert "INDEPENDENT_TARGET_RECONSTRUCTION_MISMATCH" in target_errors

    feature_tamper = copy.deepcopy(payload)
    feature_id = feature_tamper["dataset_contract"]["feature_ids"][0]
    feature_tamper["rows"][0]["features"][feature_id] += 0.25
    feature_errors = _validate(feature_tamper, panel=panel, events=events)
    assert "DATASET_ROWS_MISMATCH" in feature_errors
    assert "INDEPENDENT_FEATURE_RECONSTRUCTION_MISMATCH" in feature_errors

    commitment_tamper = copy.deepcopy(payload)
    commitment_tamper["dataset_commitment_sha256"] = "0" * 64
    assert "DATASET_COMMITMENT_MISMATCH" in _validate(
        commitment_tamper,
        panel=panel,
        events=events,
    )


def test_fold_and_event_tamper_fail_closed() -> None:
    panel = _synthetic_panel()
    events = _synthetic_events(panel)
    payload = _build(panel=panel, events=events)

    fold_tamper = copy.deepcopy(payload)
    fold_tamper["fold_ledger"][0]["test_decision_dates"].reverse()
    assert "FOLD_LEDGER_MISMATCH" in _validate(
        fold_tamper,
        panel=panel,
        events=events,
    )

    event_tamper = copy.deepcopy(payload)
    event_tamper["event_episodes"][0]["window_end_session"] = event_tamper[
        "event_episodes"
    ][0]["anchor_session"]
    assert "EVENT_EPISODES_MISMATCH" in _validate(
        event_tamper,
        panel=panel,
        events=events,
    )

    safety_tamper = copy.deepcopy(payload)
    safety_tamper["safety"]["model_training_executed"] = True
    assert "SAFETY_BOUNDARY_MISMATCH" in _validate(
        safety_tamper,
        panel=panel,
        events=events,
    )


def test_synthetic_source_and_authority_negative_cases_fail_closed(
    tmp_path: Path,
) -> None:
    panel = _synthetic_panel()
    events = _synthetic_events(panel)

    with pytest.raises(
        O1SyntheticValidationError,
        match="REAL_DATA_ROLE_FORBIDDEN_IN_SYNTHETIC_STAGE",
    ):
        _build(panel=panel, events=events, data_role="REAL_CANONICAL_DATA")

    duplicate_panel = pd.concat([panel, panel.iloc[[0]]], ignore_index=True)
    with pytest.raises(
        O1SyntheticValidationError,
        match="SYNTHETIC_PRICE_DUPLICATE_KEY",
    ):
        _build(panel=duplicate_panel, events=events)

    incomplete_event = copy.deepcopy(events)
    incomplete_event[0].pop("known_at")
    with pytest.raises(
        O1SyntheticValidationError,
        match="SYNTHETIC_EVENT_FIELDS_INVALID",
    ):
        _build(panel=panel, events=incomplete_event)

    drifted_model_policy = tmp_path / "historical-model-policy.yaml"
    shutil.copyfile(HISTORICAL_MODEL_POLICY_PATH, drifted_model_policy)
    drifted_model_policy.write_bytes(drifted_model_policy.read_bytes() + b"\n")
    with pytest.raises(
        O1SyntheticValidationError,
        match="HISTORICAL_MODEL_POLICY_SHA256_MISMATCH",
    ):
        _build(
            panel=panel,
            events=events,
            historical_model_policy_path=drifted_model_policy,
        )


def test_fold_membership_enforces_maturity_purge_and_embargo() -> None:
    panel = _synthetic_panel()
    events = _synthetic_events(panel)
    payload = _build(panel=panel, events=events)
    rows = {row["decision_date"]: row for row in payload["rows"]}

    for fold in payload["fold_ledger"]:
        assert fold["train_row_count"] == len(fold["train_decision_dates"])
        assert fold["test_row_count"] == len(fold["test_decision_dates"])
        assert fold["test_decision_dates"] == sorted(fold["test_decision_dates"])
        assert not set(fold["train_decision_dates"]) & set(
            fold["test_decision_dates"]
        )
        for decision_date in fold["train_decision_dates"]:
            row = rows[decision_date]
            assert row["label_available_on_session"] <= fold["train_cutoff"]
            assert not _overlap(
                row["label_interval_start"],
                row["label_interval_end"],
                fold["test_start"],
                fold["test_end"],
            )
            assert not _overlap(
                row["label_interval_start"],
                row["label_interval_end"],
                fold["embargo_start"],
                fold["embargo_end"],
            )


def _build(
    *,
    panel: pd.DataFrame,
    events: list[dict[str, Any]],
    data_role: str = DATA_ROLE,
    historical_model_policy_path: Path = HISTORICAL_MODEL_POLICY_PATH,
) -> dict[str, Any]:
    return build_synthetic_capability_dataset(
        audit_policy_path=AUDIT_POLICY_PATH,
        historical_model_policy_path=historical_model_policy_path,
        price_panel=panel,
        event_ledger=events,
        source_commit_sha=SOURCE_COMMIT_SHA,
        data_role=data_role,
    )


def _validate(
    payload: dict[str, Any],
    *,
    panel: pd.DataFrame,
    events: list[dict[str, Any]],
) -> tuple[str, ...]:
    return validate_synthetic_capability_dataset(
        payload,
        audit_policy_path=AUDIT_POLICY_PATH,
        historical_model_policy_path=HISTORICAL_MODEL_POLICY_PATH,
        price_panel=panel,
        event_ledger=events,
        source_commit_sha=SOURCE_COMMIT_SHA,
        data_role=DATA_ROLE,
    )


def _synthetic_panel() -> pd.DataFrame:
    sessions = pd.bdate_range("2020-08-03", periods=980)
    index = np.arange(len(sessions), dtype=float)
    prices = {
        "QQQ": 100.0
        * np.exp(0.00045 * index + 0.018 * np.sin(index / 17.0)),
        "SGOV": 100.0
        * np.exp(0.00010 * index + 0.0004 * np.sin(index / 29.0)),
        "SPY": 100.0
        * np.exp(0.00032 * index + 0.012 * np.sin(index / 23.0)),
    }
    rows: list[dict[str, Any]] = []
    for position, session in enumerate(sessions):
        for ticker in ("QQQ", "SGOV", "SPY"):
            rows.append(
                {
                    "date": session.date().isoformat(),
                    "ticker": ticker,
                    "adj_close": float(prices[ticker][position]),
                }
            )
    return pd.DataFrame(rows, columns=["date", "ticker", "adj_close"])


def _synthetic_events(panel: pd.DataFrame) -> list[dict[str, Any]]:
    sessions = sorted(panel["date"].unique())
    schedule = (
        (260, "FOMC"),
        (320, "CPI"),
        (380, "NFP"),
        (500, "FOMC"),
        (560, "CPI"),
        (620, "NFP"),
    )
    rows: list[dict[str, Any]] = []
    for sequence, (position, family) in enumerate(schedule, start=1):
        event_id = f"{family}-{sequence:02d}"
        event_time = datetime.fromisoformat(
            f"{sessions[position]}T14:00:00+00:00"
        ).astimezone(UTC)
        rows.append(
            {
                "event_id": event_id,
                "event_family": family,
                "event_timestamp": event_time.isoformat(),
                "source_published_time": event_time.isoformat(),
                "known_at": (event_time + timedelta(minutes=1)).isoformat(),
                "available_at": (event_time + timedelta(minutes=2)).isoformat(),
                "provider_name": "synthetic_primary_source",
                "endpoint_or_file": f"synthetic://{family.lower()}/{event_id}",
                "request_parameters": {"fixture": True, "event_id": event_id},
                "download_timestamp": (event_time + timedelta(days=1)).isoformat(),
                "checksum": hashlib.sha256(event_id.encode()).hexdigest(),
            }
        )
    return sorted(
        rows,
        key=lambda row: (
            row["event_timestamp"],
            row["event_family"],
            row["event_id"],
        ),
    )


def _overlap(
    left_start: str,
    left_end: str,
    right_start: str,
    right_end: str,
) -> bool:
    return left_start <= right_end and right_start <= left_end
