from __future__ import annotations

import shutil
from datetime import date, timedelta
from pathlib import Path

import pytest

from ai_trading_system import first_layer_composer_v2_prospective_oos as prospective


def _contract() -> prospective.ActivatedObservationContract:
    return prospective.activate_contract(
        prospective.load_preregistration(),
        freeze_commit="a" * 40,
        prospective_start=date(2026, 9, 8),
    )


def _identities(seed: str = "1") -> dict[str, str]:
    return {key: seed * 64 for key in prospective.REQUIRED_IDENTITIES}


def _observation(decision: date = date(2026, 9, 8), *, seed: str = "1") -> dict[str, object]:
    return prospective.create_observation(
        _contract(),
        decision_date=decision,
        trend_state="constructive",
        identities=_identities(seed),
        dq_status="PASS",
    )


def test_preregistration_is_result_blind_and_binds_terminal_evidence() -> None:
    loaded = prospective.load_preregistration()

    assert loaded.payload["policy_status"] == "RESULT_BLIND_CONTRACT_NOT_YET_CAPTURING"
    assert loaded.payload["known_result_boundary"]["current_verdict"] == "INSUFFICIENT_HOLD"
    assert loaded.payload["freeze_contract"]["prospective_start_date"] is None
    assert loaded.payload["run_envelope"] == prospective.ZERO_RUN_ENVELOPE
    assert len(loaded.file_sha256) == len(loaded.canonical_sha256) == 64


def test_loader_rejects_result_blind_boundary_tamper(tmp_path: Path) -> None:
    policy = prospective.DEFAULT_POLICY_PATH.read_text(encoding="utf-8")
    target = tmp_path / prospective.DEFAULT_POLICY_PATH
    target.parent.mkdir(parents=True)
    target.write_text(
        policy.replace(
            "prospective_outcome_accessed_by_this_contract: false",
            "prospective_outcome_accessed_by_this_contract: true",
        ),
        encoding="utf-8",
    )
    for relative in (
        Path("config/research/first_layer_composer_v2.yaml"),
        Path(
            "config/research/first_layer_composer_v2_foundational_"
            "falsification_preregistration_v1.yaml"
        ),
        Path(
            "config/research/first_layer_composer_v2_foundational_"
            "falsification_failure_fix_result_admission_v1.yaml"
        ),
        Path("config/research/first_layer_composer_v2_matched_placebo_" "result_admission_v1.yaml"),
        Path(
            "config/research/first_layer_composer_v2_temporal_influence_"
            "failure_fix_result_admission_v1.yaml"
        ),
        Path("config/research/first_layer_operational_forecast_producer_v1.yaml"),
        Path("src/ai_trading_system/first_layer_operational_forecast.py"),
    ):
        destination = tmp_path / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(relative, destination)

    with pytest.raises(
        prospective.ProspectiveObservationError,
        match="PROSPECTIVE_IDENTITY_MISMATCH",
    ):
        prospective.load_preregistration(prospective.DEFAULT_POLICY_PATH, project_root=tmp_path)


def test_activation_and_observation_forbid_backfill_and_non_pass_dq() -> None:
    policy = prospective.load_preregistration()
    with pytest.raises(
        prospective.ProspectiveObservationError, match="PROSPECTIVE_BACKFILL_FORBIDDEN"
    ):
        prospective.activate_contract(
            policy, freeze_commit="a" * 40, prospective_start=date(2025, 12, 2)
        )

    contract = _contract()
    with pytest.raises(
        prospective.ProspectiveObservationError, match="PROSPECTIVE_BACKFILL_FORBIDDEN"
    ):
        prospective.create_observation(
            contract,
            decision_date=date(2026, 9, 7),
            trend_state="neutral",
            identities=_identities(),
            dq_status="PASS",
        )
    with pytest.raises(prospective.ProspectiveObservationError, match="PROSPECTIVE_DQ_NOT_PASS"):
        prospective.create_observation(
            contract,
            decision_date=date(2026, 9, 8),
            trend_state="neutral",
            identities=_identities(),
            dq_status="WARN",
        )


def test_frozen_operational_producer_is_not_misrepresented_as_prospective_ready() -> None:
    readiness = prospective.audit_frozen_producer_readiness()

    assert readiness["status"] == "PRODUCER_NOT_READY"
    assert readiness["reason_codes"] == [
        "FROZEN_OPERATIONAL_PRODUCER_WINDOW_ENDS_AT_HISTORICAL_CUTOFF",
        "PROSPECTIVE_START_NOT_FROZEN",
    ]
    assert readiness["producer_evaluation_end"] == "2025-12-02"
    assert readiness["can_emit_post_historical_cutoff_session"] is False
    assert readiness["recommended_solution"] == (
        "NEW_VERSIONED_PROSPECTIVE_SINGLE_SESSION_PRODUCER_USING_MATURE_LABELS_ONLY"
    )
    assert readiness["temporary_workaround_allowed"] is False
    assert readiness["market_data_read"] is False


def test_state_mapping_and_identity_hashes_are_frozen() -> None:
    contract = _contract()
    long = prospective.create_observation(
        contract,
        decision_date=date(2026, 9, 8),
        trend_state="risk_on",
        identities=_identities(),
        dq_status="PASS",
    )
    flat = prospective.create_observation(
        contract,
        decision_date=date(2026, 9, 9),
        trend_state="defensive",
        identities=_identities("2"),
        dq_status="PASS",
    )

    assert long["action"] == "LONG_QQQ"
    assert flat["action"] == "FLAT_CASH"
    with pytest.raises(prospective.ProspectiveObservationError, match="PROSPECTIVE_SHA256_INVALID"):
        prospective.create_observation(
            contract,
            decision_date=date(2026, 9, 10),
            trend_state="neutral",
            identities={**_identities(), "model_sha256": "short"},
            dq_status="PASS",
        )


def test_append_is_idempotent_and_rejects_same_date_drift() -> None:
    ledger = prospective.new_ledger(_contract())
    observation = _observation()

    ledger, status = prospective.append_observation(ledger, observation)
    assert status == "OBSERVATION_WRITTEN"
    unchanged, status = prospective.append_observation(ledger, observation)
    assert status == "OBSERVATION_ALREADY_EXISTS"
    assert unchanged == ledger

    with pytest.raises(
        prospective.ProspectiveObservationError,
        match="PROSPECTIVE_SAME_DATE_IDENTITY_DRIFT",
    ):
        prospective.append_observation(ledger, _observation(seed="2"))


def test_append_rejects_insertion_before_latest_observation() -> None:
    ledger = prospective.new_ledger(_contract())
    ledger, _ = prospective.append_observation(ledger, _observation(date(2026, 9, 10)))

    with pytest.raises(
        prospective.ProspectiveObservationError, match="PROSPECTIVE_NON_APPEND_WRITE"
    ):
        prospective.append_observation(ledger, _observation(date(2026, 9, 9), seed="2"))


def test_maturity_requires_complete_session_horizon_and_is_immutable() -> None:
    decision = date(2026, 9, 8)
    ledger = prospective.new_ledger(_contract())
    ledger, _ = prospective.append_observation(ledger, _observation(decision))
    sessions = tuple(decision + timedelta(days=index) for index in range(21))
    metrics = {
        "candidate_net_return_pct": 2.0,
        "comparator_net_return_pct": 1.25,
        "paired_excess_percentage_points": 0.75,
        "one_way_cost_bps": 5.0,
    }

    with pytest.raises(
        prospective.ProspectiveObservationError, match="PROSPECTIVE_OUTCOME_IMMATURE"
    ):
        prospective.append_matured_outcomes(
            ledger,
            decision_date=decision,
            as_of_date=sessions[4],
            session_dates=sessions,
            outcomes={5: metrics},
        )

    ledger, status = prospective.append_matured_outcomes(
        ledger,
        decision_date=decision,
        as_of_date=sessions[20],
        session_dates=sessions,
        outcomes={1: metrics, 5: metrics, 20: metrics},
    )
    assert status == "MATURITY_UPDATED"
    assert prospective.scoreboard_state(ledger)["matured_counts"] == {
        "1": 1,
        "5": 1,
        "20": 1,
    }

    same, status = prospective.append_matured_outcomes(
        ledger,
        decision_date=decision,
        as_of_date=sessions[20] + timedelta(days=3),
        session_dates=sessions,
        outcomes={20: metrics},
    )
    assert status == "MATURITY_ALREADY_RECORDED"
    assert same == ledger

    changed = {**metrics, "candidate_net_return_pct": 3.0}
    changed["paired_excess_percentage_points"] = 1.75
    with pytest.raises(
        prospective.ProspectiveObservationError,
        match="PROSPECTIVE_MATURED_OUTCOME_IMMUTABLE",
    ):
        prospective.append_matured_outcomes(
            ledger,
            decision_date=decision,
            as_of_date=sessions[20],
            session_dates=sessions,
            outcomes={20: changed},
        )


def test_append_rejects_tampered_observation_identity() -> None:
    ledger = prospective.new_ledger(_contract())
    observation = _observation()
    observation["action"] = "FLAT_CASH"

    with pytest.raises(
        prospective.ProspectiveObservationError,
        match="PROSPECTIVE_IDENTITY_MISMATCH.*observation.action",
    ):
        prospective.append_observation(ledger, observation)


def test_scoreboard_cannot_promote_before_owner_reviewed_gate() -> None:
    scoreboard = prospective.scoreboard_state(prospective.new_ledger(_contract()))

    assert scoreboard["status"] == "EVIDENCE_INSUFFICIENT"
    assert scoreboard["reason_codes"] == ["OWNER_REVIEWED_SAMPLE_EPISODE_GATE_NOT_FROZEN"]
    assert scoreboard["automatic_promotion_allowed"] is False
    assert scoreboard["production_effect"] == "none"
    assert scoreboard["broker_action"] == "none"
