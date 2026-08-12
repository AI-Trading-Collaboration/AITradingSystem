from __future__ import annotations

import copy
import hashlib
import json
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pytest

from ai_trading_system.qqq_options_research.primary_window_export_safe_derived_aggregate_collector import (  # noqa: E501
    QCQQQOptionsDerivedAggregateCollectorAuthorization,
    QCQQQOptionsDerivedAggregateCollectorError,
    QCQQQOptionsDerivedAggregateCollectorEvidence,
    QCQQQOptionsDerivedAggregateCollectorProposal,
    QCQQQOptionsDerivedAggregateCollectorRunScope,
    build_qc_qqq_options_derived_aggregate_collector_proposal,
    build_qc_qqq_options_derived_aggregate_collector_run_scope,
    build_qc_qqq_options_primary_window_derived_aggregate_collector_evidence,
    load_qc_qqq_options_primary_window_derived_aggregate_collector_policy,
    render_qc_qqq_options_primary_window_derived_aggregate_collector_project,
)

ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_SHA = "c4e75323f71eb5ad92a2ed60e34429acc824ae69"
PROJECT_ID = 34_808_569
BACKTEST_ID = "2512abc123"
CREATED_AT = datetime(2026, 8, 12, 1, 0, tzinfo=UTC)
AUTHORIZED_AT = datetime(2026, 8, 12, 2, 0, tzinfo=UTC)
COLLECTED_AT = datetime(2026, 8, 12, 3, 0, tzinfo=UTC)
EXPIRES_AT = datetime(2026, 8, 13, 2, 0, tzinfo=UTC)

STATISTIC_VALUES: dict[str, int | float] = {
    "delta_max": 0.8,
    "delta_min": -0.8,
    "dte_days_max": 45,
    "dte_days_min": 1,
    "moneyness_ratio_max": 1.2,
    "moneyness_ratio_min": 0.8,
    "open_interest_max": 1000,
    "open_interest_min_nonzero": 1,
    "candidate_count": 20,
    "deterministic_tie_count": 2,
    "relative_spread_max": 0.5,
    "relative_spread_min": 0.01,
    "volume_max": 500,
    "volume_min_nonzero": 1,
    "ask_price_max": 20,
    "ask_price_min": 0.05,
    "missing_quote_count": 4,
    "one_sided_quote_count": 6,
    "two_sided_quote_count": 20,
}


def _scope(*, end: date = date(2021, 2, 23)) -> QCQQQOptionsDerivedAggregateCollectorRunScope:
    return build_qc_qqq_options_derived_aggregate_collector_run_scope(
        run_scope_id="trading-2512-test-scope",
        created_at_utc=CREATED_AT,
        repository_code_sha=REPOSITORY_SHA,
        target_project_id=PROJECT_ID,
        requested_end=end,
        project_root=ROOT,
    )


def _proposal(
    *, scope: QCQQQOptionsDerivedAggregateCollectorRunScope | None = None
) -> QCQQQOptionsDerivedAggregateCollectorProposal:
    return build_qc_qqq_options_derived_aggregate_collector_proposal(
        proposal_id="trading-2512-test-proposal",
        issued_at_utc=CREATED_AT,
        run_scope=scope or _scope(),
        project_root=ROOT,
    )


def _authorization(
    proposal: QCQQQOptionsDerivedAggregateCollectorProposal,
) -> QCQQQOptionsDerivedAggregateCollectorAuthorization:
    return QCQQQOptionsDerivedAggregateCollectorAuthorization.seal(
        schema_version="qc_qqq_options_derived_aggregate_collector_authorization.v1",
        owner_decision_token=(
            "owner_decision:TRADING-2512:2026-08-12:"
            "authorize_single_zero_order_derived_aggregate_collection_v1"
        ),
        authorized_at_utc=AUTHORIZED_AT,
        expires_at_utc=EXPIRES_AT,
        authorization_single_use=True,
        authorization_invalidates_after_evidence_collection=True,
        proposal_content_sha256=proposal.content_sha256,
        run_scope_content_sha256=proposal.run_scope.content_sha256,
        repository_code_sha=proposal.run_scope.repository_code_sha,
        target_project_id=proposal.run_scope.target_project_id,
        project_code_lf_sha256=proposal.project_code_lf_sha256,
        collector_policy_file_sha256=proposal.collector_policy_file_sha256,
        collector_policy_canonical_sha256=proposal.collector_policy_canonical_sha256,
        maximum_project_mutations=1,
        maximum_cloud_backtests=1,
        maximum_orders=0,
        maximum_fills=0,
        allowed_actions=proposal.allowed_actions,
        prohibited_actions=proposal.prohibited_actions,
        collector_id="codex-capability-coordinator",
        independent_reviewer_id="project-owner",
    )


def _runtime_identity(proposal: QCQQQOptionsDerivedAggregateCollectorProposal) -> str:
    return (
        "schema=qc_qqq_options_derived_aggregate_collector_runtime.v1"
        f"|scope={proposal.run_scope.content_sha256}"
        f"|repository={proposal.run_scope.repository_code_sha}"
        f"|policy_file={proposal.collector_policy_file_sha256}"
        f"|policy_canonical={proposal.collector_policy_canonical_sha256}"
        f"|transport={proposal.transport_map_sha256}"
    )


def _result_payload(
    proposal: QCQQQOptionsDerivedAggregateCollectorProposal,
) -> dict[str, Any]:
    loaded = load_qc_qqq_options_primary_window_derived_aggregate_collector_policy(
        project_root=ROOT
    )
    zone = ZoneInfo(loaded.policy.transport.algorithm_time_zone)
    series: dict[str, Any] = {}
    for series_mapping in loaded.policy.transport.expected_series:
        values: list[list[int | float]] = []
        for session_id in proposal.run_scope.session_ids:
            for statistic in series_mapping.mappings:
                point_time = datetime.combine(
                    session_id,
                    time(
                        loaded.policy.transport.point_local_hour,
                        loaded.policy.transport.point_local_minute,
                        statistic.ordinal_second,
                    ),
                    tzinfo=zone,
                )
                values.append(
                    [int(point_time.timestamp()), STATISTIC_VALUES[statistic.statistic_id]]
                )
        series[series_mapping.series_id] = {
            "name": series_mapping.series_id,
            "unit": series_mapping.unit_id,
            "seriesType": 1,
            "values": values,
        }
    scope = proposal.run_scope
    return {
        "algorithmConfiguration": {
            "startDate": f"{scope.requested_start.isoformat()}T00:00:00Z",
            "endDate": f"{scope.requested_end.isoformat()}T23:59:59Z",
        },
        "charts": {
            "TRADING2512_EXPORT_SAFE_DERIVED_AGGREGATES_V1": {
                "name": "TRADING2512_EXPORT_SAFE_DERIVED_AGGREGATES_V1",
                "chartType": 0,
                "series": series,
            }
        },
        "orders": {},
        "runtimeStatistics": {
            "TRADING2512_IDENTITY": _runtime_identity(proposal),
            "TRADING2512_TERMINAL": (
                "status=COMPLETE"
                f"|observed_sessions={len(scope.session_ids)}"
                "|invalid_sessions=0|orders=0|fills=0|portfolio_invested=false"
                "|raw_rows=false|log_data=false|object_store=false"
            ),
            "Holdings": "$0.00",
            "Unrealized": "$0.00",
            "Volume": "$0.00",
        },
        "state": {
            "RuntimeError": "",
            "OrderCount": "0",
            "Hostname": f"BACKTESTING-1-{BACKTEST_ID}",
            "Status": "Completed",
        },
        "statistics": {"Total Orders": "0", "Total Fees": "$0.00"},
    }


def _result_bytes(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()


def _evidence(
    payload: dict[str, Any],
    *,
    proposal: QCQQQOptionsDerivedAggregateCollectorProposal | None = None,
    authorization: QCQQQOptionsDerivedAggregateCollectorAuthorization | None = None,
    collected_at_utc: datetime = COLLECTED_AT,
    reviewed_target_project_id: int = PROJECT_ID,
    reviewed_code_sha256: str | None = None,
) -> QCQQQOptionsDerivedAggregateCollectorEvidence:
    selected_proposal = proposal or _proposal()
    selected_authorization = authorization or _authorization(selected_proposal)
    return build_qc_qqq_options_primary_window_derived_aggregate_collector_evidence(
        evidence_id="trading-2512-test-evidence",
        collected_at_utc=collected_at_utc,
        backtest_id=BACKTEST_ID,
        result_bytes=_result_bytes(payload),
        proposal=selected_proposal,
        authorization=selected_authorization,
        reviewed_target_project_id=reviewed_target_project_id,
        reviewed_project_code_lf_sha256=(
            reviewed_code_sha256 or selected_proposal.project_code_lf_sha256
        ),
        project_root=ROOT,
    )


def _series(payload: dict[str, Any]) -> dict[str, Any]:
    return payload["charts"]["TRADING2512_EXPORT_SAFE_DERIVED_AGGREGATES_V1"]["series"]


def test_policy_binds_2511_authority_and_exact_transport_without_policy_values() -> None:
    loaded = load_qc_qqq_options_primary_window_derived_aggregate_collector_policy(
        project_root=ROOT
    )

    assert loaded.policy.primary_research_start == date(2021, 2, 22)
    assert loaded.policy.platform_constraints.max_custom_series == 10
    assert loaded.policy.platform_constraints.max_points_per_series == 4000
    assert loaded.policy.platform_constraints.max_primary_sessions == 2000
    assert len(loaded.policy.transport.expected_series) == 10
    assert len(loaded.policy.supported_slots) == 9
    assert len(loaded.policy.unsupported_slots) == 9
    assert loaded.policy.unsupported_slots[-1].slot_id == "SEL_QUOTE_FRESHNESS"
    assert loaded.policy.safety.owner_policy_value_count == 0
    assert loaded.policy.safety.engine_status == "POLICY_BLOCKED_CASH_PRESERVATION"
    assert loaded.policy.safety.external_action_authorized is False
    assert loaded.policy_file_sha256 == hashlib.sha256(loaded.policy_path.read_bytes()).hexdigest()


def test_run_scope_uses_primary_window_calendar_and_exact_quota() -> None:
    scope = _scope(end=date(2021, 2, 26))

    assert scope.requested_start == scope.evaluated_start == date(2021, 2, 22)
    assert scope.requested_end == scope.evaluated_end == date(2021, 2, 26)
    assert scope.session_ids == tuple(date(2021, 2, day) for day in range(22, 27))
    assert scope.maximum_orders == scope.maximum_fills == 0
    assert (
        QCQQQOptionsDerivedAggregateCollectorRunScope.from_json_bytes(scope.canonical_bytes)
        == scope
    )


def test_run_scope_rejects_end_before_primary_start_and_transport_overflow() -> None:
    with pytest.raises(QCQQQOptionsDerivedAggregateCollectorError):
        _scope(end=date(2021, 2, 21))

    with pytest.raises(
        QCQQQOptionsDerivedAggregateCollectorError,
        match="COLLECTOR_SESSION_QUOTA_EXCEEDED",
    ):
        _scope(end=date(2030, 1, 1))


def test_project_renderer_is_deterministic_zero_order_and_export_safe() -> None:
    scope = _scope()
    first = render_qc_qqq_options_primary_window_derived_aggregate_collector_project(
        run_scope=scope, project_root=ROOT
    )
    second = render_qc_qqq_options_primary_window_derived_aggregate_collector_project(
        run_scope=scope, project_root=ROOT
    )
    code = first.code_bytes.decode()

    assert first == second
    assert first.code_lf_sha256 == hashlib.sha256(first.code_bytes).hexdigest()
    assert "Resolution.DAILY" in code
    assert "DataNormalizationMode.RAW" in code
    assert "orders=0|fills=0|portfolio_invested=false" in code
    assert "S10_QUOTE_DISPOSITION_B" in code
    assert "open_interest < 0" in code
    assert "volume < 0" in code
    assert "min(positive_oi_values)" in code
    assert "min(positive_volume_values)" in code
    compile(code, "trading_2512_qc_project.py", "exec")
    for prohibited in (
        "self.debug(",
        "self.log(",
        "self.object_store",
        "market_order(",
        "limit_order(",
        "self.buy(",
        "self.sell(",
    ):
        assert prohibited not in code.lower()


def test_proposal_is_blocked_and_canonical() -> None:
    proposal = _proposal()

    assert proposal.authorization_status == "NOT_GRANTED_FOR_EXTERNAL_PLATFORM_ACTIONS"
    assert proposal.decision == "OWNER_AUTHORIZATION_REQUIRED"
    assert proposal.owner_policy_value_count == 0
    assert proposal.selection_authorized is False
    assert proposal.external_action_performed is False
    assert (
        QCQQQOptionsDerivedAggregateCollectorProposal.from_json_bytes(proposal.canonical_bytes)
        == proposal
    )


def test_valid_result_builds_exact_observations_but_keeps_dq_and_engine_blocked() -> None:
    proposal = _proposal()
    payload = _result_payload(proposal)
    evidence = _evidence(payload, proposal=proposal)

    assert len(evidence.session_ids) == 2
    assert len(evidence.supported_slot_ids) == 9
    assert len(evidence.unsupported_slots) == 9
    assert len(evidence.observations) == 18
    assert evidence.dq_status == "NOT_EVALUATED_PENDING_LOCAL_DQ_GATE"
    assert evidence.decision == "RESULT_PARSED_DQ_NOT_EVALUATED"
    assert evidence.owner_policy_value_count == 0
    assert evidence.engine_status == "POLICY_BLOCKED_CASH_PRESERVATION"
    assert evidence.selection_authorized is False
    assert evidence.orders == evidence.fills == 0
    assert all(
        statistic.is_policy_value is False
        for observation in evidence.observations
        for statistic in observation.statistics
    )
    assert (
        QCQQQOptionsDerivedAggregateCollectorEvidence.from_json_bytes(evidence.canonical_bytes)
        == evidence
    )


def test_result_object_and_series_key_permutation_preserves_semantics_not_file_hash() -> None:
    proposal = _proposal()
    first_payload = _result_payload(proposal)
    second_payload = copy.deepcopy(first_payload)
    series = _series(second_payload)
    second_payload["charts"]["TRADING2512_EXPORT_SAFE_DERIVED_AGGREGATES_V1"]["series"] = dict(
        reversed(tuple(series.items()))
    )
    second_payload = dict(reversed(tuple(second_payload.items())))

    first = _evidence(first_payload, proposal=proposal)
    second = _evidence(second_payload, proposal=proposal)

    assert first.result_file_sha256 != second.result_file_sha256
    assert first.result_payload_sha256 == second.result_payload_sha256
    assert first.observations == second.observations


def test_result_timestamps_replay_across_new_york_dst_boundary() -> None:
    proposal = _proposal(scope=_scope(end=date(2021, 3, 15)))
    evidence = _evidence(_result_payload(proposal), proposal=proposal)

    assert date(2021, 3, 12) in evidence.session_ids
    assert date(2021, 3, 15) in evidence.session_ids
    assert len(evidence.observations) == len(evidence.session_ids) * 9


@pytest.mark.parametrize(
    ("mutation", "match"),
    [
        (
            lambda payload: _series(payload).pop("S10_QUOTE_DISPOSITION_B"),
            "series inventory drifted",
        ),
        (
            lambda payload: _series(payload).__setitem__(
                "S11_UNKNOWN",
                copy.deepcopy(_series(payload)["S10_QUOTE_DISPOSITION_B"]),
            ),
            "series inventory drifted",
        ),
        (
            lambda payload: _series(payload)["S01_DELTA_RANGE"]["values"].reverse(),
            "session/ordinal timestamp mismatch",
        ),
        (
            lambda payload: _series(payload)["S01_DELTA_RANGE"]["values"].pop(),
            "point count mismatch",
        ),
        (
            lambda payload: _series(payload)["S01_DELTA_RANGE"]["values"][0].__setitem__(
                0,
                _series(payload)["S01_DELTA_RANGE"]["values"][0][0] + 3600,
            ),
            "session/ordinal timestamp mismatch",
        ),
        (
            lambda payload: _series(payload)["S01_DELTA_RANGE"].__setitem__("unit", "percent"),
            "series unit mismatch",
        ),
    ],
)
def test_result_transport_tampering_fails_closed(mutation: Any, match: str) -> None:
    proposal = _proposal()
    payload = _result_payload(proposal)
    mutation(payload)

    with pytest.raises(QCQQQOptionsDerivedAggregateCollectorError, match=match):
        _evidence(payload, proposal=proposal)


@pytest.mark.parametrize(
    ("series_id", "point_index", "value", "match"),
    [
        ("S01_DELTA_RANGE", 0, float("nan"), "non-finite JSON constant"),
        ("S02_DTE_WINDOW", 0, 1.5, "count statistic must be integral"),
        ("S03_MONEYNESS_RANGE", 0, 0, "statistic must be positive"),
        ("S04_OPEN_INTEREST", 1, 0, "statistic must be positive"),
        ("S06_SPREAD_RANGE", 0, -0.1, "statistic must be nonnegative"),
        ("S08_ASK_RANGE", 1, -1, "statistic must be positive"),
    ],
)
def test_result_statistic_domain_tampering_fails_closed(
    series_id: str, point_index: int, value: float, match: str
) -> None:
    proposal = _proposal()
    payload = _result_payload(proposal)
    _series(payload)[series_id]["values"][point_index][1] = value

    with pytest.raises(QCQQQOptionsDerivedAggregateCollectorError, match=match):
        _evidence(payload, proposal=proposal)


@pytest.mark.parametrize(
    ("series_id", "point_index", "value", "match"),
    [
        ("S03_MONEYNESS_RANGE", 0, 0.7, "max/min envelope is inverted"),
        ("S05_RANK_PRIORITY", 1, 21, "candidate/tie/quote populations"),
        ("S10_QUOTE_DISPOSITION_B", 0, 19, "candidate/tie/quote populations"),
    ],
)
def test_result_cross_statistic_inconsistency_fails_closed(
    series_id: str, point_index: int, value: float, match: str
) -> None:
    proposal = _proposal()
    payload = _result_payload(proposal)
    _series(payload)[series_id]["values"][point_index][1] = value

    with pytest.raises(QCQQQOptionsDerivedAggregateCollectorError, match=match):
        _evidence(payload, proposal=proposal)


@pytest.mark.parametrize(
    ("mutation", "match"),
    [
        (
            lambda payload: payload["algorithmConfiguration"].__setitem__(
                "endDate", "2021-02-24T23:59:59Z"
            ),
            "algorithm range differs",
        ),
        (
            lambda payload: payload["state"].__setitem__("Status", "RuntimeError"),
            "backtest did not complete",
        ),
        (
            lambda payload: payload["state"].__setitem__("OrderCount", "1"),
            "state reports orders",
        ),
        (
            lambda payload: payload.__setitem__("orders", {"1": {"id": 1}}),
            "orders inventory is not empty",
        ),
        (
            lambda payload: payload["statistics"].__setitem__("Total Fees", "$1.00"),
            "statistics report orders or fees",
        ),
        (
            lambda payload: payload["runtimeStatistics"].__setitem__(
                "TRADING2512_IDENTITY", "forged"
            ),
            "runtime identity mismatch",
        ),
        (
            lambda payload: payload["runtimeStatistics"].__setitem__(
                "TRADING2512_TERMINAL", "status=COMPLETE"
            ),
            "terminal status is incomplete or unsafe",
        ),
        (
            lambda payload: payload["runtimeStatistics"].__setitem__("Holdings", "$1.00"),
            "runtime Holdings must be zero",
        ),
        (
            lambda payload: payload.__setitem__("rawOptionRows", [{"symbol": "PROHIBITED"}]),
            "prohibited result marker present",
        ),
    ],
)
def test_result_scope_order_and_runtime_tampering_fails_closed(mutation: Any, match: str) -> None:
    proposal = _proposal()
    payload = _result_payload(proposal)
    mutation(payload)

    with pytest.raises(QCQQQOptionsDerivedAggregateCollectorError, match=match):
        _evidence(payload, proposal=proposal)


def test_authorization_project_code_time_and_proposal_identity_fail_closed() -> None:
    proposal = _proposal()
    payload = _result_payload(proposal)

    with pytest.raises(QCQQQOptionsDerivedAggregateCollectorError, match="target-project"):
        _evidence(payload, proposal=proposal, reviewed_target_project_id=PROJECT_ID + 1)
    with pytest.raises(QCQQQOptionsDerivedAggregateCollectorError, match="project-code"):
        _evidence(payload, proposal=proposal, reviewed_code_sha256="f" * 64)
    with pytest.raises(QCQQQOptionsDerivedAggregateCollectorError, match="authorization window"):
        _evidence(
            payload,
            proposal=proposal,
            collected_at_utc=EXPIRES_AT + timedelta(seconds=1),
        )

    other = _proposal(scope=_scope(end=date(2021, 2, 24)))
    with pytest.raises(QCQQQOptionsDerivedAggregateCollectorError, match="identity mismatch"):
        _evidence(
            payload,
            proposal=proposal,
            authorization=_authorization(other),
            reviewed_code_sha256=other.project_code_lf_sha256,
        )


def test_noncanonical_or_duplicate_sealed_records_fail_closed() -> None:
    proposal = _proposal()
    noncanonical = json.dumps(proposal.model_dump(mode="json")).encode()
    proposal_id_line = next(
        line + b"\n" for line in proposal.canonical_bytes.splitlines() if b'"proposal_id"' in line
    )
    duplicate = proposal.canonical_bytes.replace(
        proposal_id_line, proposal_id_line + proposal_id_line, 1
    )

    with pytest.raises(QCQQQOptionsDerivedAggregateCollectorError):
        QCQQQOptionsDerivedAggregateCollectorProposal.from_json_bytes(noncanonical)
    with pytest.raises(QCQQQOptionsDerivedAggregateCollectorError, match="duplicate JSON key"):
        QCQQQOptionsDerivedAggregateCollectorProposal.from_json_bytes(duplicate)


def test_seal_rejects_caller_supplied_content_hash() -> None:
    scope = _scope()
    payload = scope.semantic_payload()
    payload["content_sha256"] = "0" * 64

    with pytest.raises(
        QCQQQOptionsDerivedAggregateCollectorError,
        match="rejects caller-supplied values",
    ):
        QCQQQOptionsDerivedAggregateCollectorRunScope.seal(**payload)
