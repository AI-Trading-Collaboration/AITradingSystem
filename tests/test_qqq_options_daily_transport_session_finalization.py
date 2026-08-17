from __future__ import annotations

import hashlib
import json
from datetime import date
from pathlib import Path

import pytest

from ai_trading_system.qqq_options_research.daily_transport_per_axis_collection_proposal import (
    Axis,
)
from ai_trading_system.qqq_options_research.daily_transport_session_finalization import (
    DEFAULT_PACKAGE_ROOT,
    AxisSignal,
    DailyTransportSessionReducer,
    SessionFinalizationError,
    SessionSliceObservation,
    build_session_finalization_package,
    validate_session_finalization_package,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SESSION = date(2025, 3, 14)


def _signals(*, valid: bool = True) -> dict[Axis, AxisSignal]:
    return {
        axis: AxisSignal(observed=True, valid=valid)
        for axis in (
            Axis.BID_ASK_QUOTE,
            Axis.GREEKS,
            Axis.IMPLIED_VOLATILITY,
            Axis.OPEN_INTEREST,
            Axis.VOLUME,
        )
    }


def _chain(
    *, equity_close: object = None, valid: bool = True, contract_zero: bool = True
) -> SessionSliceObservation:
    return SessionSliceObservation(
        session_id=SESSION,
        chain_contract_count=10,
        equity_close=equity_close,
        chain_axis_signals=_signals(valid=valid),
        cross_fields_without_underlying_valid=valid,
        contract_underlying_zero_observed=contract_zero,
    )


def _empty(*, equity_close: object = None) -> SessionSliceObservation:
    return SessionSliceObservation(
        session_id=SESSION,
        chain_contract_count=0,
        equity_close=equity_close,
    )


def _reduce(*observations: SessionSliceObservation):
    reducer = DailyTransportSessionReducer((SESSION,))
    for observation in observations:
        reducer.observe(observation)
    return reducer.finalize()


def test_chainless_slice_is_not_terminal_when_chain_arrives_later() -> None:
    result = _reduce(_empty(equity_close=500), _chain())
    counts = result.per_axis_status_session_counts
    assert counts["TRADING2531_OPTION_CHAIN_PRESENCE_PRESENT_SESSIONS"] == 1
    assert counts["TRADING2531_OPTION_CHAIN_PRESENCE_MISSING_SESSIONS"] == 0
    assert result.diagnostic_counts["SESSIONS_RECOVERED_AFTER_CHAINLESS"] == 1
    assert result.diagnostic_counts["CHAINLESS_SLICE_EVENTS"] == 1


def test_same_session_event_order_is_commutative() -> None:
    first = _reduce(_empty(equity_close=500), _chain())
    second = _reduce(_chain(), _empty(equity_close=500))
    assert first.per_axis_status_session_counts == second.per_axis_status_session_counts
    assert first.diagnostic_counts["SESSIONS_RECOVERED_AFTER_CHAINLESS"] == 1
    assert second.diagnostic_counts["SESSIONS_RECOVERED_AFTER_CHAINLESS"] == 0


def test_never_observed_chain_becomes_missing_only_at_finalize() -> None:
    result = _reduce(_empty(equity_close=500))
    counts = result.per_axis_status_session_counts
    assert counts["TRADING2531_OPTION_CHAIN_PRESENCE_MISSING_SESSIONS"] == 1
    assert counts["TRADING2531_UNDERLYING_PRICE_NOT_EVALUATED_SESSIONS"] == 1
    assert result.diagnostic_counts["SESSIONS_NEVER_CHAIN"] == 1


def test_same_session_equity_bar_is_canonical_and_contract_zero_is_ignored() -> None:
    result = _reduce(_chain(equity_close=500, contract_zero=True))
    counts = result.per_axis_status_session_counts
    assert counts["TRADING2531_UNDERLYING_PRICE_PRESENT_SESSIONS"] == 1
    assert counts["TRADING2531_CROSS_FIELD_CONSISTENCY_PRESENT_SESSIONS"] == 1
    assert result.diagnostic_counts["SESSIONS_WITH_CONTRACT_ZERO_IGNORED"] == 1
    assert result.diagnostic_counts["SESSIONS_WITH_CANONICAL_EQUITY_PRESENT"] == 1


def test_positive_equity_arriving_after_chain_repairs_underlying_without_stale_fallback() -> None:
    result = _reduce(_chain(), _empty(equity_close=500))
    assert (
        result.per_axis_status_session_counts[
            "TRADING2531_UNDERLYING_PRICE_PRESENT_SESSIONS"
        ]
        == 1
    )


@pytest.mark.parametrize(
    ("equity_close", "status", "diagnostic"),
    [
        (0, "INVALID", "SESSIONS_WITH_CANONICAL_EQUITY_INVALID"),
        (-1, "INVALID", "SESSIONS_WITH_CANONICAL_EQUITY_INVALID"),
        (None, "MISSING", "SESSIONS_WITH_CANONICAL_EQUITY_MISSING"),
        (float("nan"), "MISSING", "SESSIONS_WITH_CANONICAL_EQUITY_MISSING"),
    ],
)
def test_missing_or_nonpositive_equity_fails_closed(
    equity_close: object, status: str, diagnostic: str
) -> None:
    result = _reduce(_chain(equity_close=equity_close))
    assert (
        result.per_axis_status_session_counts[
            f"TRADING2531_UNDERLYING_PRICE_{status}_SESSIONS"
        ]
        == 1
    )
    assert (
        result.per_axis_status_session_counts[
            "TRADING2531_CROSS_FIELD_CONSISTENCY_INVALID_SESSIONS"
        ]
        == 1
    )
    assert result.diagnostic_counts[diagnostic] == 1


def test_duplicate_chain_events_merge_without_double_counting_session() -> None:
    result = _reduce(_chain(equity_close=500), _chain(equity_close=500))
    assert (
        result.per_axis_status_session_counts[
            "TRADING2531_OPTION_CHAIN_PRESENCE_PRESENT_SESSIONS"
        ]
        == 1
    )
    assert result.diagnostic_counts["SESSIONS_WITH_MULTIPLE_CHAIN_EVENTS"] == 1


def test_axis_signals_merge_present_over_invalid_independent_of_order() -> None:
    first = _reduce(_chain(equity_close=500, valid=False), _chain(valid=True))
    second = _reduce(_chain(valid=True), _chain(equity_close=500, valid=False))
    assert first.per_axis_status_session_counts == second.per_axis_status_session_counts
    assert (
        first.per_axis_status_session_counts[
            "TRADING2531_BID_ASK_QUOTE_PRESENT_SESSIONS"
        ]
        == 1
    )


def test_observation_rejects_partial_chain_axis_inventory() -> None:
    with pytest.raises(ValueError, match="exact chain-axis"):
        SessionSliceObservation(
            session_id=SESSION,
            chain_contract_count=1,
            chain_axis_signals={Axis.VOLUME: AxisSignal(observed=True, valid=True)},
        )


def test_reducer_rejects_out_of_scope_and_post_finalize_events() -> None:
    reducer = DailyTransportSessionReducer((SESSION,))
    with pytest.raises(ValueError, match="outside expected"):
        reducer.observe(
            SessionSliceObservation(
                session_id=date(2025, 3, 17), chain_contract_count=0
            )
        )
    reducer.finalize()
    with pytest.raises(ValueError, match="already finalized"):
        reducer.observe(_empty())


def test_build_package_binds_predecessor_and_has_no_external_authority() -> None:
    built = build_session_finalization_package(project_root=PROJECT_ROOT)
    assert built.contract["predecessor_evidence_content_sha256"] == (
        "d47f3234f58e1a7114984a7a79a5090082f923b7e02c65a66dfa8b761321f792"
    )
    assert built.contract["session_absence_finalization"] == "END_OF_ALGORITHM_ONLY"
    assert built.contract["canonical_underlying_source"] == (
        "SAME_SESSION_RAW_QQQ_EQUITY_TRADEBAR_CLOSE"
    )
    assert built.proposal["authorization_status"] == (
        "NOT_GRANTED_FOR_EXTERNAL_PLATFORM_ACTIONS"
    )
    assert built.proposal["external_action_performed"] is False
    assert built.policy["maximum_external_cloud_backtests_in_this_task"] == 0


def test_candidate_code_uses_finalization_and_same_session_bar_without_order_path() -> None:
    text = build_session_finalization_package(
        project_root=PROJECT_ROOT
    ).project_code_bytes.decode("utf-8")
    assert "data.bars.get(self._equity)" in text
    assert 'if state["chain_events"] == 0:' in text
    assert "SESSIONS_RECOVERED_AFTER_CHAINLESS" in text
    assert "underlying_last_price" in text
    assert "contract_zero_observed" in text
    assert "self.securities[self._equity].price" not in text
    assert "market_order(" not in text.casefold()
    assert "set_holdings(" not in text.casefold()
    assert "liquidate(" not in text.casefold()
    assert "self._seen.add" not in text


def test_predecessor_evidence_bytes_remain_immutable() -> None:
    path = (
        PROJECT_ROOT
        / "inputs/research/qqq_options/"
        "trading_2530_daily_transport_per_axis_collection_execution_v1/"
        "export_safe_aggregate_evidence.json"
    )
    payload = json.loads(path.read_bytes())
    assert payload["content_sha256"] == (
        "d47f3234f58e1a7114984a7a79a5090082f923b7e02c65a66dfa8b761321f792"
    )
    assert payload["source_result_file_sha256"] == (
        "2233b20a900c76cbb6938a96c635c5dabc5855349ac74ff684c8f1c657b752b7"
    )


def test_repository_package_is_exact_and_canonical() -> None:
    built = validate_session_finalization_package(project_root=PROJECT_ROOT)
    package_root = PROJECT_ROOT / DEFAULT_PACKAGE_ROOT
    assert tuple(sorted(path.name for path in package_root.iterdir())) == (
        "main.py",
        "owner_decision_request.md",
        "package_manifest.json",
        "proposal.json",
        "session_finalization_contract.json",
    )
    assert built.manifest["artifact_count"] == 4
    assert built.manifest["external_action_performed"] is False


def test_package_tamper_fails_closed(tmp_path: Path) -> None:
    built = build_session_finalization_package(project_root=PROJECT_ROOT)
    payloads = {
        "main.py": built.project_code_bytes,
        "owner_decision_request.md": built.owner_decision_request_bytes,
        "package_manifest.json": _canonical(built.manifest),
        "proposal.json": _canonical(built.proposal),
        "session_finalization_contract.json": _canonical(built.contract),
    }
    for name, raw in payloads.items():
        (tmp_path / name).write_bytes(raw)
    (tmp_path / "main.py").write_bytes(built.project_code_bytes + b"# tamper\n")
    with pytest.raises(SessionFinalizationError) as caught:
        validate_session_finalization_package(tmp_path, project_root=PROJECT_ROOT)
    assert caught.value.code == "SESSION_FINALIZATION_PACKAGE_ADMISSION_FAILED"


def test_manifest_artifact_hashes_match_exact_bytes() -> None:
    built = build_session_finalization_package(project_root=PROJECT_ROOT)
    raw = {
        "main.py": built.project_code_bytes,
        "owner_decision_request.md": built.owner_decision_request_bytes,
        "proposal.json": _canonical(built.proposal),
        "session_finalization_contract.json": _canonical(built.contract),
    }
    for artifact in built.manifest["artifacts"]:
        item = raw[str(artifact["relative_path"])]
        assert hashlib.sha256(item).hexdigest() == artifact["sha256"]
        assert len(item) == artifact["byte_count"]


def _canonical(value: object) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False)
        + "\n"
    ).encode("utf-8")
