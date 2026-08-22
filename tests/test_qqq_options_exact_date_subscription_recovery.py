from __future__ import annotations

import hashlib
import sys
import types
from datetime import date, datetime, timedelta
from pathlib import Path

import pytest

from ai_trading_system.qqq_options_research.daily_transport_per_axis_collection_proposal import (
    Axis,
    AxisStatus,
)
from ai_trading_system.qqq_options_research.daily_transport_session_finalization import (
    AxisSignal,
    DailyTransportSessionReducer,
)
from ai_trading_system.qqq_options_research.exact_date_subscription_recovery import (
    DEFAULT_PACKAGE_ROOT,
    DeliveryPath,
    ExactDateSubscriptionRecoveryError,
    ProviderHistoryRecord,
    RecoveryStatus,
    SubscriptionSessionSummary,
    admit_exact_date_provider_history,
    build_exact_date_subscription_recovery_package,
    load_exact_date_subscription_recovery_policy,
    plan_exact_date_recovery,
    validate_exact_date_subscription_recovery_package,
    write_exact_date_subscription_recovery_package,
)

ROOT = Path(__file__).resolve().parents[1]
TARGET = date(2022, 8, 26)
CHAIN_AXES = (
    Axis.BID_ASK_QUOTE,
    Axis.GREEKS,
    Axis.IMPLIED_VOLATILITY,
    Axis.OPEN_INTEREST,
    Axis.VOLUME,
)


def _signals(*, valid: bool = True) -> dict[Axis, AxisSignal]:
    return {axis: AxisSignal(observed=True, valid=valid) for axis in CHAIN_AXES}


def _missing_summary(**overrides: object) -> SubscriptionSessionSummary:
    values: dict[str, object] = {
        "session_id": TARGET,
        "session_finalized": True,
        "equity_close": 320.0,
        "subscribed_chain_event_count": 0,
        "subscribed_contract_count": 0,
    }
    values.update(overrides)
    return SubscriptionSessionSummary(**values)  # type: ignore[arg-type]


def _record(**overrides: object) -> ProviderHistoryRecord:
    values: dict[str, object] = {
        "source_date": TARGET,
        "availability_date": TARGET + timedelta(days=1),
        "contract_count": 6496,
        "chain_axis_signals": _signals(),
        "cross_fields_without_underlying_valid": True,
        "contract_underlying_zero_observed": False,
    }
    values.update(overrides)
    return ProviderHistoryRecord(**values)  # type: ignore[arg-type]


def test_normal_subscribed_slice_has_absolute_precedence() -> None:
    summary = _missing_summary(
        subscribed_chain_event_count=2,
        subscribed_contract_count=100,
    )
    assert plan_exact_date_recovery(summary) is None


@pytest.mark.parametrize(
    ("overrides", "code"),
    [
        ({"session_finalized": False}, "RECOVERY_BEFORE_SESSION_FINALIZATION"),
        ({"equity_close": None}, "RECOVERY_EQUITY_SESSION_INVALID"),
        ({"equity_close": 0}, "RECOVERY_EQUITY_SESSION_INVALID"),
        ({"equity_close": float("nan")}, "RECOVERY_EQUITY_SESSION_INVALID"),
    ],
)
def test_recovery_plan_requires_finalization_and_valid_equity(
    overrides: dict[str, object], code: str
) -> None:
    with pytest.raises(ExactDateSubscriptionRecoveryError) as caught:
        plan_exact_date_recovery(_missing_summary(**overrides))
    assert caught.value.code == code


def test_exact_date_record_adapts_into_the_same_session_reducer() -> None:
    plan = plan_exact_date_recovery(_missing_summary())
    assert plan is not None
    delivery = admit_exact_date_provider_history(plan, (_record(),))

    assert delivery.delivery_path is DeliveryPath.EXACT_DATE_PROVIDER_HISTORY_RECOVERY
    assert delivery.status is RecoveryStatus.ACCEPTED
    assert delivery.provider_record_count == 1
    assert delivery.provider_contract_count == 6496
    assert delivery.session_observation.session_id == TARGET
    assert delivery.session_observation.chain_contract_count == 6496
    assert delivery.session_observation.chain_axis_signals == _signals()

    reducer = DailyTransportSessionReducer((TARGET,))
    reducer.observe(delivery.session_observation)
    result = reducer.finalize()
    assert result.observed_session_count == 1
    assert result.diagnostic_counts["SESSIONS_NEVER_CHAIN"] == 0
    assert (
        result.per_axis_status_session_counts[
            "TRADING2531_OPTION_CHAIN_PRESENCE_PRESENT_SESSIONS"
        ]
        == 1
    )
    for axis in CHAIN_AXES:
        assert (
            result.per_axis_status_session_counts[
                f"TRADING2531_{axis.value}_{AxisStatus.PRESENT.value}_SESSIONS"
            ]
            == 1
        )


@pytest.mark.parametrize(
    ("records", "code"),
    [
        ((), "RECOVERY_EXACT_DATE_RECORD_MISSING"),
        ((_record(), _record()), "RECOVERY_EXACT_DATE_RECORD_DUPLICATE"),
        (
            (_record(source_date=TARGET - timedelta(days=1)),),
            "RECOVERY_CROSS_DATE_FALLBACK",
        ),
        (
            (_record(availability_date=TARGET + timedelta(days=2)),),
            "RECOVERY_AVAILABILITY_IDENTITY_INVALID",
        ),
        (
            (_record(contract_count=0, chain_axis_signals={}),),
            "RECOVERY_EXACT_DATE_RECORD_EMPTY",
        ),
    ],
)
def test_provider_record_defects_fail_closed(
    records: tuple[ProviderHistoryRecord, ...], code: str
) -> None:
    plan = plan_exact_date_recovery(_missing_summary())
    assert plan is not None
    with pytest.raises(ExactDateSubscriptionRecoveryError) as caught:
        admit_exact_date_provider_history(plan, records)
    assert caught.value.code == code


def test_policy_and_package_bind_resolved_evidence_without_promoting_dq() -> None:
    loaded = load_exact_date_subscription_recovery_policy(project_root=ROOT)
    built = build_exact_date_subscription_recovery_package(project_root=ROOT)

    assert loaded.payload["target_source_date"] == "2022-08-26"
    assert loaded.payload["predecessor_exact_date_contract_count"] == 6496
    assert loaded.payload["cross_date_fallback_allowed"] is False
    assert loaded.payload["external_action_authorized"] is False
    assert built.recovery_contract["normal_slice_precedence"] == (
        "ABSOLUTE_NO_PROVIDER_QUERY_WHEN_CHAIN_PRESENT"
    )
    assert built.recovery_contract["recovered_record_adapter"] == (
        "SESSION_SLICE_OBSERVATION_V2_SAME_AXIS_REDUCER"
    )
    assert built.recovery_contract["observed_count_is_acceptance_threshold"] is False
    assert built.recovery_contract["cloud_validation_status"] == "NOT_EXECUTED"
    assert built.recovery_contract["dq_status"] == "FAIL"
    assert built.recovery_contract["pit_status"] == "NOT_EVALUATED"
    assert built.proposal["external_action_authorized"] is False
    assert built.proposal["maximum_orders"] == built.proposal["maximum_fills"] == 0
    assert built.manifest["artifact_count"] == 3


def test_candidate_is_zero_order_exact_date_and_same_axis() -> None:
    built = build_exact_date_subscription_recovery_package(project_root=ROOT)
    source = built.project_code_bytes.decode("utf-8")
    compile(source, "trading_2541_main.py", "exec")

    assert "data.option_chains.get(self._option)" in source
    assert "self.history[OptionUniverse](self._option, start_time, end_time)" in source
    assert "source_date = record.time.date().isoformat()" in source
    assert "expected_availability = record.time.date() + timedelta(days=1)" in source
    assert 'state["delivery_path"] = "SUBSCRIBED_SLICE"' in source
    assert 'state["delivery_path"] = "EXACT_DATE_PROVIDER_HISTORY_RECOVERY"' in source
    assert "END_OF_ALGORITHM_ONLY" not in source or "on_end_of_algorithm" in source
    assert "TRADING2541_RECOVERED_SESSION_COUNT" not in source
    assert '"RECOVERED_SESSION_COUNT": recovered_sessions' in source
    assert "self.market_order(" not in source
    assert "self.set_holdings(" not in source
    assert "self.liquidate(" not in source
    assert "self.option_chain(" not in source
    assert "self.log(" not in source
    assert "self.debug(" not in source
    assert "object_store" in source
    assert "dq_pit_promoted=false" in source


class _HistoryAccessor:
    def __init__(self, records: tuple[object, ...]) -> None:
        self.records = records
        self.calls = 0

    def __getitem__(self, _: object) -> object:
        def query(*args: object) -> tuple[object, ...]:
            self.calls += 1
            assert len(args) == 3
            return self.records

        return query


class _UniverseRecord:
    def __init__(
        self,
        source_date: date,
        contract_count: int,
        *,
        availability_offset_days: int = 1,
    ) -> None:
        self.time = datetime.combine(source_date, datetime.min.time())
        self.end_time = self.time + timedelta(days=availability_offset_days)
        self._contracts = tuple(object() for _ in range(contract_count))

    def __iter__(self):  # type: ignore[no-untyped-def]
        return iter(self._contracts)


def _load_candidate(monkeypatch: pytest.MonkeyPatch) -> type[object]:
    algorithm_imports = types.ModuleType("AlgorithmImports")
    algorithm_imports.__all__ = ["QCAlgorithm", "Slice", "OptionUniverse"]
    algorithm_imports.QCAlgorithm = object
    algorithm_imports.Slice = object
    algorithm_imports.OptionUniverse = object
    monkeypatch.setitem(sys.modules, "AlgorithmImports", algorithm_imports)
    source = build_exact_date_subscription_recovery_package(
        project_root=ROOT
    ).project_code_bytes
    namespace: dict[str, object] = {}
    exec(compile(source, "trading_2541_main.py", "exec"), namespace)
    return namespace["QQQOptionsExactDateSubscriptionRecovery"]  # type: ignore[return-value]


def test_candidate_accepts_only_exact_source_and_availability(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate = _load_candidate(monkeypatch)
    algorithm = object.__new__(candidate)
    history = _HistoryAccessor((_UniverseRecord(TARGET, 4),))
    algorithm.history = history
    algorithm._option = "QQQ_OPTION"
    algorithm._merge_contracts = lambda state, contracts, session: state.update(  # type: ignore[attr-defined]
        {"merged_count": len(contracts), "merged_session": session}
    )
    state: dict[str, object] = {"chain_events": 0, "delivery_path": "UNRESOLVED"}

    result = algorithm._recover_target(TARGET.isoformat(), state)

    assert history.calls == 1
    assert result["status"] == "ACCEPTED"
    assert result["exact_date_record_count"] == 1
    assert result["exact_date_contract_count"] == 4
    assert result["non_target_record_count"] == 0
    assert state["chain_events"] == 1
    assert state["delivery_path"] == "EXACT_DATE_PROVIDER_HISTORY_RECOVERY"
    assert state["merged_session"] == TARGET


def test_candidate_rejects_cross_date_without_merging(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate = _load_candidate(monkeypatch)
    algorithm = object.__new__(candidate)
    history = _HistoryAccessor((_UniverseRecord(TARGET - timedelta(days=1), 4),))
    algorithm.history = history
    algorithm._option = "QQQ_OPTION"
    algorithm._merge_contracts = lambda *args: pytest.fail("must not merge")  # type: ignore[attr-defined]
    state: dict[str, object] = {"chain_events": 0, "delivery_path": "UNRESOLVED"}

    result = algorithm._recover_target(TARGET.isoformat(), state)

    assert history.calls == 1
    assert result["status"] == "CROSS_DATE_FALLBACK_REJECTED"
    assert result["non_target_record_count"] == 1
    assert state["chain_events"] == 0
    assert state["delivery_path"] == "UNRESOLVED"


def test_repository_package_replays_exactly() -> None:
    built = validate_exact_date_subscription_recovery_package(project_root=ROOT)
    package_root = ROOT / DEFAULT_PACKAGE_ROOT
    assert (package_root / "main.py").read_bytes() == built.project_code_bytes
    assert hashlib.sha256(built.project_code_bytes).hexdigest() == built.proposal[
        "project_code_lf_sha256"
    ]


def test_package_writer_and_tamper_detection(tmp_path: Path) -> None:
    target = tmp_path / "package"
    write_exact_date_subscription_recovery_package(
        output_root=target,
        project_root=ROOT,
    )
    validate_exact_date_subscription_recovery_package(
        package_root=target,
        project_root=ROOT,
    )
    (target / "main.py").write_bytes((target / "main.py").read_bytes() + b"\n")
    with pytest.raises(ExactDateSubscriptionRecoveryError) as caught:
        validate_exact_date_subscription_recovery_package(
            package_root=target,
            project_root=ROOT,
        )
    assert caught.value.code == "RECOVERY_PACKAGE_REPLAY_MISMATCH"
