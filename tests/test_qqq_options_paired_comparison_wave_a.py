from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest

from ai_trading_system.qqq_options_research.paired_comparison_wave_a import (
    AuthorizationEvidence,
    AxisFinding,
    AxisStatus,
    CalendarPartitionEvidence,
    ComparatorAction,
    ComparatorEvent,
    FullyFundedQQQCashLedger,
    PairedComparisonWaveAError,
    QQQQuote,
    ReplayContext,
    admit_export_safe_aggregate,
    export_safe_field_inventory,
    load_wave_a_authority,
    quantconnect_comparator_helper_sha256,
    reduce_axis_findings,
    render_quantconnect_comparator_helper_fragment,
    replay_export_safe_aggregate,
)


def _aggregate_payload() -> dict[str, object]:
    authority = load_wave_a_authority()
    payload: dict[str, object] = {field: 0 for field in export_safe_field_inventory(authority)}
    for field in (
        "RUN_ID",
        "PROJECT_ID",
        "BACKTEST_ID",
        "LEAN_VERSION",
        "PLATFORM_VERSION",
        "CALENDAR_ID",
        "CANONICAL_DQ_RECEIPT_IDENTITY",
        "SIGNAL_MAPPING_IDENTITY",
        "EVENT_RECONCILIATION_STATUS",
        "SECONDARY_CAPITAL_AT_RISK_TIME_RESULT",
    ):
        payload[field] = f"fixture-{field.lower()}"
    payload.update(
        {
            "REPOSITORY_EXACT_COMMIT": "a" * 40,
            "QC_CODE_FILE_SHA256": "b" * 64,
            "POLICY_FILE_SHA256": "c" * 64,
            "POLICY_CANONICAL_SHA256": "d" * 64,
            "FREEZE_ADMISSION_FILE_SHA256": authority.file_sha256,
            "COMPARATOR_CONTRACT_FILE_SHA256": authority.contract.file_sha256,
            "COMPARATOR_CONTRACT_CANONICAL_SHA256": authority.contract.canonical_sha256,
            "SIGNAL_PACKAGE_RECEIPT_SHA256": "e" * 64,
            "SIGNAL_INDEX_SHA256": "f" * 64,
            "NORMALIZED_SIGNAL_SOURCE_SHA256": "1" * 64,
            "RUN_MANIFEST_SHA256": "2" * 64,
            "CLOUD_BUILD_IDENTITIES": ["fixture-build"],
            "REQUESTED_DATE_RANGE": "2021-02-22/2025-12-02",
            "EVALUATED_DATE_RANGE": "2021-02-22/2025-12-02",
            "SESSION_COUNT": 1202,
            "DATA_QUALITY_STATUS": "PASS",
            "POINT_IN_TIME_STATUS": "PASS",
            "MANIFEST_REPLAY_STATUS": "PASS",
            "EXPECTED_SIGNAL_SESSION_COUNT": 1202,
            "OBSERVED_SIGNAL_SESSION_COUNT": 1202,
            "MISSING_SIGNAL_SESSION_COUNT": 0,
            "DUPLICATE_SIGNAL_SESSION_COUNT": 0,
            "UNKNOWN_SIGNAL_COUNT": 0,
            "EXPECTED_TRANSITION_COUNT": 83,
            "OBSERVED_TRANSITION_COUNT": 83,
            "OPTIONIZED_START_EQUITY_USD": Decimal("100000.00"),
            "OPTIONIZED_END_EQUITY_USD": Decimal("105000.00"),
            "OPTIONIZED_NET_PNL_USD": Decimal("5000.00"),
            "OPTIONIZED_NET_RETURN": Decimal("0.05"),
            "UNDERLYING_START_EQUITY_USD": Decimal("100000.00"),
            "UNDERLYING_END_EQUITY_USD": Decimal("104000.00"),
            "UNDERLYING_NET_PNL_USD": Decimal("4000.00"),
            "UNDERLYING_NET_RETURN": Decimal("0.04"),
            "UNDERLYING_MIN_CASH_USD": Decimal("100.00"),
            "COMPARATOR_ID": "UNDERLYING_IMPLEMENTATION",
            "COMPARATOR_VERSION": "1.0.0-draft.1",
            "COMPARATOR_CONTRACT_SHA256": authority.contract.canonical_sha256,
            "SIGNAL_IDENTITY_MATCH": True,
            "EFFECTIVE_EVENT_ALIGNMENT_COUNT": 83,
            "EFFECTIVE_EVENT_MISMATCH_COUNT": 0,
            "PRIMARY_RETURN_DELTA": Decimal("0.01"),
            "PRIMARY_DRAWDOWN_DELTA": Decimal("0"),
            "PREREGISTERED_NAMED_DIAGNOSTIC_RESULTS": [
                {"diagnostic_id": "SGOV_CARRY_COMPARATOR", "result": "fixture"},
                {"diagnostic_id": "QQQ_BUY_AND_HOLD", "result": "fixture"},
            ],
        }
    )
    return payload


def _context(
    *, authorization: AuthorizationEvidence = AuthorizationEvidence.GRANTED
) -> ReplayContext:
    authority = load_wave_a_authority()
    partitions = tuple(
        CalendarPartitionEvidence(
            partition_id=row.partition_id,
            start=row.start,
            end=row.end,
            included_exactly_once=True,
        )
        for row in authority.contract.contract.calendar_diagnostics.partitions
    )
    return ReplayContext(
        signal_hashes_exact=True,
        signal_semantics_match=True,
        option_alpha_input_detected=False,
        window_and_calendar_unchanged=True,
        dq_receipt_and_manifest_identity_exact=True,
        frozen_slot_count=37,
        frozen_slots_exact=True,
        engine_default_substituted=False,
        signal_input_lineage_complete=True,
        comparator_platform_evidence_complete=True,
        comparator_changed_after_freeze=False,
        comparator_order_submission_count=0,
        accounting_reconciled=True,
        local_option_repricing_used=False,
        risk_fields_reconciled=True,
        local_greek_reconstruction_used=False,
        export_inventory_evidence_complete=True,
        raw_option_export_detected=False,
        platform_identity_complete=True,
        platform_identity_drift=False,
        platform_identity_supported=True,
        calendar_partitions=partitions,
        diagnostic_inventory_complete=True,
        post_result_diagnostic_added=False,
        result_used_for_selection=False,
        authorization=authorization,
    )


def test_authority_and_exact_export_inventory_are_frozen() -> None:
    authority = load_wave_a_authority()

    assert authority.file_sha256 == (
        "fbedb47e5f2a748dc75669faabee9641ba7e0596de4ad8c340ed7ebcbd4c5c76"
    )
    assert authority.contract.file_sha256 == (
        "8c748634f6869eb4d4e9dfb14493acd072d146074ce7e86462eec0adae15714a"
    )
    assert authority.contract.canonical_sha256 == (
        "6f77cf17af6e435799a2e86e1fb6a81936368e053b2367efb3a8e2be13412267"
    )
    inventory = export_safe_field_inventory(authority)
    assert len(inventory) == 101
    assert len(set(inventory)) == len(inventory)
    assert inventory[0] == "RUN_ID"
    assert inventory[-1] == "PREREGISTERED_NAMED_DIAGNOSTIC_RESULTS"


def test_fully_funded_ledger_uses_ask_bid_remainder_and_explicit_fees() -> None:
    ledger = FullyFundedQQQCashLedger()
    start = datetime(2024, 1, 2, 15, 30, tzinfo=UTC)
    ledger.apply_event(
        ComparatorEvent(
            event_id="entry-1",
            occurred_at=start,
            action=ComparatorAction.LONG_CALL,
            quote=QQQQuote(bid=Decimal("99"), ask=Decimal("100")),
            fee_usd=Decimal("10"),
            option_contract_eligible=False,
        )
    )

    assert ledger.shares == 999
    assert ledger.cash == Decimal("90")

    ledger.apply_event(
        ComparatorEvent(
            event_id="exit-1",
            occurred_at=start + timedelta(days=1),
            action=ComparatorAction.FLAT,
            quote=QQQQuote(bid=Decimal("110"), ask=Decimal("111")),
            fee_usd=Decimal("10"),
            option_contract_eligible=True,
        )
    )
    aggregate = ledger.aggregate()

    assert aggregate.end_equity_usd == Decimal("109970")
    assert aggregate.net_pnl_usd == Decimal("9970.00")
    assert aggregate.net_return == Decimal("0.0997")
    assert aggregate.fees_usd == Decimal("20")
    assert aggregate.spread_slippage_cost_usd == Decimal("999")
    assert aggregate.time_in_market_minutes == Decimal("1440")
    assert aggregate.deployed_capital_holding_time == Decimal("143856000")
    assert aggregate.long_episode_count == 1
    assert aggregate.flat_episode_count == 1
    assert aggregate.effective_event_mismatch_count == 0
    assert aggregate.ending_share_count == 0


def test_ledger_fails_closed_on_bad_transition_clock_quote_or_fee() -> None:
    start = datetime(2024, 1, 2, tzinfo=UTC)
    ledger = FullyFundedQQQCashLedger()
    with pytest.raises(PairedComparisonWaveAError, match="INVALID_LEDGER_TRANSITION"):
        ledger.apply_event(
            ComparatorEvent(
                event_id="flat-first",
                occurred_at=start,
                action=ComparatorAction.FLAT,
                quote=QQQQuote(bid=Decimal("99"), ask=Decimal("100")),
                fee_usd=Decimal("0"),
                option_contract_eligible=False,
            )
        )
    with pytest.raises(PairedComparisonWaveAError, match="INVALID_EVENT"):
        ComparatorEvent(
            event_id="negative-fee",
            occurred_at=start,
            action=ComparatorAction.LONG_CALL,
            quote=QQQQuote(bid=Decimal("99"), ask=Decimal("100")),
            fee_usd=Decimal("-0.01"),
            option_contract_eligible=True,
        )
    with pytest.raises(PairedComparisonWaveAError, match="INVALID_QUOTE"):
        QQQQuote(bid=Decimal("101"), ask=Decimal("100"))


def test_aggregate_admission_is_exact_deterministic_and_aggregate_only() -> None:
    payload = _aggregate_payload()
    first = admit_export_safe_aggregate(payload)
    second = admit_export_safe_aggregate(dict(reversed(tuple(payload.items()))))

    assert first == second
    assert first.field_count == 101
    assert first.aggregate_only is True
    assert first.raw_option_rows == 0
    assert first.raw_option_exports == 0

    missing = dict(payload)
    missing.pop("RUN_ID")
    with pytest.raises(PairedComparisonWaveAError, match="EXPORT_FIELD_INVENTORY_MISMATCH"):
        admit_export_safe_aggregate(missing)

    extra = dict(payload)
    extra["EXTRA"] = 1
    with pytest.raises(PairedComparisonWaveAError, match="EXPORT_FIELD_INVENTORY_MISMATCH"):
        admit_export_safe_aggregate(extra)

    raw_nested = dict(payload)
    raw_nested["PREREGISTERED_NAMED_DIAGNOSTIC_RESULTS"] = [{"OPTION_SID": "forbidden"}]
    with pytest.raises(PairedComparisonWaveAError, match="RAW_OPTION_EXPORT_REJECTED"):
        admit_export_safe_aggregate(raw_nested)

    drifted = dict(payload)
    drifted["COMPARATOR_CONTRACT_SHA256"] = "0" * 64
    with pytest.raises(PairedComparisonWaveAError, match="AGGREGATE_AUTHORITY_DRIFT"):
        admit_export_safe_aggregate(drifted)

    malformed = dict(payload)
    malformed["QC_CODE_FILE_SHA256"] = "not-a-sha"
    with pytest.raises(PairedComparisonWaveAError, match="AGGREGATE_IDENTITY_INVALID"):
        admit_export_safe_aggregate(malformed)

    negative_count = dict(payload)
    negative_count["ENTRY_FILL_COUNT"] = -1
    with pytest.raises(PairedComparisonWaveAError, match="AGGREGATE_COUNT_INVALID"):
        admit_export_safe_aggregate(negative_count)


def test_full_synthetic_replay_has_16_pass_axes_and_zero_external_counters() -> None:
    receipt = replay_export_safe_aggregate(_aggregate_payload(), context=_context())

    assert len(receipt.axis_findings) == 16
    assert {finding.status for finding in receipt.axis_findings} == {AxisStatus.PASS}
    assert receipt.terminal_status is AxisStatus.PASS
    assert receipt.terminal_precedence == ("INVALID", "FAIL", "INSUFFICIENT", "PASS")
    assert receipt.external_actions_executed == 0
    assert receipt.backtests_executed == 0
    assert receipt.orders == receipt.fills == receipt.positions == 0
    assert receipt.canonical_sha256 == (
        "f5d30239fb2591888a9d06b08f0485b5705d7ae12b700c8e0beb765c81fd2e63"
    )


def test_actual_wave_a_authorization_boundary_remains_insufficient() -> None:
    receipt = replay_export_safe_aggregate(
        _aggregate_payload(),
        context=_context(authorization=AuthorizationEvidence.MISSING),
    )

    external = receipt.axis_findings[-1]
    assert external.axis_id == "EXTERNAL_AUTHORIZATION"
    assert external.status is AxisStatus.INSUFFICIENT
    assert receipt.terminal_status is AxisStatus.INSUFFICIENT


def test_nonpositive_estimand_is_valid_fail_without_retry() -> None:
    payload = _aggregate_payload()
    payload["PRIMARY_RETURN_DELTA"] = Decimal("0")
    payload["OPTIONIZED_END_EQUITY_USD"] = Decimal("104000.00")
    payload["OPTIONIZED_NET_PNL_USD"] = Decimal("4000.00")
    payload["OPTIONIZED_NET_RETURN"] = Decimal("0.04")
    receipt = replay_export_safe_aggregate(payload, context=_context())

    estimand = next(
        finding
        for finding in receipt.axis_findings
        if finding.axis_id == "PRIMARY_IMPLEMENTATION_ESTIMAND"
    )
    assert estimand.status is AxisStatus.FAIL
    assert receipt.terminal_status is AxisStatus.FAIL
    assert receipt.backtests_executed == 0


def test_accounting_identity_mismatch_is_a_fail() -> None:
    payload = _aggregate_payload()
    payload["UNDERLYING_END_EQUITY_USD"] = Decimal("103999.99")
    receipt = replay_export_safe_aggregate(payload, context=_context())

    accounting = next(
        finding for finding in receipt.axis_findings if finding.axis_id == "ACCOUNTING"
    )
    assert accounting.status is AxisStatus.FAIL
    assert receipt.terminal_status is AxisStatus.FAIL


def test_calendar_identity_drift_is_invalid_and_precedence_is_exact() -> None:
    context = _context()
    assert context.calendar_partitions is not None
    first = context.calendar_partitions[0]
    drifted = replace(
        context,
        calendar_partitions=(
            replace(first, start=date(2021, 2, 23)),
            *context.calendar_partitions[1:],
        ),
    )
    receipt = replay_export_safe_aggregate(_aggregate_payload(), context=drifted)

    assert receipt.terminal_status is AxisStatus.INVALID
    assert (
        reduce_axis_findings(
            (
                AxisFinding("a", AxisStatus.PASS, "fixture"),
                AxisFinding("b", AxisStatus.INSUFFICIENT, "fixture"),
                AxisFinding("c", AxisStatus.FAIL, "fixture"),
                AxisFinding("d", AxisStatus.INVALID, "fixture"),
            )
        )
        is AxisStatus.INVALID
    )
    with pytest.raises(PairedComparisonWaveAError, match="TERMINAL_PRECEDENCE_DRIFT"):
        reduce_axis_findings(
            (AxisFinding("a", AxisStatus.PASS, "fixture"),),
            terminal_precedence=("FAIL", "INVALID", "INSUFFICIENT", "PASS"),
        )


def test_quantconnect_helper_is_deterministic_non_runnable_fragment() -> None:
    first = render_quantconnect_comparator_helper_fragment()
    second = render_quantconnect_comparator_helper_fragment()

    assert first == second
    assert b"\r" not in first
    assert first.endswith(b"\n")
    assert b"QCAlgorithm" not in first
    assert b"MarketOrder(" not in first
    assert b'INITIAL_CASH_USD = Decimal("100000.00")' in first
    assert b'"RUN_ID"' in first
    assert b'"PREREGISTERED_NAMED_DIAGNOSTIC_RESULTS"' in first
    assert quantconnect_comparator_helper_sha256() == (
        "7e67d422296db8773e9b9ddb4ec4dd5278976929add1612ff2e0d69b7f042b17"
    )
    compile(first.decode("utf-8"), "qc_comparator_helper_fragment.py", "exec")


def test_no_market_or_provider_data_paths_are_imported() -> None:
    module_source = (
        load_wave_a_authority().path.parent.parent.parent
        / "src"
        / "ai_trading_system"
        / "qqq_options_research"
        / "paired_comparison_wave_a.py"
    ).read_text(encoding="utf-8")

    assert "requests" not in module_source
    assert "yfinance" not in module_source
    assert "QuantBook" not in module_source
    assert "ObjectStore" not in module_source
