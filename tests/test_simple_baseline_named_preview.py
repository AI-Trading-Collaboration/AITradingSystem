"""Prospective wrapper boundaries; synthetic descriptions do not confer authority."""

from __future__ import annotations

import builtins
import hashlib
import io
import json
import platform
import socket
import subprocess
from copy import deepcopy
from dataclasses import replace
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest
from test_named_data_quality_execution_contract import _dispatch, _receipt
from test_named_simple_baseline_preview import ROOT, _csv, _prices, _result, _scope

from ai_trading_system import simple_baseline_named_preview as preview_module
from ai_trading_system import trading_calendar
from ai_trading_system.contracts.data_quality_execution import (
    DataQualityDateWindow,
    canonical_json_value,
)
from ai_trading_system.contracts.named_data_quality_execution import (
    EQUAL_RISK_GUARD_RATE_SERIES,
    EQUAL_RISK_PRICE_REGISTRY_PATH,
    EQUAL_RISK_PRICE_SOURCE_MANIFEST_PATH,
    EQUAL_RISK_PRICE_SOURCE_MANIFEST_SHA256,
    FIVE_CANDIDATE_PREVIEW_SOURCE_MANIFEST_PATH,
    FIVE_CANDIDATE_PREVIEW_SOURCE_MANIFEST_SHA256,
    PROSPECTIVE_FIVE_CANDIDATE_SOURCE_MANIFEST_PATH,
    PROSPECTIVE_FIVE_CANDIDATE_SOURCE_MANIFEST_SHA256,
)
from ai_trading_system.contracts.named_simple_baseline_preview import FROZEN_PREVIEW_CANDIDATES
from ai_trading_system.simple_baseline_named_preview import (
    NamedSimpleBaselinePreviewError,
    _parse_registry,
    build_prospective_simple_baseline_preview,
    rebuild_prospective_simple_baseline_preview,
)
from ai_trading_system.simple_baseline_portfolio_control import (
    _dynamic_candidate_strategies,
    _target_weight_frame,
)


def _forbid(*args: object, **kwargs: object) -> Any:
    raise AssertionError("prospective preview performed forbidden I/O or accepted a description")


@pytest.mark.parametrize("description", ["legacy_dto", "json_object", "json_bytes", "signal"])
def test_prospective_preview_cannot_mint_capability_from_description(
    description: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    legacy = _result((ROOT / EQUAL_RISK_PRICE_REGISTRY_PATH).read_bytes())
    inputs: dict[str, Any] = {
        "legacy_dto": legacy,
        "json_object": legacy.to_dict(),
        "json_bytes": legacy.canonical_bytes,
        "signal": {"target_weights": {"QQQ": 1.0}, "data_quality_status": "PASS"},
    }
    with monkeypatch.context() as guard:
        guard.setattr(builtins, "open", _forbid)
        guard.setattr(io, "open", _forbid)
        guard.setattr(Path, "read_bytes", _forbid)
        guard.setattr(Path, "write_bytes", _forbid)
        guard.setattr(socket, "socket", _forbid)
        guard.setattr(subprocess, "run", _forbid)
        guard.setattr(preview_module, "_parse_registry", _forbid)
        guard.setattr(preview_module, "_price_matrix", _forbid)
        guard.setattr(preview_module, "_candidate_previews", _forbid)
        with pytest.raises(NamedSimpleBaselinePreviewError) as error:
            build_prospective_simple_baseline_preview(
                inputs[description], required_scope=legacy.scope
            )
    assert error.value.code == "NAMED_PREVIEW_VERIFIED_INPUTS_REQUIRED"
    # These objects remain ordinary descriptions; the wrapper must not mutate
    # their original clock or attach a purported prospective admission result.
    assert (
        json.loads(legacy.canonical_bytes)["schema_version"] == "named_simple_baseline_preview.v1"
    )
    assert legacy.to_dict()["observation_created"] is False


def test_prospective_preview_rejects_an_accessor_impostor(monkeypatch: pytest.MonkeyPatch) -> None:
    legacy = _result((ROOT / EQUAL_RISK_PRICE_REGISTRY_PATH).read_bytes())

    class Impostor:
        def inputs_for_prospective_five_candidate_preview(self, **kwargs: object) -> Any:
            _forbid()

    with monkeypatch.context() as guard:
        guard.setattr(preview_module, "_parse_registry", _forbid)
        with pytest.raises(NamedSimpleBaselinePreviewError, match="VERIFIED_INPUTS_REQUIRED"):
            build_prospective_simple_baseline_preview(Impostor(), required_scope=legacy.scope)  # type: ignore[arg-type]


def _rebuild_inputs(*, end: date = date(2026, 9, 3), extra_rows: bool = False) -> dict[str, Any]:
    """Synthetic receipt/dispatch descriptions, never an executed DQ or verifier seal."""
    registry = (ROOT / EQUAL_RISK_PRICE_REGISTRY_PATH).read_bytes()
    scope = _scope(registry, end)
    sessions = tuple(
        stamp.date()
        for stamp in pd.date_range(scope.requested_window.start, end)
        if trading_calendar.is_us_equity_trading_day(stamp.date())
    )
    next_session = end + timedelta(days=1)
    while not trading_calendar.is_us_equity_trading_day(next_session):
        next_session += timedelta(days=1)
    prices = _prices(n=len(sessions))
    prices.index = pd.DatetimeIndex(sessions)
    content = _csv(prices)
    if extra_rows:
        content += b"2021-02-19,QQQ,99\n2021-02-22,SPY,99\n"
    original = _receipt()
    execution = replace(
        original.execution,
        source_manifest_path=PROSPECTIVE_FIVE_CANDIDATE_SOURCE_MANIFEST_PATH,
        source_manifest_sha256=PROSPECTIVE_FIVE_CANDIDATE_SOURCE_MANIFEST_SHA256,
    )
    evaluated = DataQualityDateWindow(scope.requested_window.start, sessions[-2])
    request = replace(
        original.request,
        source_manifest_path=execution.source_manifest_path,
        source_manifest_sha256=execution.source_manifest_sha256,
        scope=replace(
            original.request.scope,
            as_of=end,
            requested_window=scope.requested_window,
            expected_rate_series=EQUAL_RISK_GUARD_RATE_SERIES,
        ),
        expected_evaluated_window=evaluated,
    )
    inputs = tuple(
        replace(
            item,
            member=replace(
                item.member, sha256=hashlib.sha256(content).hexdigest(), size_bytes=len(content)
            ),
            row_count=len(sessions) * 3 + (2 if extra_rows else 0),
            observed_min_date=date(2021, 2, 19) if extra_rows else scope.requested_window.start,
            observed_max_date=end,
        )
        if item.role == "prices"
        else replace(item, observed_max_date=sessions[-2])
        for item in original.inputs
    )
    receipt = replace(
        original,
        request=request,
        evaluated_window=evaluated,
        publication=replace(original.publication, transaction_window=scope.requested_window),
        inputs=inputs,
        execution=execution,
        execution_dependencies=original.execution_dependencies + (scope.registry_binding,),
        data_quality_evidence=replace(original.data_quality_evidence, as_of=end),
    )
    return {
        "receipt": receipt,
        "successful_dispatch": _dispatch(receipt),
        "required_scope": scope,
        "prices": content,
        "registry": registry,
        "sessions": sessions,
        "next_session": next_session,
    }


def _replace_receipt(values: dict[str, Any], **changes: Any) -> dict[str, Any]:
    receipt = replace(values["receipt"], **changes)
    return {**values, "receipt": receipt, "successful_dispatch": _dispatch(receipt)}


def _rebind_prices(values: dict[str, Any], content: bytes) -> dict[str, Any]:
    inputs = tuple(
        replace(
            item,
            member=replace(
                item.member, sha256=hashlib.sha256(content).hexdigest(), size_bytes=len(content)
            ),
            row_count=len(content.splitlines()) - 1,
        )
        if item.role == "prices"
        else item
        for item in values["receipt"].inputs
    )
    return {**_replace_receipt(values, inputs=inputs), "prices": content}


@pytest.mark.parametrize("end", (date(2022, 4, 1), date(2022, 4, 18), date(2025, 1, 8)))
def test_retained_rebuild_preserves_all_frozen_targets_and_full_result_identity(end: date) -> None:
    values = _rebuild_inputs(end=end)
    result = rebuild_prospective_simple_baseline_preview(**values)
    receipt, scope = values["receipt"], values["required_scope"]
    config = _parse_registry(values["registry"])
    prices = pd.read_csv(io.BytesIO(values["prices"]), parse_dates=["date"]).pivot(
        index="date", columns="ticker", values="adj_close"
    )
    definitions = {
        row["strategy_id"]: row
        for row in [*config["strategies"], *_dynamic_candidate_strategies(config)]
    }
    candidates = result["candidates"]
    assert [
        (row["candidate_id"], row["candidate_role"], row["registry_strategy_id"])
        for row in candidates
    ] == list(FROZEN_PREVIEW_CANDIDATES)
    for row in candidates:
        expected = _target_weight_frame(definitions[row["registry_strategy_id"]], prices, config)
        assert row["target_weights"] == pytest.approx(expected.iloc[-1].to_dict())
        assert row["target_session"] == end.isoformat()
        assert row["legacy_applied_session"] == values["next_session"].isoformat()
        dynamic = row["candidate_id"] == "dyn_tqqq_capped_trend"
        rebalance = (
            end
            if dynamic
            else next(
                session
                for session in values["sessions"]
                if (session.year, session.month) == (end.year, end.month)
            )
        )
        assert row["target_rebalance_session"] == rebalance.isoformat()
        assert row["available_at_rebalance_price_rows"] == values["sessions"].index(rebalance) + 1
        assert {
            "candidate_id",
            "candidate_role",
            "registry_strategy_id",
            "rebalance_frequency",
            "target_session",
            "target_rebalance_session",
            "signal_price_through",
            "legacy_applied_session",
            "target_weights",
            "required_lookback_price_rows",
            "available_at_rebalance_price_rows",
            "zero_volatility_initialization",
        } == set(row)
    assert result["schema_version"] == "prospective_five_candidate_preview.v1"
    assert result["scope"] == scope.to_dict()
    assert result["data_quality_receipt"] == receipt.to_dict()
    assert result["data_quality_receipt_id"] == receipt.receipt_id
    assert result["successful_original_dispatch"] == values["successful_dispatch"].to_dict()
    assert result["execution_identity"] == receipt.execution.to_dict()
    assert (
        result["requested_window"]
        == result["consumed_price_window"]
        == scope.requested_window.to_dict()
    )
    assert result["original_dq_evaluated_window"] == receipt.evaluated_window.to_dict()
    assert receipt.evaluated_window.end < end
    assert result["source_row_count"] == result["consumed_row_count"] == len(values["sessions"]) * 3
    assert (
        result["excluded_before_window_row_count"] == result["excluded_other_ticker_row_count"] == 0
    )
    assert result["feature_session"] == end.isoformat()
    assert result["decision_effective_session"] == values["next_session"].isoformat()
    assert result["timing_version"] == "NEXT_XNYS_CLOSE_FORWARD_V1"
    assert result["return_clock"] == "EFFECTIVE_SESSION_CLOSE_TO_NEXT_XNYS_SESSION_CLOSE"
    assert result["legacy_return_equivalent"] is False
    assert result["legacy_applied_session_is_execution_evidence"] is False
    assert result["recompute_target_at_effective_session"] is False
    assert result["carried_target_is_rebalance_evidence"] is False
    assert result["calculation_environment"] == {
        "python_implementation": platform.python_implementation(),
        "python_version": platform.python_version(),
        "pandas_version": pd.__version__,
        "numpy_version": np.__version__,
    }
    unsigned = {key: value for key, value in result.items() if key != "preview_id"}
    assert (
        result["preview_id"]
        == "prospective_five_candidate_preview_"
        + hashlib.sha256(canonical_json_value(unsigned).encode("utf-8")).hexdigest()
    )
    assert json.loads(canonical_json_value(result)) == result
    assert result == rebuild_prospective_simple_baseline_preview(**values)
    if end == date(2025, 1, 8):
        assert result["decision_effective_session"] == "2025-01-10"


def test_retained_rebuild_is_pure_and_never_becomes_a_live_capability(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    values = _rebuild_inputs(extra_rows=True)
    with monkeypatch.context() as guard:
        guard.setattr(builtins, "open", _forbid)
        guard.setattr(io, "open", _forbid)
        guard.setattr(Path, "read_bytes", _forbid)
        guard.setattr(Path, "write_bytes", _forbid)
        guard.setattr(socket, "socket", _forbid)
        guard.setattr(subprocess, "run", _forbid)
        guard.setattr(pd, "read_csv", _forbid)
        guard.setattr(trading_calendar, "is_us_equity_trading_day", _forbid)
        guard.setattr(trading_calendar, "us_equity_market_session", _forbid)
        result = rebuild_prospective_simple_baseline_preview(**values)
        with pytest.raises(NamedSimpleBaselinePreviewError, match="VERIFIED_INPUTS_REQUIRED"):
            build_prospective_simple_baseline_preview(
                result, required_scope=values["required_scope"]
            )  # type: ignore[arg-type]
    assert result["source_row_count"] == result["consumed_row_count"] + 2
    assert (
        result["excluded_before_window_row_count"] == result["excluded_other_ticker_row_count"] == 1
    )
    for field in (
        "observation_authorized",
        "verified_input_seal_exported",
        "consumer_cutover_allowed",
        "dispatch_allowed",
        "dq_validation_executed",
        "observation_created",
        "outcome_read",
        "returns_computed",
        "timing_evidence_established",
    ):
        assert result[field] is False
    assert result["pit_status"] == result["oos_status"] == "NOT_ESTABLISHED"
    assert result["read_only_plan"]["rates_role"] == "DQ_GUARD_ONLY"
    assert result["production_effect"] == result["broker_action"] == "none"


@pytest.mark.parametrize("field", ("receipt", "successful_dispatch", "required_scope"))
def test_retained_rebuild_requires_typed_descriptions_not_json(field: str) -> None:
    values = _rebuild_inputs()
    values[field] = values[field].to_dict()
    with pytest.raises(NamedSimpleBaselinePreviewError, match="EVIDENCE_TYPES_REQUIRED"):
        rebuild_prospective_simple_baseline_preview(**values)


@pytest.mark.parametrize(
    "damage",
    (
        "price_bytes",
        "price_mutable",
        "price_row_count",
        "registry_bytes",
        "registry_mutable",
        "registry_dependency",
        "dispatch",
    ),
)
def test_retained_rebuild_rejects_changed_bytes_or_unmatched_evidence(damage: str) -> None:
    values = _rebuild_inputs()
    if damage == "price_bytes":
        values["prices"] += b"2021-02-19,QQQ,99\n"
    elif damage == "price_mutable":
        values["prices"] = bytearray(values["prices"])
    elif damage == "price_row_count":
        values = _replace_receipt(
            values,
            inputs=tuple(
                replace(item, row_count=item.row_count + 1) if item.role == "prices" else item
                for item in values["receipt"].inputs
            ),
        )
    elif damage == "registry_bytes":
        values["registry"] += b"# unreviewed\n"
    elif damage == "registry_mutable":
        values["registry"] = bytearray(values["registry"])
    elif damage == "registry_dependency":
        values = _replace_receipt(
            values,
            execution_dependencies=tuple(
                item
                for item in values["receipt"].execution_dependencies
                if item.relative_path != EQUAL_RISK_PRICE_REGISTRY_PATH
            ),
        )
    else:
        values["successful_dispatch"] = _dispatch(_receipt())
    with pytest.raises(ValueError):
        rebuild_prospective_simple_baseline_preview(**values)


@pytest.mark.parametrize("profile", ("old57", "old59"))
def test_retained_rebuild_does_not_expand_original_source_profiles(profile: str) -> None:
    values = _rebuild_inputs()
    path, digest = {
        "old57": (EQUAL_RISK_PRICE_SOURCE_MANIFEST_PATH, EQUAL_RISK_PRICE_SOURCE_MANIFEST_SHA256),
        "old59": (
            FIVE_CANDIDATE_PREVIEW_SOURCE_MANIFEST_PATH,
            FIVE_CANDIDATE_PREVIEW_SOURCE_MANIFEST_SHA256,
        ),
    }[profile]
    receipt = values["receipt"]
    values = _replace_receipt(
        values,
        execution=replace(
            receipt.execution, source_manifest_path=path, source_manifest_sha256=digest
        ),
        request=replace(receipt.request, source_manifest_path=path, source_manifest_sha256=digest),
    )
    with pytest.raises(NamedSimpleBaselinePreviewError, match="SOURCE_PROFILE_MISMATCH"):
        rebuild_prospective_simple_baseline_preview(**values)


@pytest.mark.parametrize("damage", ("two_assets", "one_rate", "tail_window"))
def test_retained_rebuild_rejects_a_partial_original_dq_scope(damage: str) -> None:
    values = _rebuild_inputs()
    receipt = values["receipt"]
    changes = {
        "two_assets": {"expected_price_tickers": ("QQQ", "SGOV")},
        "one_rate": {"expected_rate_series": ("DGS10",)},
        "tail_window": {
            "requested_window": DataQualityDateWindow(
                date(2021, 2, 23), receipt.request.scope.as_of
            )
        },
    }[damage]
    request = replace(
        receipt.request,
        scope=replace(receipt.request.scope, **changes),
        expected_evaluated_window=None,
    )
    evaluated = DataQualityDateWindow(
        request.scope.requested_window.start, receipt.evaluated_window.end
    )
    values = _replace_receipt(values, request=request, evaluated_window=evaluated)
    with pytest.raises(NamedSimpleBaselinePreviewError, match="RECEIPT_SCOPE_MISMATCH"):
        rebuild_prospective_simple_baseline_preview(**values)


@pytest.mark.parametrize("status", ("PASS_WITH_WARNINGS", "FAIL"))
def test_retained_rebuild_requires_strict_pass_even_with_correlated_dispatch(status: str) -> None:
    values = _rebuild_inputs()
    receipt = values["receipt"]
    failed = status == "FAIL"
    report = replace(
        receipt.report,
        status=status,
        error_count=1 if failed else 0,
        warning_count=0 if failed else 1,
        issue_codes=("SYNTHETIC_DQ_ISSUE",),
        blocking_issue_codes=("SYNTHETIC_DQ_ISSUE",) if failed else (),
    )
    evidence = replace(
        receipt.data_quality_evidence,
        status=status,
        passed=not failed,
        error_count=report.error_count,
        warning_count=report.warning_count,
        blocking_issues=report.blocking_issue_codes,
    )
    values = _replace_receipt(values, report=report, data_quality_evidence=evidence)
    with pytest.raises(NamedSimpleBaselinePreviewError, match="DQ_NOT_PASS"):
        rebuild_prospective_simple_baseline_preview(**values)


@pytest.mark.parametrize(
    "damage", ("mutable_sessions", "missing_last", "duplicate", "next_not_later")
)
def test_retained_rebuild_requires_a_complete_immutable_ordered_calendar_witness(
    damage: str,
) -> None:
    values = _rebuild_inputs()
    if damage == "mutable_sessions":
        values["sessions"] = list(values["sessions"])
    elif damage == "missing_last":
        values["sessions"] = values["sessions"][:-1]
    elif damage == "duplicate":
        values["sessions"] = values["sessions"] + values["sessions"][-1:]
    else:
        values["next_session"] = values["required_scope"].as_of
    with pytest.raises(ValueError):
        rebuild_prospective_simple_baseline_preview(**values)


@pytest.mark.parametrize("damage", ("missing_date", "duplicate_key", "future_date", "nan"))
def test_rebound_price_hash_cannot_hide_invalid_calculation_inputs(damage: str) -> None:
    values = _rebuild_inputs()
    lines = values["prices"].splitlines(keepends=True)
    if damage == "missing_date":
        missing = values["sessions"][10].isoformat().encode() + b","
        content = b"".join(line for line in lines if not line.startswith(missing))
    elif damage == "duplicate_key":
        content = b"".join(lines + [lines[1]])
    elif damage == "future_date":
        content = b"".join(lines) + f"{values['next_session']},QQQ,100\n".encode()
    else:
        first = lines[1].decode().strip().split(",")
        lines[1] = f"{first[0]},{first[1]},nan\n".encode()
        content = b"".join(lines)
    with pytest.raises(NamedSimpleBaselinePreviewError):
        rebuild_prospective_simple_baseline_preview(**_rebind_prices(values, content))


def test_prospective_rebuild_cannot_weaken_the_original_59_profile_dto() -> None:
    values = _rebuild_inputs()
    legacy = _result(values["registry"])
    with pytest.raises(ValueError, match="scope/registry/source identity"):
        replace(
            legacy, receipt=values["receipt"], successful_dispatch=values["successful_dispatch"]
        )


@pytest.mark.parametrize(
    "damage", ("weight", "rebalance", "window", "environment", "safety", "preview_id")
)
def test_independent_rebuild_does_not_trust_rehashed_recorded_signal_fields(damage: str) -> None:
    values = _rebuild_inputs()
    truth = rebuild_prospective_simple_baseline_preview(**values)
    retained = deepcopy(truth)
    if damage == "weight":
        retained["candidates"][0]["target_weights"] = {"QQQ": 0.5, "SGOV": 0.5}
    elif damage == "rebalance":
        retained["candidates"][0]["target_rebalance_session"] = values[
            "required_scope"
        ].as_of.isoformat()
    elif damage == "window":
        retained["original_dq_evaluated_window"]["end"] = values["required_scope"].as_of.isoformat()
    elif damage == "environment":
        retained["calculation_environment"]["pandas_version"] = "unverified-version"
    elif damage == "safety":
        retained["outcome_read"] = True
    else:
        retained["preview_id"] = "prospective_five_candidate_preview_" + "0" * 64
    if damage != "preview_id":
        unsigned = {key: value for key, value in retained.items() if key != "preview_id"}
        retained["preview_id"] = (
            "prospective_five_candidate_preview_"
            + hashlib.sha256(canonical_json_value(unsigned).encode()).hexdigest()
        )
    # The coordinator compares these complete canonical bytes, not merely the
    # forged result's now internally consistent hash or selected weight fields.
    assert canonical_json_value(retained) != canonical_json_value(
        rebuild_prospective_simple_baseline_preview(**values)
    )
