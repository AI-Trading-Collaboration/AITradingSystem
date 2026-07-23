from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from ai_trading_system.config import (
    PROJECT_ROOT as REAL_PROJECT_ROOT,
)
from ai_trading_system.config import (
    configured_price_tickers,
    configured_rate_series,
    load_universe,
)
from ai_trading_system.contracts.data_quality import DataQualityEvidence
from ai_trading_system.contracts.data_quality_execution import (
    DAILY_DEFAULT_DATA_QUALITY_EXECUTION_PROFILE_ID,
    MANUAL_DATA_QUALITY_EXECUTION_PROFILE_ID,
    DataQualityDateWindow,
    DataQualityExecutionReceipt,
    DataQualityImplementationSourceBinding,
    DataQualityInputBinding,
    DataQualityInvocationParameter,
    DataQualityPolicyBinding,
    DataQualityReportBinding,
    DataQualityValidatorBinding,
)
from ai_trading_system.contracts.status import PolicyRole
from ai_trading_system.data import quality_execution_discovery
from ai_trading_system.data.quality import DataFileSummary, DataQualityReport
from ai_trading_system.data.quality_execution import (
    CanonicalDataQualityExecutionResult,
    DataQualityExecutionError,
)
from ai_trading_system.data.quality_execution_discovery import (
    DEFAULT_DATA_QUALITY_EXECUTION_PROFILE_ID,
    DiscoveredDataQualityExecution,
    default_data_quality_execution_discovery_path,
    load_default_data_quality_execution_discovery,
    publish_default_data_quality_execution_discovery,
)

AS_OF = date(2026, 7, 22)
STARTED_AT = datetime(2026, 7, 22, 20, 0, tzinfo=UTC)
CHECKED_AT = STARTED_AT + timedelta(seconds=1)
ENDED_AT = STARTED_AT + timedelta(seconds=2)
PUBLISHED_AT = STARTED_AT + timedelta(seconds=3)
SHA_A = "a" * 64
SHA_B = "b" * 64
SHA_C = "c" * 64
SHA_D = "d" * 64


def test_publish_and_load_default_discovery_pointer_is_exact_and_status_free(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result = _execution_result(tmp_path)
    monkeypatch.setattr(quality_execution_discovery, "_utc_now", lambda: PUBLISHED_AT)

    discovered = publish_default_data_quality_execution_discovery(result, project_root=tmp_path)
    loaded = load_default_data_quality_execution_discovery(AS_OF, project_root=tmp_path)

    expected_path = (
        tmp_path / "outputs/data_quality/executions/discovery/daily_default/2026-07-22/current.json"
    )
    assert isinstance(discovered, DiscoveredDataQualityExecution)
    assert discovered == loaded
    assert discovered.pointer_path == expected_path
    assert discovered.receipt_path == result.receipt_path
    assert discovered.receipt == result.receipt
    assert discovered.pointer.profile_id == DEFAULT_DATA_QUALITY_EXECUTION_PROFILE_ID
    assert discovered.pointer.published_at == PUBLISHED_AT
    assert expected_path.read_bytes() == discovered.pointer.canonical_bytes
    pointer_payload = json.loads(expected_path.read_text(encoding="utf-8"))
    assert set(pointer_payload) == {
        "schema_version",
        "profile_id",
        "as_of",
        "published_at",
        "receipt_id",
        "receipt_path",
        "receipt_sha256",
        "receipt_size_bytes",
    }
    assert "status" not in pointer_payload
    assert "passed" not in pointer_payload
    assert "evidence_id" not in pointer_payload


def test_default_discovery_path_is_profile_and_as_of_isolated(tmp_path: Path) -> None:
    assert default_data_quality_execution_discovery_path(AS_OF, project_root=tmp_path) == (
        tmp_path.resolve()
        / "outputs/data_quality/executions/discovery/daily_default/2026-07-22/current.json"
    )
    with pytest.raises(DataQualityExecutionError, match="DQ_RECEIPT_FIELDS_INVALID"):
        default_data_quality_execution_discovery_path(
            datetime(2026, 7, 22, tzinfo=UTC), project_root=tmp_path
        )


@pytest.mark.parametrize(
    ("mutation", "expected_code"),
    [
        ("duplicate", "DQ_RECEIPT_FIELDS_INVALID"),
        ("unknown", "DQ_RECEIPT_FIELDS_INVALID"),
        ("noncanonical", "DQ_RECEIPT_FIELDS_INVALID"),
        ("type", "DQ_RECEIPT_FIELDS_INVALID"),
        ("nonfinite", "DQ_RECEIPT_FIELDS_INVALID"),
        ("non_utc", "DQ_EXECUTION_CHRONOLOGY_INVALID"),
        ("path", "DQ_RECEIPT_FIELDS_INVALID"),
        ("hash", "DQ_RECEIPT_ID_MISMATCH"),
        ("size", "DQ_RECEIPT_ID_MISMATCH"),
        ("profile", "DQ_RECEIPT_FIELDS_INVALID"),
        ("as_of", "DQ_AS_OF_MISMATCH"),
    ],
)
def test_loader_rejects_pointer_tamper_and_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
    expected_code: str,
) -> None:
    result = _execution_result(tmp_path)
    monkeypatch.setattr(quality_execution_discovery, "_utc_now", lambda: PUBLISHED_AT)
    discovered = publish_default_data_quality_execution_discovery(result, project_root=tmp_path)
    pointer_path = discovered.pointer_path
    payload = json.loads(pointer_path.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)

    if mutation == "duplicate":
        content = pointer_path.read_bytes().replace(
            b'{\n  "as_of":', b'{\n  "as_of": "2026-07-22",\n  "as_of":', 1
        )
    elif mutation == "noncanonical":
        content = pointer_path.read_bytes().rstrip()
    elif mutation == "nonfinite":
        content = pointer_path.read_bytes().replace(
            str(payload["receipt_size_bytes"]).encode("ascii"), b"NaN", 1
        )
    else:
        _mutate_payload(payload, mutation)
        content = _canonical_json_bytes(payload)
    pointer_path.write_bytes(content)

    with pytest.raises(DataQualityExecutionError, match=expected_code):
        load_default_data_quality_execution_discovery(AS_OF, project_root=tmp_path)


def test_loader_rejects_receipt_byte_tamper_after_pointer_publish(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result = _execution_result(tmp_path)
    monkeypatch.setattr(quality_execution_discovery, "_utc_now", lambda: PUBLISHED_AT)
    discovered = publish_default_data_quality_execution_discovery(result, project_root=tmp_path)
    discovered.receipt_path.write_bytes(discovered.receipt_path.read_bytes() + b"\n")

    with pytest.raises(DataQualityExecutionError, match="DQ_RECEIPT_ID_MISMATCH"):
        load_default_data_quality_execution_discovery(AS_OF, project_root=tmp_path)


def test_publish_rereads_receipt_and_rejects_missing_or_memory_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result = _execution_result(tmp_path)
    monkeypatch.setattr(quality_execution_discovery, "_utc_now", lambda: PUBLISHED_AT)
    result.receipt_path.unlink()
    with pytest.raises(DataQualityExecutionError, match="DQ_RECEIPT_MISSING"):
        publish_default_data_quality_execution_discovery(result, project_root=tmp_path)

    result = _execution_result(tmp_path)
    drifted_receipt = replace(result.receipt, run_id="different-run-id")
    drifted_result = replace(result, receipt=drifted_receipt)
    with pytest.raises(DataQualityExecutionError, match="DQ_RECEIPT_ID_MISMATCH"):
        publish_default_data_quality_execution_discovery(drifted_result, project_root=tmp_path)


def test_publish_rejects_non_utc_or_pre_completion_publication_time(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result = _execution_result(tmp_path)
    monkeypatch.setattr(
        quality_execution_discovery,
        "_utc_now",
        lambda: ENDED_AT - timedelta(microseconds=1),
    )
    with pytest.raises(DataQualityExecutionError, match="DQ_EXECUTION_CHRONOLOGY_INVALID"):
        publish_default_data_quality_execution_discovery(result, project_root=tmp_path)

    monkeypatch.setattr(
        quality_execution_discovery,
        "_utc_now",
        lambda: datetime(2026, 7, 23, 8, 0, tzinfo=timezone(timedelta(hours=9))),
    )
    with pytest.raises(DataQualityExecutionError, match="DQ_EXECUTION_CHRONOLOGY_INVALID"):
        publish_default_data_quality_execution_discovery(result, project_root=tmp_path)


def test_manual_execution_cannot_publish_default_discovery(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result = _execution_result(tmp_path, profile_id=MANUAL_DATA_QUALITY_EXECUTION_PROFILE_ID)
    monkeypatch.setattr(quality_execution_discovery, "_utc_now", lambda: PUBLISHED_AT)

    with pytest.raises(DataQualityExecutionError, match="DQ_RECEIPT_FIELDS_INVALID"):
        publish_default_data_quality_execution_discovery(result, project_root=tmp_path)


def test_publish_rejects_discovery_parent_symlink_escape(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_root = tmp_path / "project"
    outside = tmp_path / "outside"
    project_root.mkdir()
    outside.mkdir()
    result = _execution_result(project_root)
    discovery_parent = project_root / "outputs/data_quality/executions/discovery"
    try:
        os.symlink(outside, discovery_parent, target_is_directory=True)
    except OSError as exc:
        pytest.skip(f"directory symlink creation unavailable: {exc}")
    monkeypatch.setattr(quality_execution_discovery, "_utc_now", lambda: PUBLISHED_AT)

    with pytest.raises(DataQualityExecutionError, match="DQ_RECEIPT_FIELDS_INVALID"):
        publish_default_data_quality_execution_discovery(result, project_root=project_root)
    assert not any(outside.rglob("current.json"))


def test_missing_default_pointer_is_fail_closed(tmp_path: Path) -> None:
    with pytest.raises(DataQualityExecutionError, match="DQ_RECEIPT_MISSING"):
        load_default_data_quality_execution_discovery(AS_OF, project_root=tmp_path)


def _mutate_payload(payload: dict[str, Any], mutation: str) -> None:
    mutations: dict[str, Callable[[], None]] = {
        "unknown": lambda: payload.__setitem__("status", "PASS"),
        "type": lambda: payload.__setitem__("receipt_size_bytes", "123"),
        "non_utc": lambda: payload.__setitem__("published_at", "2026-07-23T08:00:00+09:00"),
        "path": lambda: payload.__setitem__(
            "receipt_path", "outputs/data_quality/executions/../receipt.json"
        ),
        "hash": lambda: payload.__setitem__("receipt_sha256", SHA_A),
        "size": lambda: payload.__setitem__(
            "receipt_size_bytes", int(payload["receipt_size_bytes"]) + 1
        ),
        "profile": lambda: payload.__setitem__("profile_id", "manual.v1"),
        "as_of": lambda: payload.__setitem__("as_of", "2026-07-21"),
    }
    mutations[mutation]()


def _execution_result(
    root: Path,
    *,
    profile_id: str = DAILY_DEFAULT_DATA_QUALITY_EXECUTION_PROFILE_ID,
) -> CanonicalDataQualityExecutionResult:
    universe_path = root / "config/universe.yaml"
    universe_path.parent.mkdir(parents=True, exist_ok=True)
    universe_bytes = (REAL_PROJECT_ROOT / "config/universe.yaml").read_bytes()
    universe_path.write_bytes(universe_bytes)
    receipt = _receipt(
        profile_id=profile_id,
        universe_sha256=hashlib.sha256(universe_bytes).hexdigest(),
    )
    receipt_path = root / f"outputs/data_quality/executions/{receipt.receipt_id}/receipt.json"
    receipt_path.parent.mkdir(parents=True, exist_ok=True)
    receipt_path.write_bytes(receipt.canonical_bytes)
    missing_prices = DataFileSummary(path=root / "prices.csv", exists=False)
    missing_rates = DataFileSummary(path=root / "rates.csv", exists=False)
    report = DataQualityReport(
        checked_at=CHECKED_AT,
        as_of=AS_OF,
        price_summary=missing_prices,
        rate_summary=missing_rates,
        expected_price_tickers=("QQQ",),
        expected_rate_series=("DGS10",),
    )
    return CanonicalDataQualityExecutionResult(
        receipt=receipt,
        receipt_path=receipt_path,
        report=report,
        report_path=root / "outputs/data_quality/reports/report.md",
    )


def _receipt(
    *,
    profile_id: str = DAILY_DEFAULT_DATA_QUALITY_EXECUTION_PROFILE_ID,
    universe_sha256: str = SHA_D,
) -> DataQualityExecutionReceipt:
    policy = DataQualityPolicyBinding(
        policy_id="DATA_QUALITY_CACHE_GATE",
        policy_version="data_quality_cache_gate.v1",
        status="REVIEWED",
        owner="data_platform_owner",
        role=PolicyRole.DATA_QUALITY,
        path="config/data_quality.yaml",
        sha256=SHA_A,
    )
    report = DataQualityReportBinding(
        path="outputs/data_quality/reports/report.md",
        sha256=SHA_C,
        size_bytes=100,
        status="PASS",
        error_count=0,
        warning_count=0,
        info_count=0,
    )
    evidence = DataQualityEvidence(
        contract_id="cached_market_macro_validation",
        policy_id=policy.policy_id,
        policy_version=policy.policy_version,
        status="PASS",
        passed=True,
        checked_at=CHECKED_AT,
        as_of=AS_OF,
        report_path=report.path,
        report_sha256=report.sha256,
        checked_input_count=3,
    )
    return DataQualityExecutionReceipt(
        run_id="dq-run-discovery-fixture",
        contract_id=evidence.contract_id,
        started_at=STARTED_AT,
        ended_at=ENDED_AT,
        checked_at=CHECKED_AT,
        as_of=AS_OF,
        requested_window=DataQualityDateWindow(date(2021, 2, 22), AS_OF),
        evaluated_window=DataQualityDateWindow(date(2021, 2, 22), AS_OF),
        policy=policy,
        validator=DataQualityValidatorBinding(
            validator_id="aits.validate-data",
            validator_version="quality_execution.run_canonical_data_quality_execution.v1",
            entrypoint=(
                "ai_trading_system.data.quality_execution:" "run_canonical_data_quality_execution"
            ),
            implementation_sources=(
                DataQualityImplementationSourceBinding(
                    path="src/ai_trading_system/data/quality.py", sha256=SHA_A
                ),
                DataQualityImplementationSourceBinding(
                    path="src/ai_trading_system/data/quality_execution.py", sha256=SHA_B
                ),
            ),
        ),
        invocation=tuple(
            DataQualityInvocationParameter.from_value(name, value)
            for name, value in {
                "as_of": AS_OF.isoformat(),
                "backtest_manifest_path": None,
                "evaluated_window": DataQualityDateWindow(date(2021, 2, 22), AS_OF).to_dict(),
                "execution_profile_id": profile_id,
                "execution_profile_config_path": (
                    "config/universe.yaml"
                    if profile_id == DAILY_DEFAULT_DATA_QUALITY_EXECUTION_PROFILE_ID
                    else None
                ),
                "execution_profile_config_sha256": (
                    universe_sha256
                    if profile_id == DAILY_DEFAULT_DATA_QUALITY_EXECUTION_PROFILE_ID
                    else None
                ),
                "expected_price_tickers": configured_price_tickers(load_universe()),
                "expected_rate_series": configured_rate_series(load_universe()),
                "manifest_path": "data/raw/download_manifest.csv",
                "policy_path": "config/data_quality.yaml",
                "prices_path": "data/raw/prices_daily.csv",
                "rates_path": "data/raw/rates_daily.csv",
                "requested_window": DataQualityDateWindow(date(2021, 2, 22), AS_OF).to_dict(),
                "require_secondary_prices": True,
                "secondary_prices_path": "data/raw/prices_marketstack_daily.csv",
            }.items()
        ),
        inputs=(
            DataQualityInputBinding(
                role="prices",
                path="data/raw/prices_daily.csv",
                exists=True,
                schema_id="prices_daily.v1",
                source_role="primary_market_prices",
                sha256=SHA_A,
                size_bytes=100,
                row_count=10,
                manifest_path="data/raw/download_manifest.csv",
                manifest_sha256=SHA_D,
                matched_source_ids=("fixture_prices",),
                matched_record_refs=(f"manifest_record_{'a' * 16}",),
            ),
            DataQualityInputBinding(
                role="rates",
                path="data/raw/rates_daily.csv",
                exists=True,
                schema_id="rates_daily.v1",
                source_role="primary_macro_rates",
                sha256=SHA_B,
                size_bytes=80,
                row_count=8,
                manifest_path="data/raw/download_manifest.csv",
                manifest_sha256=SHA_D,
                matched_source_ids=("fixture_rates",),
                matched_record_refs=(f"manifest_record_{'b' * 16}",),
            ),
            DataQualityInputBinding(
                role="secondary_prices",
                path="data/raw/prices_marketstack_daily.csv",
                exists=True,
                schema_id="prices_daily.v1",
                source_role="secondary_market_prices",
                sha256=SHA_C,
                size_bytes=90,
                row_count=9,
                manifest_path="data/raw/download_manifest.csv",
                manifest_sha256=SHA_D,
                matched_source_ids=("fixture_marketstack_prices",),
                matched_record_refs=(f"manifest_record_{'c' * 16}",),
            ),
        ),
        report=report,
        data_quality_evidence=evidence,
        dq_execution_provenance_verified=True,
        consumer_cutover_allowed=False,
        production_effect="none",
    )


def _canonical_json_bytes(payload: dict[str, Any]) -> bytes:
    return (
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + "\n"
    ).encode("utf-8")
