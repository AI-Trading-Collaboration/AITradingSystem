from __future__ import annotations

import csv
import hashlib
import json
import shutil
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Literal

import pytest
import yaml
from pydantic import ValidationError

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.contracts.data_quality_execution import DataQualityDateWindow
from ai_trading_system.data import quality_execution
from ai_trading_system.data.download_publication import (
    DownloadArtifactCandidate,
    DownloadSourceBinding,
    publish_download_transaction,
)
from ai_trading_system.data.quality import (
    DataFileSummary,
    DataQualityIssue,
    DataQualityReport,
    Severity,
)
from ai_trading_system.data.quality_execution import (
    CanonicalDataQualityExecutionRequest,
    run_canonical_data_quality_execution,
)
from ai_trading_system.qqq_options_research.contracts import (
    DailySignalRecord,
    QQQOptionsContractError,
    RunManifestRecord,
)
from ai_trading_system.qqq_options_research.signal_package import (
    DEFAULT_QQQ_OPTIONS_SIGNAL_EXPORT_POLICY_PATH,
    NormalizedDailySignalInput,
    QQQOptionsSignalIndex,
    QQQOptionsSignalPackageReceipt,
    SignalSourceArtifact,
    build_qqq_options_signal_package,
    canonical_normalized_signal_source_bytes,
    load_qqq_options_signal_export_policy,
    next_us_equity_trading_session,
    resolve_normalized_signal_effective_session,
    write_qqq_options_signal_package,
)

_REPOSITORY_SHA = "a" * 40
_DQ_BYTES_STATUS = Literal["PASS", "PASS_WITH_WARNINGS", "FAIL"]
_START = date(2021, 2, 22)
_END = date(2021, 2, 26)
_CHECKED_AT = datetime(2021, 2, 26, 23, 0, tzinfo=UTC)
_CREATED_AT = datetime(2021, 3, 1, 12, 0, tzinfo=UTC)


def test_signal_export_v2_binds_current_reviewed_calendar() -> None:
    loaded = load_qqq_options_signal_export_policy(
        Path("config/research/qqq_options_signal_export_v2.yaml")
    )

    assert loaded.policy.policy_id == "qqq_options_signal_export_v2"
    assert loaded.policy.policy_version == "2.0.0"
    assert loaded.policy.calendar_policy_version == "1.1.0"
    assert loaded.policy.calendar_policy_sha256 == (
        "375dd5e07b57208c5afb700d4fec96fdb3c95b29d562b553c3ecc5c4e6f97416"
    )


@dataclass(frozen=True)
class _DQContext:
    root: Path
    receipt_path: Path
    policy_path: Path


def _sha_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _sha_path(path: Path) -> str:
    return _sha_bytes(path.read_bytes())


def _signals() -> tuple[NormalizedDailySignalInput, ...]:
    directions = ("LONG_CALL", "FLAT", "LONG_PUT", "FLAT", "LONG_CALL")
    return tuple(
        NormalizedDailySignalInput(
            signal_session=date(2021, 2, day),
            source_data_cutoff_utc=datetime(2021, 2, day, 21, 0, tzinfo=UTC),
            generated_at_utc=datetime(2021, 2, day, 21, 5, tzinfo=UTC),
            signal=direction,
        )
        for day, direction in zip(range(22, 27), directions, strict=True)
    )


def _source(signals: tuple[NormalizedDailySignalInput, ...]) -> SignalSourceArtifact:
    content = canonical_normalized_signal_source_bytes(signals)
    return SignalSourceArtifact(
        artifact_id="qqq-normalized-signal-v1",
        locator="outputs/qqq/normalized_signal.json",
        sha256=_sha_bytes(content),
        byte_count=len(content),
        export_classification="EXPORT_ALLOWED_DERIVED",
    )


@pytest.fixture
def dq_context(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> _DQContext:
    return _prepare_dq_context(tmp_path / "pass", monkeypatch, status="PASS")


def _prepare_dq_context(
    root: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    status: _DQ_BYTES_STATUS,
) -> _DQContext:
    prices_path = root / "data/raw/prices_daily.csv"
    rates_path = root / "data/raw/rates_daily.csv"
    policy_path = root / "config/data_quality.yaml"
    prices_path.parent.mkdir(parents=True, exist_ok=True)
    policy_path.parent.mkdir(parents=True, exist_ok=True)

    sessions = tuple(date(2021, 2, day) for day in range(22, 27))
    price_rows = "".join(
        f"{session.isoformat()},QQQ,100,101,99,100,100,1000\n" for session in sessions
    )
    rate_rows = "".join(f"{session.isoformat()},DGS10,4.2\n" for session in sessions)
    prices_path.write_text(
        "date,ticker,open,high,low,close,adj_close,volume\n" + price_rows,
        encoding="utf-8",
    )
    rates_path.write_text(
        "date,series,value\n" + rate_rows,
        encoding="utf-8",
    )
    policy_path.write_text(_data_quality_policy_yaml(), encoding="utf-8")
    _copy_authority_files(root)
    _publish_cache(root, prices_path=prices_path, rates_path=rates_path)

    request = CanonicalDataQualityExecutionRequest(
        as_of=_END,
        requested_window=DataQualityDateWindow(_START, _END),
        evaluated_window=DataQualityDateWindow(_START, _END),
        prices_path=Path("data/raw/prices_daily.csv"),
        rates_path=Path("data/raw/rates_daily.csv"),
        manifest_path=Path("data/raw/download_manifest.csv"),
        expected_price_tickers=("QQQ",),
        expected_rate_series=("DGS10",),
        policy_path=Path("config/data_quality.yaml"),
    )

    def fake_validate_data_cache(**kwargs: Any) -> DataQualityReport:
        issues: tuple[DataQualityIssue, ...]
        if status == "FAIL":
            issues = (
                DataQualityIssue(
                    Severity.ERROR,
                    "fixture_blocker",
                    "controlled semantic failure",
                    source="fixture",
                ),
            )
        elif status == "PASS_WITH_WARNINGS":
            issues = (
                DataQualityIssue(
                    Severity.WARNING,
                    "fixture_warning",
                    "controlled semantic warning",
                    source="fixture",
                ),
            )
        else:
            issues = ()
        return DataQualityReport(
            checked_at=_CHECKED_AT,
            as_of=kwargs["as_of"],
            price_summary=DataFileSummary(
                path=prices_path,
                exists=True,
                rows=5,
                sha256=_sha_path(prices_path),
                min_date=_START,
                max_date=_END,
            ),
            rate_summary=DataFileSummary(
                path=rates_path,
                exists=True,
                rows=5,
                sha256=_sha_path(rates_path),
                min_date=_START,
                max_date=_END,
            ),
            expected_price_tickers=("QQQ",),
            expected_rate_series=("DGS10",),
            manifest_summary=DataFileSummary(
                path=root / "data/raw/download_manifest.csv",
                exists=True,
                rows=2,
                sha256=_sha_path(root / "data/raw/download_manifest.csv"),
            ),
            price_consistency_start_date=_START,
            rate_consistency_start_date=_START,
            issues=issues,
        )

    monkeypatch.setattr(quality_execution, "validate_data_cache", fake_validate_data_cache)
    clock_values = iter(
        (
            datetime(2021, 2, 26, 22, 59, 59, tzinfo=UTC),
            datetime(2021, 2, 26, 23, 0, 1, tzinfo=UTC),
        )
    )
    monkeypatch.setattr(
        quality_execution,
        "_utc_now",
        lambda: next(clock_values, datetime(2021, 2, 26, 23, 0, 1, tzinfo=UTC)),
    )
    result = run_canonical_data_quality_execution(request, project_root=root)
    return _DQContext(root=root, receipt_path=result.receipt_path, policy_path=policy_path)


def _copy_authority_files(root: Path) -> None:
    relatives = (
        Path("src/ai_trading_system/data/immutable_publish.py"),
        Path("src/ai_trading_system/data/quality_execution.py"),
        Path("src/ai_trading_system/data/quality.py"),
        Path("src/ai_trading_system/trading_calendar.py"),
        Path("src/ai_trading_system/us_equity_special_closure_policy.py"),
        Path("config/data/us_equity_special_closure_registry.yaml"),
        Path("config/data/archive/us_equity_special_closure_registry_1_0_0.yaml"),
        Path("config/research/qqq_options_shared_contract_v1.yaml"),
        Path("config/research/qqq_options_dq_pit_identity_v1.yaml"),
        Path("config/research/qqq_options_signal_export_v1.yaml"),
    )
    for relative in relatives:
        target = root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(PROJECT_ROOT / relative, target)
    shutil.copy2(
        PROJECT_ROOT
        / "config/data/archive/us_equity_special_closure_registry_1_0_0.yaml",
        root / "config/data/us_equity_special_closure_registry.yaml",
    )


def _publish_cache(root: Path, *, prices_path: Path, rates_path: Path) -> None:
    prices_raw = prices_path.read_bytes()
    rates_raw = rates_path.read_bytes()
    price_keys = _csv_row_keys(prices_raw, identity_column="ticker")
    rate_keys = _csv_row_keys(rates_raw, identity_column="series")
    publish_download_transaction(
        output_dir=root / "data/raw",
        requested_start=_START,
        requested_end=_END,
        published_at=datetime(2021, 2, 26, 22, 0, tzinfo=UTC),
        artifacts=(
            DownloadArtifactCandidate(
                role="prices",
                filename="prices_daily.csv",
                content=prices_raw,
                row_count=len(price_keys),
                source_event_ids=("prices:qqq_signal_fixture",),
            ),
            DownloadArtifactCandidate(
                role="rates",
                filename="rates_daily.csv",
                content=rates_raw,
                row_count=len(rate_keys),
                source_event_ids=("rates:qqq_signal_fixture",),
            ),
        ),
        source_bindings=(
            DownloadSourceBinding(
                source_event_id="prices:qqq_signal_fixture",
                artifact_role="prices",
                source_kind="LIVE_PROVIDER",
                source_id="qqq_signal_fixture_prices",
                provider="fixture",
                endpoint="prices",
                request_parameters={"start": _START.isoformat(), "end": _END.isoformat()},
                winning_row_count=len(price_keys),
                allocation_mode="REMAINDER",
                winning_row_keys=price_keys,
            ),
            DownloadSourceBinding(
                source_event_id="rates:qqq_signal_fixture",
                artifact_role="rates",
                source_kind="LIVE_PROVIDER",
                source_id="qqq_signal_fixture_rates",
                provider="fixture",
                endpoint="rates",
                request_parameters={"start": _START.isoformat(), "end": _END.isoformat()},
                winning_row_count=len(rate_keys),
                allocation_mode="REMAINDER",
                winning_row_keys=rate_keys,
            ),
        ),
    )


def _csv_row_keys(
    content: bytes,
    *,
    identity_column: str,
) -> tuple[tuple[str, str], ...]:
    rows = csv.DictReader(content.decode("utf-8").splitlines())
    return tuple(sorted((str(row[identity_column]), str(row["date"])) for row in rows))


def _data_quality_policy_yaml() -> str:
    return """\
governance:
  policy_id: DATA_QUALITY_CACHE_GATE
  policy_version: data_quality_cache_gate.v1
  status: REVIEWED
  owner: data_platform_owner
  role: data_quality
  reviewed_at: 2026-07-23
  rationale: Canonical QQQ signal fixture policy.
  review_condition: Review on semantic changes.
prices:
  max_stale_calendar_days: 7
  suspicious_daily_return_abs: 0.2
  extreme_daily_return_abs: 0.5
  suspicious_adjustment_ratio_change_abs: 0.25
  consistency_start_date: 2021-02-22
rates:
  max_stale_calendar_days: 7
  min_plausible_value: -1.0
  max_plausible_value: 25.0
  suspicious_daily_change_abs: 0.75
  extreme_daily_change_abs: 2.0
  consistency_start_date: 2021-02-22
"""


def _build_kwargs(context: _DQContext) -> dict[str, Any]:
    signals = _signals()
    source_bytes = canonical_normalized_signal_source_bytes(signals)
    return {
        "run_id": "qqq-options-run-202102",
        "signals": signals,
        "source_artifact": _source(signals),
        "source_artifact_bytes": source_bytes,
        "data_quality_receipt_path": context.receipt_path,
        "expected_data_quality_as_of": _END,
        "expected_data_quality_policy_path": context.policy_path,
        "expected_data_quality_input_roles": ("prices", "rates"),
        "requested_start": _START,
        "requested_end": _END,
        "evaluated_start": _START,
        "evaluated_end": _END,
        "initial_cash_usd": Decimal("100000"),
        "created_at_utc": _CREATED_AT,
        "producer_version": "qqq-options-signal-package.v1",
        "repository_code_sha": _REPOSITORY_SHA,
        "lineage_id": "qqq-options-signal-package-202102",
        "project_root": context.root,
    }


def _replace_signals(
    kwargs: dict[str, Any],
    signals: tuple[NormalizedDailySignalInput, ...],
) -> None:
    content = canonical_normalized_signal_source_bytes(signals)
    kwargs["signals"] = signals
    kwargs["source_artifact_bytes"] = content
    kwargs["source_artifact"] = _source(signals)


def test_policy_binds_predecessors_calendar_window_mapping_and_safety() -> None:
    loaded = load_qqq_options_signal_export_policy()

    assert loaded.policy_sha256 == _sha_path(loaded.policy_path)
    assert loaded.policy.shared_contract_sha256 == (
        "c89916ee7c3a4d9979780bf9359b0b39f61a383fe25aaf251e61ae629b43ff6b"
    )
    assert loaded.policy.shared_policy_sha256 == (
        "d7cc57a3c38bf0a1efc0553cb8b6e7ff302efc5afd2be3a44e2f04e82a056349"
    )
    assert loaded.policy.dq_pit_policy_sha256 == (
        "1e0128c40d9b125f5ce7d2a264f8308eb6885af70ebbdfe2184fed9af85b2358"
    )
    assert loaded.policy.primary_research_start == date(2021, 2, 22)
    assert loaded.policy.legacy_non_default_start == date(2022, 12, 1)
    assert loaded.policy.legacy_non_default_start_is_default is False
    assert loaded.policy.approved_non_primary_window_authorities == ()
    assert loaded.policy.etf_signal_mapping_allowed is False
    assert loaded.policy.safety.strategy_execution_allowed is False


def test_build_verifies_canonical_dq_and_seals_exact_replay(
    dq_context: _DQContext,
) -> None:
    package = build_qqq_options_signal_package(**_build_kwargs(dq_context))

    assert len(package.daily_signals) == 5
    assert package.run_manifest.requested_start == date(2021, 2, 22)
    assert package.run_manifest.evaluated_start == date(2021, 2, 22)
    assert package.run_manifest.initial_cash_usd == Decimal("100000")
    assert package.run_manifest.engine_identity_status == "UNKNOWN"
    assert package.run_manifest.evidence_admission_decision == ("CAPABILITY_OR_LICENSE_BLOCKED")
    assert package.run_manifest.signal_artifact_sha256 == _sha_bytes(
        package.signal_index.canonical_bytes
    )
    assert package.receipt.research_window_role == "PRIMARY"
    assert package.receipt.research_window_authority is None
    assert package.receipt.local_cached_data_gate.status == "PASS"
    assert package.receipt.local_dq_execution_receipt.locator.startswith(
        "outputs/data_quality/executions/"
    )
    assert package.receipt.option_event_dq_status == "NOT_EVALUATED"
    assert package.receipt.option_event_pit_status == "NOT_EVALUATED"

    for record in package.daily_signals:
        assert DailySignalRecord.from_json_bytes(record.canonical_bytes) == record
    assert RunManifestRecord.from_json_bytes(package.run_manifest.canonical_bytes) == (
        package.run_manifest
    )
    assert QQQOptionsSignalIndex.from_json_bytes(package.signal_index.canonical_bytes) == (
        package.signal_index
    )
    assert (
        QQQOptionsSignalPackageReceipt.from_json_bytes(package.receipt.canonical_bytes)
        == package.receipt
    )


def test_build_is_deterministic_for_input_order(dq_context: _DQContext) -> None:
    forward = build_qqq_options_signal_package(**_build_kwargs(dq_context))
    reversed_kwargs = _build_kwargs(dq_context)
    _replace_signals(reversed_kwargs, tuple(reversed(_signals())))
    reverse = build_qqq_options_signal_package(**reversed_kwargs)

    assert forward.files == reverse.files
    assert forward.receipt.content_sha256 == reverse.receipt.content_sha256


def test_writer_is_atomic_idempotent_and_rejects_drift(
    dq_context: _DQContext,
    tmp_path: Path,
) -> None:
    package = build_qqq_options_signal_package(**_build_kwargs(dq_context))
    output_root = tmp_path / "packages"
    target = write_qqq_options_signal_package(package, output_root=output_root)

    assert target == write_qqq_options_signal_package(package, output_root=output_root)
    (target / "run_manifest.json").write_bytes(b"tampered\n")
    with pytest.raises(QQQOptionsContractError, match="QQQ_OPTIONS_SIGNAL_PACKAGE_BYTES_MISMATCH"):
        write_qqq_options_signal_package(package, output_root=output_root)


def test_fake_pass_declaration_and_arbitrary_bytes_cannot_enter_builder(
    dq_context: _DQContext,
) -> None:
    fake = dq_context.root / "outputs/data_quality/executions/fake/receipt.json"
    fake.parent.mkdir(parents=True, exist_ok=True)
    fake.write_bytes(b'{"status":"PASS","scope":"CACHED_MARKET_MACRO"}\n')
    kwargs = _build_kwargs(dq_context)
    kwargs["data_quality_receipt_path"] = fake

    with pytest.raises(QQQOptionsContractError, match="QQQ_OPTIONS_SIGNAL_LOCAL_DQ_INVALID"):
        build_qqq_options_signal_package(**kwargs)


@pytest.mark.parametrize("status", ("FAIL", "PASS_WITH_WARNINGS"))
def test_semantic_nonpass_dq_receipt_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    status: _DQ_BYTES_STATUS,
) -> None:
    context = _prepare_dq_context(tmp_path / status.lower(), monkeypatch, status=status)

    with pytest.raises(QQQOptionsContractError, match="QQQ_OPTIONS_SIGNAL_LOCAL_DQ_INVALID"):
        build_qqq_options_signal_package(**_build_kwargs(context))


def test_unknown_dq_status_scope_asof_and_report_hash_fail_closed(
    dq_context: _DQContext,
) -> None:
    scope = _build_kwargs(dq_context)
    scope["expected_data_quality_input_roles"] = ("prices",)
    with pytest.raises(QQQOptionsContractError, match="QQQ_OPTIONS_SIGNAL_LOCAL_DQ_INVALID"):
        build_qqq_options_signal_package(**scope)

    as_of = _build_kwargs(dq_context)
    as_of["expected_data_quality_as_of"] = date(2021, 2, 25)
    with pytest.raises(QQQOptionsContractError, match="QQQ_OPTIONS_SIGNAL_LOCAL_DQ_INVALID"):
        build_qqq_options_signal_package(**as_of)

    receipt_payload = json.loads(dq_context.receipt_path.read_text(encoding="utf-8"))
    receipt_payload["report"]["status"] = "UNKNOWN"
    dq_context.receipt_path.write_text(
        json.dumps(receipt_payload, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    with pytest.raises(QQQOptionsContractError, match="QQQ_OPTIONS_SIGNAL_LOCAL_DQ_INVALID"):
        build_qqq_options_signal_package(**_build_kwargs(dq_context))


def test_report_semantic_bytes_and_package_window_mismatch_fail_closed(
    dq_context: _DQContext,
) -> None:
    window = _build_kwargs(dq_context)
    window["requested_end"] = date(2021, 2, 25)
    with pytest.raises(
        QQQOptionsContractError, match="QQQ_OPTIONS_SIGNAL_LOCAL_DQ_WINDOW_MISMATCH"
    ):
        build_qqq_options_signal_package(**window)

    receipt_payload = json.loads(dq_context.receipt_path.read_text(encoding="utf-8"))
    report_path = dq_context.root / receipt_payload["report"]["path"]
    report_path.write_bytes(report_path.read_bytes() + b"tampered\n")
    with pytest.raises(QQQOptionsContractError, match="QQQ_OPTIONS_SIGNAL_LOCAL_DQ_INVALID"):
        build_qqq_options_signal_package(**_build_kwargs(dq_context))


def test_primary_default_and_unreviewed_alternate_starts_fail_closed(
    dq_context: _DQContext,
) -> None:
    assert build_qqq_options_signal_package(
        **_build_kwargs(dq_context)
    ).run_manifest.requested_start == date(2021, 2, 22)

    pre_window = _build_kwargs(dq_context)
    pre_window["requested_start"] = date(2020, 1, 2)
    pre_window["evaluated_start"] = date(2020, 1, 2)
    with pytest.raises(
        QQQOptionsContractError, match="QQQ_OPTIONS_SIGNAL_PRIMARY_WINDOW_START_MISMATCH"
    ):
        build_qqq_options_signal_package(**pre_window)

    legacy = _build_kwargs(dq_context)
    legacy["requested_start"] = date(2022, 12, 1)
    legacy["evaluated_start"] = date(2022, 12, 1)
    with pytest.raises(
        QQQOptionsContractError, match="QQQ_OPTIONS_SIGNAL_PRIMARY_WINDOW_START_MISMATCH"
    ):
        build_qqq_options_signal_package(**legacy)

    sensitivity = _build_kwargs(dq_context)
    sensitivity["research_window_role"] = "SENSITIVITY"
    with pytest.raises(
        QQQOptionsContractError, match="QQQ_OPTIONS_SIGNAL_NON_PRIMARY_AUTHORITY_REQUIRED"
    ):
        build_qqq_options_signal_package(**sensitivity)


@pytest.mark.parametrize("direction", ("bullish", "bearish", "neutral"))
def test_unreviewed_etf_direction_mapping_fails_closed(direction: str) -> None:
    with pytest.raises(ValidationError):
        NormalizedDailySignalInput(
            signal_session=date(2021, 2, 22),
            source_data_cutoff_utc=datetime(2021, 2, 22, 21, 0, tzinfo=UTC),
            generated_at_utc=datetime(2021, 2, 22, 21, 5, tzinfo=UTC),
            signal=direction,  # type: ignore[arg-type]
        )


def test_duplicate_missing_and_extra_sessions_fail_closed(dq_context: _DQContext) -> None:
    duplicate = _build_kwargs(dq_context)
    _replace_signals(duplicate, (*_signals(), _signals()[0]))
    with pytest.raises(QQQOptionsContractError, match="QQQ_OPTIONS_SIGNAL_DUPLICATE_SESSION"):
        build_qqq_options_signal_package(**duplicate)

    missing = _build_kwargs(dq_context)
    _replace_signals(missing, _signals()[:-1])
    with pytest.raises(QQQOptionsContractError, match="QQQ_OPTIONS_SIGNAL_COVERAGE_MISMATCH"):
        build_qqq_options_signal_package(**missing)

    extra_signal = NormalizedDailySignalInput(
        signal_session=date(2021, 2, 27),
        source_data_cutoff_utc=datetime(2021, 2, 27, 21, 0, tzinfo=UTC),
        generated_at_utc=datetime(2021, 2, 27, 21, 5, tzinfo=UTC),
        signal="FLAT",
    )
    extra = _build_kwargs(dq_context)
    _replace_signals(extra, (*_signals(), extra_signal))
    with pytest.raises(QQQOptionsContractError, match="QQQ_OPTIONS_SIGNAL_COVERAGE_MISMATCH"):
        build_qqq_options_signal_package(**extra)


def test_cutoff_partial_session_and_special_closure_are_reviewed() -> None:
    before_close = _signals()[0].model_copy(
        update={"source_data_cutoff_utc": datetime(2021, 2, 22, 20, 59, tzinfo=UTC)}
    )
    with pytest.raises(QQQOptionsContractError, match="QQQ_OPTIONS_SIGNAL_SOURCE_CUTOFF_INVALID"):
        resolve_normalized_signal_effective_session(before_close)

    partial = NormalizedDailySignalInput(
        signal_session=date(2021, 11, 26),
        source_data_cutoff_utc=datetime(2021, 11, 26, 18, 0, tzinfo=UTC),
        generated_at_utc=datetime(2021, 11, 26, 18, 5, tzinfo=UTC),
        signal="FLAT",
    )
    assert resolve_normalized_signal_effective_session(partial) == date(2021, 11, 29)
    assert next_us_equity_trading_session(date(2025, 1, 8)) == date(2025, 1, 10)


def test_source_semantics_and_float_cash_fail_closed(dq_context: _DQContext) -> None:
    source_tamper = _build_kwargs(dq_context)
    source_tamper["source_artifact_bytes"] += b"tamper"
    with pytest.raises(
        QQQOptionsContractError, match="QQQ_OPTIONS_SIGNAL_SOURCE_SEMANTIC_MISMATCH"
    ):
        build_qqq_options_signal_package(**source_tamper)

    float_cash = _build_kwargs(dq_context)
    float_cash["initial_cash_usd"] = 100000.0
    with pytest.raises(ValidationError, match="Decimal or canonical decimal string"):
        build_qqq_options_signal_package(**float_cash)


@pytest.mark.parametrize(
    "mutation",
    (
        {"mapping_table": {"bullish": "LONG_CALL"}},
        {"etf_signal_mapping_allowed": True},
        {"signal_lag_sessions": 0},
        {"primary_research_start": "2022-12-01"},
        {"legacy_non_default_start_is_default": True},
        {"shared_contract_sha256": "b" * 64},
        {"dq_pit_policy_sha256": "c" * 64},
        {"safety": {"production_allowed": True}},
    ),
)
def test_policy_drift_mapping_window_and_unsafe_flags_fail_closed(
    tmp_path: Path,
    mutation: dict[str, Any],
) -> None:
    payload = yaml.safe_load(
        DEFAULT_QQQ_OPTIONS_SIGNAL_EXPORT_POLICY_PATH.read_text(encoding="utf-8")
    )
    if "safety" in mutation:
        payload["safety"].update(mutation["safety"])
    else:
        payload.update(mutation)
    candidate = tmp_path / "policy.yaml"
    candidate.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    with pytest.raises(QQQOptionsContractError, match="QQQ_OPTIONS_SIGNAL_EXPORT_POLICY_INVALID"):
        load_qqq_options_signal_export_policy(candidate)


def test_index_and_receipt_reject_noncanonical_and_semantic_tamper(
    dq_context: _DQContext,
) -> None:
    package = build_qqq_options_signal_package(**_build_kwargs(dq_context))
    noncanonical = package.signal_index.canonical_bytes.replace(b'": ', b'":', 1)
    tampered = package.receipt.canonical_bytes.replace(
        b'"option_event_dq_status": "NOT_EVALUATED"',
        b'"option_event_dq_status": "PASS"',
    )

    with pytest.raises(QQQOptionsContractError, match="QQQ_OPTIONS_SIGNAL_INDEX_NOT_CANONICAL"):
        QQQOptionsSignalIndex.from_json_bytes(noncanonical)
    with pytest.raises(QQQOptionsContractError, match="QQQ_OPTIONS_SIGNAL_RECEIPT_INVALID"):
        QQQOptionsSignalPackageReceipt.from_json_bytes(tampered)
