from __future__ import annotations

import csv
import hashlib
import os
import subprocess
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.contracts.data_quality_execution import (
    DAILY_DEFAULT_DATA_QUALITY_EXECUTION_PROFILE_ID,
    MANUAL_DATA_QUALITY_EXECUTION_PROFILE_ID,
    DataQualityDateWindow,
    DataQualityValidatorBinding,
)
from ai_trading_system.data import immutable_publish, quality, quality_execution
from ai_trading_system.data.download_publication import (
    DownloadArtifactCandidate,
    DownloadPublicationIntegrityError,
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
    DataQualityExecutionError,
    load_reviewed_data_quality_policy,
    run_canonical_data_quality_execution,
    verify_data_quality_execution_receipt,
)

AS_OF = date(2026, 7, 23)
STARTED_AT = datetime(2026, 7, 23, 8, 59, 59, tzinfo=UTC)
CHECKED_AT = datetime(2026, 7, 23, 9, 0, tzinfo=UTC)
ENDED_AT = datetime(2026, 7, 23, 9, 0, 1, tzinfo=UTC)
PRICE_HEADER = "date,ticker,open,high,low,close,adj_close,volume\n"
PRICE_ROW = "2026-07-23,QQQ,100,101,99,100,100,1000\n"
PRICE_ROW_PREVIOUS = "2026-07-22,QQQ,100,101,99,100,100,1000\n"
RATE_HEADER = "date,series,value\n"
RATE_ROW = "2026-07-23,DGS10,4.2\n"
RATE_ROW_PREVIOUS = "2026-07-22,DGS10,4.2\n"
RATE_ROW_EARLIER = "2026-07-21,DGS10,4.2\n"


@dataclass(frozen=True)
class ExecutionFixture:
    root: Path
    prices_path: Path
    rates_path: Path
    manifest_path: Path
    policy_path: Path
    request: CanonicalDataQualityExecutionRequest


@pytest.fixture
def execution_fixture(tmp_path: Path) -> ExecutionFixture:
    root = tmp_path / "project"
    prices_path = root / "data/raw/prices_daily.csv"
    rates_path = root / "data/raw/rates_daily.csv"
    manifest_path = root / "data/raw/download_manifest.csv"
    policy_path = root / "config/data_quality.yaml"
    for path in (prices_path, rates_path, policy_path):
        path.parent.mkdir(parents=True, exist_ok=True)
    prices_path.write_text(PRICE_HEADER + PRICE_ROW, encoding="utf-8")
    rates_path.write_text(RATE_HEADER + RATE_ROW, encoding="utf-8")
    policy_path.write_text(_policy_yaml(), encoding="utf-8")
    _copy_validator_sources(root)
    _publish_execution_cache(
        root,
        prices_path=prices_path,
        rates_path=rates_path,
        requested_start=AS_OF,
        requested_end=AS_OF,
    )
    request = CanonicalDataQualityExecutionRequest(
        as_of=AS_OF,
        requested_window=DataQualityDateWindow(AS_OF, AS_OF),
        prices_path=Path("data/raw/prices_daily.csv"),
        rates_path=Path("data/raw/rates_daily.csv"),
        manifest_path=Path("data/raw/download_manifest.csv"),
        expected_price_tickers=("QQQ",),
        expected_rate_series=("DGS10",),
        policy_path=Path("config/data_quality.yaml"),
    )
    return ExecutionFixture(
        root=root,
        prices_path=prices_path,
        rates_path=rates_path,
        manifest_path=manifest_path,
        policy_path=policy_path,
        request=request,
    )


def test_runner_calls_canonical_validator_once_and_verifies_strict_pass(
    execution_fixture: ExecutionFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _install_report_spy(monkeypatch, status="PASS")

    result = run_canonical_data_quality_execution(
        execution_fixture.request, project_root=execution_fixture.root
    )
    preflight = verify_data_quality_execution_receipt(
        result.receipt_path,
        expected_as_of=AS_OF,
        expected_policy_path=execution_fixture.policy_path,
        expected_input_roles={"prices", "rates"},
        project_root=execution_fixture.root,
    )

    assert len(calls) == 1
    assert result.report.status == "PASS"
    assert result.receipt.report.status == "PASS"
    assert result.receipt.started_at == STARTED_AT
    assert result.receipt.checked_at == CHECKED_AT
    assert result.receipt.ended_at == ENDED_AT
    assert result.receipt.consumer_cutover_allowed is False
    assert result.receipt.production_effect == "none"
    assert result.receipt.evaluated_window == DataQualityDateWindow(AS_OF, AS_OF)
    invocation = {item.name: item.value_json for item in result.receipt.invocation}
    assert invocation["execution_profile_id"] == (f'"{MANUAL_DATA_QUALITY_EXECUTION_PROFILE_ID}"')
    assert len(result.report_path.read_bytes()) == result.receipt.report.size_bytes
    assert result.receipt_path.read_bytes() == result.receipt.canonical_bytes
    assert result.receipt_path.relative_to(execution_fixture.root).as_posix() == (
        f"outputs/data_quality/executions/{result.receipt.receipt_id}/receipt.json"
    )
    assert not list(execution_fixture.root.rglob("latest"))
    assert preflight.receipt_id == result.receipt.receipt_id
    assert preflight.status == "PASS"


@pytest.mark.parametrize("escaped_parent", ["reports", "executions"])
def test_runner_rejects_canonical_output_parent_symlink_escape(
    execution_fixture: ExecutionFixture,
    tmp_path: Path,
    escaped_parent: str,
) -> None:
    output_parent = execution_fixture.root / "outputs/data_quality" / escaped_parent
    outside = tmp_path / f"outside-{escaped_parent}"
    output_parent.parent.mkdir(parents=True, exist_ok=True)
    outside.mkdir()
    try:
        os.symlink(outside, output_parent, target_is_directory=True)
    except OSError as exc:
        pytest.skip(f"directory symlink creation unavailable: {exc}")

    with pytest.raises(DataQualityExecutionError, match="DQ_RECEIPT_FIELDS_INVALID"):
        run_canonical_data_quality_execution(
            execution_fixture.request,
            project_root=execution_fixture.root,
        )

    assert not any(path.is_file() for path in outside.rglob("*"))


@pytest.mark.skipif(os.name != "nt", reason="Windows junction/share semantics")
def test_bound_canonical_output_writer_blocks_parent_junction_swap(
    execution_fixture: ExecutionFixture,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _install_report_spy(monkeypatch, status="PASS")
    outside = tmp_path / "outside-junction"
    outside.mkdir()
    original = immutable_publish._write_bytes_atomic_bound
    observed = {"attempted": False, "blocked": False}

    def attempt_swap_then_write(binding, path, content, *args, **kwargs):
        if not observed["attempted"]:
            observed["attempted"] = True
            try:
                path.parent.rmdir()
            except OSError:
                observed["blocked"] = True
            else:
                completed = subprocess.run(
                    ["cmd", "/c", "mklink", "/J", str(path.parent), str(outside)],
                    check=False,
                    capture_output=True,
                    text=True,
                    shell=False,
                )
                if completed.returncode != 0:
                    raise AssertionError(f"junction race setup failed: {completed.stderr}")
        return original(binding, path, content, *args, **kwargs)

    monkeypatch.setattr(
        immutable_publish,
        "_write_bytes_atomic_bound",
        attempt_swap_then_write,
    )

    result = run_canonical_data_quality_execution(
        execution_fixture.request,
        project_root=execution_fixture.root,
    )

    assert observed == {"attempted": True, "blocked": True}
    assert result.receipt_path.is_file()
    assert not any(path.is_file() for path in outside.rglob("*"))


def test_runner_rejects_empty_common_window_at_contract_v1_boundary(
    execution_fixture: ExecutionFixture,
) -> None:
    closed_as_of = date(2026, 7, 22)
    request = replace(
        execution_fixture.request,
        as_of=closed_as_of,
        requested_window=DataQualityDateWindow(date(2021, 2, 22), closed_as_of),
        evaluated_window=DataQualityDateWindow(date(2021, 2, 22), closed_as_of),
    )

    with pytest.raises(DataQualityExecutionError, match="DQ_WINDOW_MISMATCH"):
        run_canonical_data_quality_execution(request, project_root=execution_fixture.root)

    # Contract v1 cannot honestly encode an empty date intersection. It must not
    # manufacture requested==evaluated merely to materialize a FAIL receipt.
    assert not (execution_fixture.root / "outputs/data_quality/executions").exists()


def test_direct_api_cannot_label_custom_paths_as_daily_default(
    execution_fixture: ExecutionFixture,
) -> None:
    request = replace(
        execution_fixture.request,
        execution_profile_id=DAILY_DEFAULT_DATA_QUALITY_EXECUTION_PROFILE_ID,
        prices_path=Path("data/custom/my_prices.csv"),
        rates_path=Path("data/custom/my_rates.csv"),
    )

    with pytest.raises(DataQualityExecutionError, match="DQ_RECEIPT_FIELDS_INVALID"):
        run_canonical_data_quality_execution(request, project_root=execution_fixture.root)

    assert not (execution_fixture.root / "outputs").exists()


def test_daily_default_profile_rejects_external_universe_symlink(
    execution_fixture: ExecutionFixture,
    tmp_path: Path,
) -> None:
    outside = tmp_path / "outside_universe.yaml"
    outside.write_bytes((PROJECT_ROOT / "config/universe.yaml").read_bytes())
    universe_path = execution_fixture.root / "config/universe.yaml"
    try:
        os.symlink(outside, universe_path)
    except OSError as exc:
        pytest.skip(f"file symlink creation unavailable: {exc}")
    request = replace(
        execution_fixture.request,
        execution_profile_id=DAILY_DEFAULT_DATA_QUALITY_EXECUTION_PROFILE_ID,
        secondary_prices_path=Path("data/raw/prices_marketstack_daily.csv"),
        require_secondary_prices=True,
    )

    with pytest.raises(DataQualityExecutionError, match="DQ_RECEIPT_FIELDS_INVALID"):
        run_canonical_data_quality_execution(request, project_root=execution_fixture.root)

    assert not (execution_fixture.root / "outputs").exists()


def test_daily_default_profile_blocks_universe_leaf_swap_after_metadata(
    execution_fixture: ExecutionFixture,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    universe_path = execution_fixture.root / "config/universe.yaml"
    universe_path.write_bytes((PROJECT_ROOT / "config/universe.yaml").read_bytes())
    outside = tmp_path / "outside_universe_swap.yaml"
    outside.write_bytes((PROJECT_ROOT / "config/universe.yaml").read_bytes())
    original = immutable_publish._bound_path_metadata
    swapped = False

    def swap_after_metadata(binding, path, field, *args, **kwargs):
        nonlocal swapped
        observed = original(binding, path, field, *args, **kwargs)
        if path == universe_path and field == "contained artifact" and not swapped:
            try:
                path.unlink()
                os.symlink(outside, path)
            except OSError as exc:
                pytest.skip(f"file symlink swap unavailable: {exc}")
            swapped = True
        return observed

    monkeypatch.setattr(
        immutable_publish,
        "_bound_path_metadata",
        swap_after_metadata,
    )
    request = replace(
        execution_fixture.request,
        execution_profile_id=DAILY_DEFAULT_DATA_QUALITY_EXECUTION_PROFILE_ID,
        secondary_prices_path=Path("data/raw/prices_marketstack_daily.csv"),
        require_secondary_prices=True,
    )

    with pytest.raises(DataQualityExecutionError, match="DQ_RECEIPT_FIELDS_INVALID"):
        run_canonical_data_quality_execution(request, project_root=execution_fixture.root)

    assert swapped is True
    assert not (execution_fixture.root / "outputs").exists()


def test_daily_default_profile_config_drift_during_validation_fails_before_output(
    execution_fixture: ExecutionFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    universe_path = execution_fixture.root / "config/universe.yaml"
    universe_path.write_text(
        """market:
  decision_frequency: daily
  benchmarks: [QQQ]
  defensive: []
  simple_baseline_research: []
macro:
  volatility: []
  rates: [DGS10]
  currency: []
ai_chain:
  core_watchlist: []
scoring_weights: {}
""",
        encoding="utf-8",
    )
    secondary_path = execution_fixture.root / "data/raw/prices_marketstack_daily.csv"
    secondary_path.write_text(PRICE_HEADER + PRICE_ROW, encoding="utf-8")
    calls = _install_report_spy(
        monkeypatch,
        status="PASS",
        on_validate=lambda: universe_path.write_text(
            universe_path.read_text(encoding="utf-8") + "# changed during validation\n",
            encoding="utf-8",
        ),
    )
    request = replace(
        execution_fixture.request,
        execution_profile_id=DAILY_DEFAULT_DATA_QUALITY_EXECUTION_PROFILE_ID,
        secondary_prices_path=Path("data/raw/prices_marketstack_daily.csv"),
        require_secondary_prices=True,
    )

    with pytest.raises(DataQualityExecutionError, match="DQ_INPUT_SHA_MISMATCH"):
        run_canonical_data_quality_execution(request, project_root=execution_fixture.root)

    assert len(calls) == 1
    assert not (execution_fixture.root / "outputs").exists()


def test_runner_discloses_actual_common_window_from_two_day_captured_inputs(
    execution_fixture: ExecutionFixture,
) -> None:
    execution_fixture.prices_path.write_text(
        PRICE_HEADER + PRICE_ROW_PREVIOUS + PRICE_ROW,
        encoding="utf-8",
    )
    execution_fixture.rates_path.write_text(
        RATE_HEADER + RATE_ROW_EARLIER + RATE_ROW_PREVIOUS,
        encoding="utf-8",
    )
    requested_start = date(2026, 7, 22)
    _publish_execution_cache(
        execution_fixture.root,
        prices_path=execution_fixture.prices_path,
        rates_path=execution_fixture.rates_path,
        requested_start=requested_start,
        requested_end=AS_OF,
        published_at=ENDED_AT,
    )
    request = replace(
        execution_fixture.request,
        requested_window=DataQualityDateWindow(requested_start, AS_OF),
    )

    result = run_canonical_data_quality_execution(
        request,
        project_root=execution_fixture.root,
    )

    actual = DataQualityDateWindow(date(2026, 7, 22), date(2026, 7, 22))
    assert result.receipt.evaluated_window == actual
    invocation = {item.name: item.value_json for item in result.receipt.invocation}
    assert invocation["evaluated_window"] == ('{"end":"2026-07-22","start":"2026-07-22"}')


def test_explicit_evaluated_window_mismatch_materializes_fail_receipt_with_actual_window(
    execution_fixture: ExecutionFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_report_spy(monkeypatch, status="PASS")
    request = replace(
        execution_fixture.request,
        requested_window=DataQualityDateWindow(date(2026, 7, 22), AS_OF),
        evaluated_window=DataQualityDateWindow(date(2026, 7, 22), AS_OF),
    )

    result = run_canonical_data_quality_execution(request, project_root=execution_fixture.root)

    assert result.receipt.evaluated_window == DataQualityDateWindow(AS_OF, AS_OF)
    assert result.receipt.report.status == "FAIL"
    assert "DQ_WINDOW_MISMATCH" in result.receipt.report.blocking_issue_codes


@pytest.mark.parametrize(
    ("status", "expected_code"),
    [
        ("FAIL", "DQ_EXECUTION_FAILED"),
        ("PASS_WITH_WARNINGS", "DQ_WARNING_NOT_ALLOWED"),
    ],
)
def test_runner_materializes_non_pass_receipt_but_verifier_is_fail_closed(
    execution_fixture: ExecutionFixture,
    monkeypatch: pytest.MonkeyPatch,
    status: str,
    expected_code: str,
) -> None:
    calls = _install_report_spy(monkeypatch, status=status)

    result = run_canonical_data_quality_execution(
        execution_fixture.request, project_root=execution_fixture.root
    )

    assert len(calls) == 1
    assert result.receipt_path.is_file()
    assert result.report_path.is_file()
    assert result.receipt.report.status == status
    with pytest.raises(DataQualityExecutionError, match=expected_code):
        verify_data_quality_execution_receipt(
            result.receipt_path,
            expected_as_of=AS_OF,
            expected_policy_path=execution_fixture.policy_path,
            expected_input_roles=("prices", "rates"),
            project_root=execution_fixture.root,
        )


@pytest.mark.parametrize(
    ("target", "expected_code"),
    [
        ("receipt", "DQ_RECEIPT_FIELDS_INVALID"),
        ("policy", "DQ_POLICY_SHA_MISMATCH"),
        ("validator", "DQ_VALIDATOR_SHA_MISMATCH"),
        ("input", "DQ_INPUT_SHA_MISMATCH"),
        ("manifest", "DQ_MANIFEST_SHA_MISMATCH"),
        ("report", "DQ_REPORT_SHA_MISMATCH"),
    ],
)
def test_verifier_rejects_bound_byte_tamper(
    execution_fixture: ExecutionFixture,
    monkeypatch: pytest.MonkeyPatch,
    target: str,
    expected_code: str,
) -> None:
    _install_report_spy(monkeypatch, status="PASS")
    result = run_canonical_data_quality_execution(
        execution_fixture.request, project_root=execution_fixture.root
    )
    target_path = {
        "receipt": result.receipt_path,
        "policy": execution_fixture.policy_path,
        "validator": execution_fixture.root / "src/ai_trading_system/data/quality.py",
        "input": execution_fixture.prices_path,
        "manifest": execution_fixture.manifest_path,
        "report": result.report_path,
    }[target]
    target_path.write_bytes(target_path.read_bytes() + b"\n")

    with pytest.raises(DataQualityExecutionError, match=expected_code):
        verify_data_quality_execution_receipt(
            result.receipt_path,
            expected_as_of=AS_OF,
            expected_policy_path=execution_fixture.policy_path,
            expected_input_roles=("prices", "rates"),
            project_root=execution_fixture.root,
        )


def test_verifier_rejects_expected_context_drift(
    execution_fixture: ExecutionFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_report_spy(monkeypatch, status="PASS")
    result = run_canonical_data_quality_execution(
        execution_fixture.request, project_root=execution_fixture.root
    )

    with pytest.raises(DataQualityExecutionError, match="DQ_AS_OF_MISMATCH"):
        verify_data_quality_execution_receipt(
            result.receipt_path,
            expected_as_of=date(2026, 7, 22),
            expected_policy_path=execution_fixture.policy_path,
            expected_input_roles=("prices", "rates"),
            project_root=execution_fixture.root,
        )
    with pytest.raises(DataQualityExecutionError, match="DQ_INPUT_SET_MISMATCH"):
        verify_data_quality_execution_receipt(
            result.receipt_path,
            expected_as_of=AS_OF,
            expected_policy_path=execution_fixture.policy_path,
            expected_input_roles=("prices",),
            project_root=execution_fixture.root,
        )
    with pytest.raises(DataQualityExecutionError, match="DQ_POLICY_PATH_MISMATCH"):
        verify_data_quality_execution_receipt(
            result.receipt_path,
            expected_as_of=AS_OF,
            expected_policy_path=Path("config/other_data_quality.yaml"),
            expected_input_roles=("prices", "rates"),
            project_root=execution_fixture.root,
        )


def test_verifier_rejects_validator_identity_tamper_even_when_receipt_is_canonical(
    execution_fixture: ExecutionFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_report_spy(monkeypatch, status="PASS")
    result = run_canonical_data_quality_execution(
        execution_fixture.request, project_root=execution_fixture.root
    )
    forged_validator = DataQualityValidatorBinding(
        validator_id=result.receipt.validator.validator_id,
        validator_version=result.receipt.validator.validator_version,
        entrypoint="ai_trading_system.data.quality:validate_data_cache",
        implementation_sources=result.receipt.validator.implementation_sources,
    )
    forged = replace(result.receipt, validator=forged_validator)
    forged_path = (
        execution_fixture.root / f"outputs/data_quality/executions/{forged.receipt_id}/receipt.json"
    )
    forged_path.parent.mkdir(parents=True, exist_ok=True)
    forged_path.write_bytes(forged.canonical_bytes)

    with pytest.raises(DataQualityExecutionError, match="DQ_VALIDATOR_ENTRYPOINT_MISMATCH"):
        verify_data_quality_execution_receipt(
            forged_path,
            expected_as_of=AS_OF,
            expected_policy_path=execution_fixture.policy_path,
            expected_input_roles=("prices", "rates"),
            project_root=execution_fixture.root,
        )


@pytest.mark.parametrize(
    ("field", "expected_code"),
    [
        ("source", "DQ_SOURCE_ID_UNREVIEWED"),
        ("record", "DQ_MANIFEST_CURRENT_CHECKSUM_MISSING"),
    ],
)
def test_verifier_recomputes_manifest_source_and_full_row_record_refs(
    execution_fixture: ExecutionFixture,
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    expected_code: str,
) -> None:
    _install_report_spy(monkeypatch, status="PASS")
    result = run_canonical_data_quality_execution(
        execution_fixture.request, project_root=execution_fixture.root
    )
    prices = next(item for item in result.receipt.inputs if item.role == "prices")
    if field == "source":
        forged_prices = replace(prices, matched_source_ids=("forged_source",))
    else:
        forged_prices = replace(prices, matched_record_refs=(f"manifest_record_{'f' * 16}",))
    forged = replace(
        result.receipt,
        inputs=tuple(
            forged_prices if item.role == "prices" else item for item in result.receipt.inputs
        ),
    )
    forged_path = (
        execution_fixture.root / f"outputs/data_quality/executions/{forged.receipt_id}/receipt.json"
    )
    forged_path.parent.mkdir(parents=True, exist_ok=True)
    forged_path.write_bytes(forged.canonical_bytes)

    with pytest.raises(DataQualityExecutionError, match=expected_code):
        verify_data_quality_execution_receipt(
            forged_path,
            expected_as_of=AS_OF,
            expected_policy_path=execution_fixture.policy_path,
            expected_input_roles=("prices", "rates"),
            project_root=execution_fixture.root,
        )


@pytest.mark.parametrize("field", ["schema_id", "source_role"])
def test_verifier_recomputes_input_schema_and_source_role(
    execution_fixture: ExecutionFixture,
    monkeypatch: pytest.MonkeyPatch,
    field: str,
) -> None:
    _install_report_spy(monkeypatch, status="PASS")
    result = run_canonical_data_quality_execution(
        execution_fixture.request, project_root=execution_fixture.root
    )
    prices = next(item for item in result.receipt.inputs if item.role == "prices")
    forged_prices = (
        replace(prices, schema_id="forged.v1")
        if field == "schema_id"
        else replace(prices, source_role="forged_source_role")
    )
    forged = replace(
        result.receipt,
        inputs=tuple(
            forged_prices if item.role == "prices" else item for item in result.receipt.inputs
        ),
    )
    forged_path = (
        execution_fixture.root / f"outputs/data_quality/executions/{forged.receipt_id}/receipt.json"
    )
    forged_path.parent.mkdir(parents=True, exist_ok=True)
    forged_path.write_bytes(forged.canonical_bytes)

    with pytest.raises(DataQualityExecutionError, match="DQ_INPUT_SET_MISMATCH"):
        verify_data_quality_execution_receipt(
            forged_path,
            expected_as_of=AS_OF,
            expected_policy_path=execution_fixture.policy_path,
            expected_input_roles=("prices", "rates"),
            project_root=execution_fixture.root,
        )


def test_current_checksum_missing_is_a_blocker_not_a_warning_fallback(
    execution_fixture: ExecutionFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_manifest(
        execution_fixture.manifest_path,
        prices_sha="a" * 64,
        rates_sha=_sha256(execution_fixture.rates_path),
    )
    _install_report_spy(monkeypatch, status="PASS")

    result = run_canonical_data_quality_execution(
        execution_fixture.request, project_root=execution_fixture.root
    )

    assert result.receipt.report.status == "FAIL"
    assert "DQ_MANIFEST_CURRENT_CHECKSUM_MISSING" in (result.receipt.report.blocking_issue_codes)
    with pytest.raises(DataQualityExecutionError, match="DQ_MANIFEST_CURRENT_CHECKSUM_MISSING"):
        verify_data_quality_execution_receipt(
            result.receipt_path,
            expected_as_of=AS_OF,
            expected_policy_path=execution_fixture.policy_path,
            expected_input_roles=("prices", "rates"),
            project_root=execution_fixture.root,
        )


def test_runner_binds_validator_and_manifest_to_single_captured_input_bytes(
    execution_fixture: ExecutionFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_sha = _sha256(execution_fixture.prices_path)
    real_validator = quality.validate_data_cache

    def replace_after_capture(**kwargs: Any) -> DataQualityReport:
        execution_fixture.prices_path.write_text(
            PRICE_HEADER + "2026-07-23,QQQ,200,201,199,200,200,2000\n",
            encoding="utf-8",
        )
        return real_validator(**kwargs)

    monkeypatch.setattr(quality_execution, "validate_data_cache", replace_after_capture)

    result = run_canonical_data_quality_execution(
        execution_fixture.request,
        project_root=execution_fixture.root,
    )

    prices_binding = next(item for item in result.receipt.inputs if item.role == "prices")
    assert result.receipt.report.status == "PASS"
    assert prices_binding.sha256 == original_sha
    assert prices_binding.sha256 != _sha256(execution_fixture.prices_path)
    with pytest.raises(DataQualityExecutionError, match="DQ_INPUT_SHA_MISMATCH"):
        verify_data_quality_execution_receipt(
            result.receipt_path,
            expected_as_of=AS_OF,
            expected_policy_path=execution_fixture.policy_path,
            expected_input_roles=("prices", "rates"),
            project_root=execution_fixture.root,
        )


def test_runner_resolves_canonical_publication_once_on_success(
    execution_fixture: ExecutionFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0
    real_resolver = quality.resolve_download_publication_if_present

    def counted_resolver(*, output_dir: Path):
        nonlocal calls
        calls += 1
        return real_resolver(output_dir=output_dir)

    monkeypatch.setattr(
        quality,
        "resolve_download_publication_if_present",
        counted_resolver,
    )

    result = run_canonical_data_quality_execution(
        execution_fixture.request,
        project_root=execution_fixture.root,
    )

    assert calls == 1
    assert result.receipt.report.status == "PASS"


def test_runner_preserves_first_invalid_publication_observation_without_retry(
    execution_fixture: ExecutionFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0
    real_resolver = quality.resolve_download_publication_if_present

    def invalid_then_valid(*, output_dir: Path):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise DownloadPublicationIntegrityError(
                "DOWNLOAD_DISCOVERY_INVALID",
                "controlled first observation failure",
                path=output_dir / ".download_publications/current/download_composite.json",
            )
        return real_resolver(output_dir=output_dir)

    monkeypatch.setattr(
        quality,
        "resolve_download_publication_if_present",
        invalid_then_valid,
    )

    result = run_canonical_data_quality_execution(
        execution_fixture.request,
        project_root=execution_fixture.root,
    )

    assert calls == 1
    assert result.receipt.report.status == "FAIL"
    assert "download_publication_invalid" in result.receipt.report.blocking_issue_codes


def test_runner_preserves_first_absent_publication_observation_without_retry(
    execution_fixture: ExecutionFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0
    real_resolver = quality.resolve_download_publication_if_present

    def absent_then_present(*, output_dir: Path):
        nonlocal calls
        calls += 1
        if calls == 1:
            return None
        return real_resolver(output_dir=output_dir)

    monkeypatch.setattr(
        quality,
        "resolve_download_publication_if_present",
        absent_then_present,
    )

    result = run_canonical_data_quality_execution(
        execution_fixture.request,
        project_root=execution_fixture.root,
    )

    assert calls == 1
    assert result.receipt.report.status == "FAIL"
    assert (
        "download_publication_required_for_requested_window"
        in result.receipt.report.blocking_issue_codes
    )


def test_direct_validator_reads_each_supplied_file_bytes_once(
    execution_fixture: ExecutionFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = load_reviewed_data_quality_policy(
        execution_fixture.policy_path,
        project_root=execution_fixture.root,
    ).config
    tracked = {
        path.resolve(): 0
        for path in (
            execution_fixture.prices_path,
            execution_fixture.rates_path,
            execution_fixture.manifest_path,
        )
    }
    original_read_bytes = Path.read_bytes

    def counted_read_bytes(path: Path) -> bytes:
        resolved = path.resolve()
        if resolved in tracked:
            tracked[resolved] += 1
        return original_read_bytes(path)

    monkeypatch.setattr(Path, "read_bytes", counted_read_bytes)

    report = quality.validate_data_cache(
        prices_path=execution_fixture.prices_path,
        rates_path=execution_fixture.rates_path,
        expected_price_tickers=["QQQ"],
        expected_rate_series=["DGS10"],
        quality_config=config,
        as_of=AS_OF,
        manifest_path=execution_fixture.manifest_path,
    )

    assert report.status == "PASS"
    assert set(tracked.values()) == {1}


@pytest.mark.parametrize(
    ("target", "expected_code"),
    [
        ("policy", "DQ_POLICY_SHA_MISMATCH"),
        ("validator", "DQ_VALIDATOR_SHA_MISMATCH"),
    ],
)
def test_runner_rejects_policy_or_validator_source_mutation_during_execution(
    execution_fixture: ExecutionFixture,
    monkeypatch: pytest.MonkeyPatch,
    target: str,
    expected_code: str,
) -> None:
    target_path = (
        execution_fixture.policy_path
        if target == "policy"
        else execution_fixture.root / "src/ai_trading_system/data/quality.py"
    )

    def mutate_target() -> None:
        target_path.write_bytes(target_path.read_bytes() + b"\n")

    calls = _install_report_spy(
        monkeypatch,
        status="PASS",
        on_validate=mutate_target,
    )

    with pytest.raises(DataQualityExecutionError, match=expected_code):
        run_canonical_data_quality_execution(
            execution_fixture.request,
            project_root=execution_fixture.root,
        )

    assert len(calls) == 1
    assert not (execution_fixture.root / "outputs/data_quality/executions").exists()


def test_runner_rejects_repo_external_bound_input_without_legacy_fallback(
    execution_fixture: ExecutionFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _install_report_spy(monkeypatch, status="PASS")
    external_path = execution_fixture.root.parent / "external_prices.csv"
    external_path.write_text(PRICE_HEADER + PRICE_ROW, encoding="utf-8")
    request = replace(execution_fixture.request, prices_path=external_path)

    with pytest.raises(DataQualityExecutionError, match="DQ_RECEIPT_FIELDS_INVALID"):
        run_canonical_data_quality_execution(request, project_root=execution_fixture.root)

    assert calls == []
    assert not (execution_fixture.root / "outputs").exists()


def test_runner_does_not_compare_as_of_to_the_utc_calendar_day(
    execution_fixture: ExecutionFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _install_report_spy(monkeypatch, status="PASS")
    utc_previous_day = datetime(2026, 7, 22, 23, 30, tzinfo=UTC)
    utc_next_day = datetime(2026, 7, 23, 10, 0, tzinfo=UTC)
    clock_values = iter((utc_previous_day, utc_next_day))
    monkeypatch.setattr(
        quality_execution,
        "_utc_now",
        lambda: next(clock_values, utc_next_day),
    )

    result = run_canonical_data_quality_execution(
        execution_fixture.request, project_root=execution_fixture.root
    )

    assert len(calls) == 1
    assert result.receipt.as_of == AS_OF
    assert result.receipt.started_at.date() == date(2026, 7, 22)
    assert result.receipt.checked_at.date() == AS_OF


def test_runner_rejects_future_report_checked_at_without_time_rewrite(
    execution_fixture: ExecutionFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _install_report_spy(monkeypatch, status="PASS")
    premature_end = CHECKED_AT - timedelta(microseconds=1)
    clock_values = iter((STARTED_AT, premature_end))
    monkeypatch.setattr(
        quality_execution,
        "_utc_now",
        lambda: next(clock_values, premature_end),
    )

    with pytest.raises(DataQualityExecutionError, match="DQ_EXECUTION_CHRONOLOGY_INVALID"):
        run_canonical_data_quality_execution(
            execution_fixture.request, project_root=execution_fixture.root
        )

    assert len(calls) == 1
    assert not (execution_fixture.root / "outputs/data_quality/executions").exists()


def test_runner_rejects_as_of_after_real_checked_calendar_date(
    execution_fixture: ExecutionFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    checked_at = datetime(2026, 7, 22, 23, 45, tzinfo=UTC)
    calls = _install_report_spy(monkeypatch, status="PASS", checked_at=checked_at)
    ended_at = datetime(2026, 7, 23, 0, 15, tzinfo=UTC)
    clock_values = iter((datetime(2026, 7, 22, 23, 30, tzinfo=UTC), ended_at))
    monkeypatch.setattr(
        quality_execution,
        "_utc_now",
        lambda: next(clock_values, ended_at),
    )

    with pytest.raises(DataQualityExecutionError, match="DQ_AS_OF_MISMATCH"):
        run_canonical_data_quality_execution(
            execution_fixture.request, project_root=execution_fixture.root
        )

    assert len(calls) == 1
    assert not (execution_fixture.root / "outputs/data_quality/executions").exists()


def _install_report_spy(
    monkeypatch: pytest.MonkeyPatch,
    *,
    status: str,
    checked_at: datetime = CHECKED_AT,
    on_validate: Callable[[], None] | None = None,
) -> list[dict[str, Any]]:
    calls: list[dict[str, Any]] = []

    def fake_validate_data_cache(**kwargs: Any) -> DataQualityReport:
        calls.append(kwargs)
        if on_validate is not None:
            on_validate()
        prices_path = kwargs["prices_path"]
        rates_path = kwargs["rates_path"]
        manifest_path = kwargs["manifest_path"]
        secondary_prices_path = kwargs.get("secondary_prices_path")
        assert isinstance(prices_path, Path)
        assert isinstance(rates_path, Path)
        assert isinstance(manifest_path, Path)
        issues: tuple[DataQualityIssue, ...]
        if status == "FAIL":
            issues = (
                DataQualityIssue(
                    Severity.ERROR,
                    "fixture_blocker",
                    "controlled failure",
                    source="fixture",
                ),
            )
        elif status == "PASS_WITH_WARNINGS":
            issues = (
                DataQualityIssue(
                    Severity.WARNING,
                    "fixture_warning",
                    "controlled warning",
                    source="fixture",
                ),
            )
        else:
            issues = ()
        return DataQualityReport(
            checked_at=checked_at,
            as_of=kwargs["as_of"],
            price_summary=DataFileSummary(
                path=prices_path,
                exists=True,
                rows=1,
                sha256=_sha256(prices_path),
                min_date=AS_OF,
                max_date=AS_OF,
            ),
            rate_summary=DataFileSummary(
                path=rates_path,
                exists=True,
                rows=1,
                sha256=_sha256(rates_path),
                min_date=AS_OF,
                max_date=AS_OF,
            ),
            expected_price_tickers=tuple(kwargs["expected_price_tickers"]),
            expected_rate_series=tuple(kwargs["expected_rate_series"]),
            secondary_price_summary=(
                DataFileSummary(
                    path=secondary_prices_path,
                    exists=True,
                    rows=1,
                    sha256=_sha256(secondary_prices_path),
                    min_date=AS_OF,
                    max_date=AS_OF,
                )
                if isinstance(secondary_prices_path, Path)
                else None
            ),
            manifest_summary=DataFileSummary(
                path=manifest_path,
                exists=True,
                rows=2,
                sha256=_sha256(manifest_path),
            ),
            price_consistency_start_date=date(2021, 2, 22),
            rate_consistency_start_date=date(2021, 2, 22),
            issues=issues,
        )

    monkeypatch.setattr(quality_execution, "validate_data_cache", fake_validate_data_cache)
    clock_values = iter((STARTED_AT, ENDED_AT))
    monkeypatch.setattr(
        quality_execution,
        "_utc_now",
        lambda: next(clock_values, ENDED_AT),
    )
    return calls


def _copy_validator_sources(root: Path) -> None:
    for relative in (
        Path("src/ai_trading_system/data/immutable_publish.py"),
        Path("src/ai_trading_system/data/quality_execution.py"),
        Path("src/ai_trading_system/data/quality.py"),
    ):
        target = root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes((PROJECT_ROOT / relative).read_bytes())


def _publish_execution_cache(
    root: Path,
    *,
    prices_path: Path,
    rates_path: Path,
    requested_start: date,
    requested_end: date,
    published_at: datetime = STARTED_AT,
) -> None:
    prices_raw = prices_path.read_bytes()
    rates_raw = rates_path.read_bytes()
    price_keys = _csv_row_keys(prices_raw, identity_column="ticker")
    rate_keys = _csv_row_keys(rates_raw, identity_column="series")
    publish_download_transaction(
        output_dir=root / "data/raw",
        requested_start=requested_start,
        requested_end=requested_end,
        published_at=published_at,
        artifacts=(
            DownloadArtifactCandidate(
                role="prices",
                filename="prices_daily.csv",
                content=prices_raw,
                row_count=len(price_keys),
                source_event_ids=("prices:execution_fixture",),
            ),
            DownloadArtifactCandidate(
                role="rates",
                filename="rates_daily.csv",
                content=rates_raw,
                row_count=len(rate_keys),
                source_event_ids=("rates:execution_fixture",),
            ),
        ),
        source_bindings=(
            DownloadSourceBinding(
                source_event_id="prices:execution_fixture",
                artifact_role="prices",
                source_kind="LIVE_PROVIDER",
                source_id="execution_fixture_prices",
                provider="execution_fixture",
                endpoint="prices",
                request_parameters={
                    "start": requested_start.isoformat(),
                    "end": requested_end.isoformat(),
                },
                winning_row_count=len(price_keys),
                allocation_mode="REMAINDER",
                winning_row_keys=price_keys,
            ),
            DownloadSourceBinding(
                source_event_id="rates:execution_fixture",
                artifact_role="rates",
                source_kind="LIVE_PROVIDER",
                source_id="execution_fixture_rates",
                provider="execution_fixture",
                endpoint="rates",
                request_parameters={
                    "start": requested_start.isoformat(),
                    "end": requested_end.isoformat(),
                },
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


def _write_manifest(
    path: Path,
    *,
    prices_sha: str,
    rates_sha: str,
    prices_rows: int = 1,
    rates_rows: int = 1,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "downloaded_at,source_id,provider,endpoint,request_parameters,output_path,row_count,checksum_sha256\n"
        "2026-07-23T08:00:00+00:00,fixture_prices,fixture,fixture,{},data/raw/prices_daily.csv,"
        f"{prices_rows},"
        f"{prices_sha}\n"
        "2026-07-23T08:00:00+00:00,fixture_rates,fixture,fixture,{},data/raw/rates_daily.csv,"
        f"{rates_rows},"
        f"{rates_sha}\n",
        encoding="utf-8",
    )


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _policy_yaml() -> str:
    return """\
governance:
  policy_id: DATA_QUALITY_CACHE_GATE
  policy_version: data_quality_cache_gate.v1
  status: REVIEWED
  owner: data_platform_owner
  role: data_quality
  reviewed_at: 2026-07-23
  rationale: Canonical tmp-fixture policy.
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
