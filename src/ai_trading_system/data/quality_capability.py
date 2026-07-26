from __future__ import annotations

import io
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd

from ai_trading_system.config import DataQualityConfig, load_data_quality
from ai_trading_system.contracts.data_quality_capability import (
    CapabilityFileBinding,
    CapabilityIssueBinding,
    CapabilityQualityBinding,
    ConsumerDataCapabilityPolicy,
    ConsumerDataCapabilityReceipt,
    DataQualityCapabilityContractError,
)
from ai_trading_system.data.quality import (
    DataFileSnapshot,
    DataQualityReport,
    DataQualityRequestedWindowAuthority,
    Severity,
    capture_data_file_snapshots,
    render_data_quality_report,
    validate_data_cache,
)
from ai_trading_system.platform.artifacts import sha256_path, write_bytes_atomic
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_CAPABILITY_POLICY_PATH = Path(
    "config/data_quality/decision_target_label_core_capability_v1.yaml"
)


@dataclass(frozen=True)
class ConsumerDataCapabilityBuildResult:
    receipt: ConsumerDataCapabilityReceipt
    receipt_path: Path
    scoped_prices_path: Path
    scoped_rates_path: Path
    full_report_path: Path
    scoped_report_path: Path


def load_reviewed_consumer_data_capability_policy(
    path: Path = DEFAULT_CAPABILITY_POLICY_PATH,
    *,
    project_root: Path | None = None,
) -> ConsumerDataCapabilityPolicy:
    root = Path.cwd() if project_root is None else project_root
    resolved = path if path.is_absolute() else root / path
    try:
        payload = safe_load_yaml_path(resolved)
        if not isinstance(payload, Mapping):
            raise TypeError("policy root must be a mapping")
        return ConsumerDataCapabilityPolicy.model_validate(dict(payload))
    except (OSError, TypeError, ValueError) as exc:
        raise DataQualityCapabilityContractError(
            "DQ_CAPABILITY_POLICY_INVALID", f"{resolved}: {exc}"
        ) from exc


def build_consumer_data_capability(
    *,
    capability_policy: ConsumerDataCapabilityPolicy,
    capability_policy_path: Path,
    data_quality_policy_path: Path,
    prices_path: Path,
    rates_path: Path,
    output_root: Path,
    as_of: date,
    full_expected_price_tickers: Sequence[str],
    full_expected_rate_series: Sequence[str],
    manifest_path: Path | None = None,
    backtest_manifest_path: Path | None = None,
    secondary_prices_path: Path | None = None,
    require_secondary_prices: bool = False,
    quality_config: DataQualityConfig | None = None,
    generated_at: datetime | None = None,
) -> ConsumerDataCapabilityBuildResult:
    checked_at = generated_at or datetime.now(UTC)
    if checked_at.tzinfo is None or checked_at.utcoffset() is None:
        raise DataQualityCapabilityContractError(
            "DQ_CAPABILITY_GENERATED_AT_INVALID", "generated_at must be timezone-aware"
        )
    if checked_at.date() < as_of:
        raise DataQualityCapabilityContractError(
            "DQ_CAPABILITY_GENERATED_AT_INVALID", "generated_at cannot precede as_of"
        )
    if capability_policy.requested_start > as_of:
        raise DataQualityCapabilityContractError(
            "DQ_CAPABILITY_WINDOW_INVALID", "requested_start cannot follow as_of"
        )

    config = quality_config or load_data_quality(data_quality_policy_path)
    policy_sha = sha256_path(capability_policy_path)
    dq_policy_sha = sha256_path(data_quality_policy_path)
    input_paths = {"canonical_prices": prices_path, "canonical_rates": rates_path}
    if manifest_path is not None:
        input_paths["canonical_manifest"] = manifest_path
    if backtest_manifest_path is not None:
        input_paths["canonical_backtest_manifest"] = backtest_manifest_path
    if secondary_prices_path is not None:
        input_paths["canonical_secondary_prices"] = secondary_prices_path
    snapshots = capture_data_file_snapshots(input_paths)

    output_root.mkdir(parents=True, exist_ok=True)
    full_report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=sorted(set(str(item) for item in full_expected_price_tickers)),
        expected_rate_series=sorted(set(str(item) for item in full_expected_rate_series)),
        quality_config=config,
        as_of=as_of,
        manifest_path=manifest_path,
        backtest_manifest_path=backtest_manifest_path,
        secondary_prices_path=secondary_prices_path,
        require_secondary_prices=require_secondary_prices,
        file_snapshots=_quality_snapshot_roles(snapshots),
        requested_window=(
            (capability_policy.requested_start, as_of) if manifest_path is not None else None
        ),
        checked_at=checked_at,
    )
    full_report_path = output_root / "full_canonical_data_quality_report.md"
    write_bytes_atomic(
        full_report_path,
        render_data_quality_report(full_report).encode("utf-8"),
    )

    isolated_codes, unisolated_codes = _classify_global_errors(
        full_report,
        policy=capability_policy,
    )
    prices = _read_snapshot_csv(snapshots["canonical_prices"])
    rates = _read_snapshot_csv(snapshots["canonical_rates"])
    scoped_prices = _project_prices(
        prices,
        policy=capability_policy,
        as_of=as_of,
    )
    scoped_rates = _project_rates(
        rates,
        policy=capability_policy,
        as_of=as_of,
    )
    scoped_prices_path = output_root / "capability_market_panel.csv"
    scoped_rates_path = output_root / "capability_rates_snapshot.csv"
    write_bytes_atomic(scoped_prices_path, _dataframe_csv_bytes(scoped_prices))
    write_bytes_atomic(scoped_rates_path, _dataframe_csv_bytes(scoped_rates))

    authority: DataQualityRequestedWindowAuthority | None = None
    if not unisolated_codes:
        authority = _requested_window_authority(
            policy=capability_policy,
            policy_sha256=policy_sha,
            as_of=as_of,
            scoped_prices_path=scoped_prices_path,
            scoped_rates_path=scoped_rates_path,
            canonical_source_sha256s=tuple(
                sorted(
                    snapshot.sha256
                    for snapshot in snapshots.values()
                    if snapshot.sha256 is not None
                )
            ),
            full_report_path=full_report_path,
            unisolated_global_error_codes=(),
        )
    scoped_report = validate_data_cache(
        prices_path=scoped_prices_path,
        rates_path=scoped_rates_path,
        expected_price_tickers=list(capability_policy.required_price_tickers),
        expected_rate_series=list(capability_policy.required_rate_series),
        quality_config=config,
        as_of=as_of,
        requested_window=(capability_policy.requested_start, as_of),
        requested_window_authority=authority,
        checked_at=checked_at,
    )
    scoped_report_path = output_root / "scoped_data_quality_report.md"
    write_bytes_atomic(
        scoped_report_path,
        render_data_quality_report(scoped_report).encode("utf-8"),
    )

    canonical_inputs = tuple(
        _snapshot_binding(role, snapshot) for role, snapshot in sorted(snapshots.items())
    )
    materialized_inputs = (
        _path_binding("scoped_prices", scoped_prices_path, row_count=len(scoped_prices)),
        _path_binding("scoped_rates", scoped_rates_path, row_count=len(scoped_rates)),
    )
    full_quality = _quality_binding(full_report, full_report_path, isolated_codes=isolated_codes)
    scoped_quality = _quality_binding(scoped_report, scoped_report_path, isolated_codes=())
    capability_passed = scoped_report.status == "PASS" and not unisolated_codes
    receipt = ConsumerDataCapabilityReceipt(
        schema_version="data_quality_consumer_capability_receipt.v1",
        policy_id=capability_policy.policy_id,
        policy_version=capability_policy.policy_version,
        policy_path=capability_policy_path.as_posix(),
        policy_sha256=policy_sha,
        data_quality_policy_path=data_quality_policy_path.as_posix(),
        data_quality_policy_sha256=dq_policy_sha,
        capability_id=capability_policy.capability_id,
        capability_version=capability_policy.capability_version,
        consumer_id=capability_policy.consumer_id,
        consumer_version=capability_policy.consumer_version,
        requested_start=capability_policy.requested_start,
        as_of=as_of,
        generated_at=checked_at,
        required_price_tickers=capability_policy.required_price_tickers,
        required_rate_series=capability_policy.required_rate_series,
        required_price_fields=capability_policy.required_price_fields,
        full_expected_price_tickers=tuple(
            sorted(set(str(item) for item in full_expected_price_tickers))
        ),
        full_expected_rate_series=tuple(
            sorted(set(str(item) for item in full_expected_rate_series))
        ),
        full_require_secondary_prices=require_secondary_prices,
        canonical_inputs=canonical_inputs,
        materialized_inputs=materialized_inputs,
        full_quality=full_quality,
        scoped_quality=scoped_quality,
        requested_window_authority_id=(None if authority is None else authority.authority_id),
        capability_passed=capability_passed,
        global_cache_pass_claimed=full_report.status == "PASS",
        isolated_global_error_codes=isolated_codes,
        unisolated_global_error_codes=unisolated_codes,
        cross_consumer_reuse_allowed=False,
        daily_operation_authorized=False,
        production_effect="none",
        broker_action="none",
    )
    receipt_dir = output_root / "capability_receipts"
    receipt_path = receipt_dir / f"{receipt.receipt_id}.json"
    write_bytes_atomic(receipt_path, receipt.canonical_bytes)
    return ConsumerDataCapabilityBuildResult(
        receipt=receipt,
        receipt_path=receipt_path,
        scoped_prices_path=scoped_prices_path,
        scoped_rates_path=scoped_rates_path,
        full_report_path=full_report_path,
        scoped_report_path=scoped_report_path,
    )


def verify_consumer_data_capability_receipt(
    receipt_path: Path,
    *,
    capability_policy_path: Path,
    data_quality_policy_path: Path,
    quality_config: DataQualityConfig | None = None,
) -> ConsumerDataCapabilityReceipt:
    try:
        receipt = ConsumerDataCapabilityReceipt.from_json_bytes(receipt_path.read_bytes())
    except (OSError, ValueError) as exc:
        if isinstance(exc, DataQualityCapabilityContractError):
            raise
        raise DataQualityCapabilityContractError(
            "DQ_CAPABILITY_RECEIPT_READ_FAILED", f"{receipt_path}: {exc}"
        ) from exc
    policy = load_reviewed_consumer_data_capability_policy(capability_policy_path)
    _verify_policy_binding(
        receipt,
        policy=policy,
        policy_path=capability_policy_path,
        data_quality_policy_path=data_quality_policy_path,
    )
    bindings = {
        item.role: item for item in (*receipt.canonical_inputs, *receipt.materialized_inputs)
    }
    for binding in bindings.values():
        _verify_file_binding(binding)

    prices = _read_path_csv(Path(bindings["canonical_prices"].path))
    rates = _read_path_csv(Path(bindings["canonical_rates"].path))
    projected_prices = _project_prices(prices, policy=policy, as_of=receipt.as_of)
    projected_rates = _project_rates(rates, policy=policy, as_of=receipt.as_of)
    if _dataframe_csv_bytes(projected_prices) != Path(bindings["scoped_prices"].path).read_bytes():
        raise DataQualityCapabilityContractError(
            "DQ_CAPABILITY_PROJECTION_MISMATCH", "scoped prices are not source-derived"
        )
    if _dataframe_csv_bytes(projected_rates) != Path(bindings["scoped_rates"].path).read_bytes():
        raise DataQualityCapabilityContractError(
            "DQ_CAPABILITY_PROJECTION_MISMATCH", "scoped rates are not source-derived"
        )

    config = quality_config or load_data_quality(data_quality_policy_path)
    manifest = bindings.get("canonical_manifest")
    backtest_manifest = bindings.get("canonical_backtest_manifest")
    secondary = bindings.get("canonical_secondary_prices")
    full_report = validate_data_cache(
        prices_path=Path(bindings["canonical_prices"].path),
        rates_path=Path(bindings["canonical_rates"].path),
        expected_price_tickers=list(receipt.full_expected_price_tickers),
        expected_rate_series=list(receipt.full_expected_rate_series),
        quality_config=config,
        as_of=receipt.as_of,
        manifest_path=None if manifest is None else Path(manifest.path),
        backtest_manifest_path=None if backtest_manifest is None else Path(backtest_manifest.path),
        secondary_prices_path=None if secondary is None else Path(secondary.path),
        require_secondary_prices=receipt.full_require_secondary_prices,
        requested_window=(
            (receipt.requested_start, receipt.as_of) if manifest is not None else None
        ),
        checked_at=receipt.generated_at,
    )
    isolated_codes, unisolated_codes = _classify_global_errors(full_report, policy=policy)
    if (
        isolated_codes != receipt.isolated_global_error_codes
        or unisolated_codes != receipt.unisolated_global_error_codes
    ):
        raise DataQualityCapabilityContractError(
            "DQ_CAPABILITY_GLOBAL_ATTRIBUTION_MISMATCH",
            receipt.receipt_id,
        )
    expected_full = _quality_binding(
        full_report,
        Path(receipt.full_quality.report.path),
        isolated_codes=isolated_codes,
    )
    if expected_full != receipt.full_quality:
        raise DataQualityCapabilityContractError(
            "DQ_CAPABILITY_FULL_REPORT_MISMATCH", receipt.receipt_id
        )
    if (
        render_data_quality_report(full_report).encode("utf-8")
        != Path(receipt.full_quality.report.path).read_bytes()
    ):
        raise DataQualityCapabilityContractError(
            "DQ_CAPABILITY_FULL_REPORT_MISMATCH", "full report bytes differ"
        )

    authority: DataQualityRequestedWindowAuthority | None = None
    if not unisolated_codes:
        authority = _requested_window_authority(
            policy=policy,
            policy_sha256=receipt.policy_sha256,
            as_of=receipt.as_of,
            scoped_prices_path=Path(bindings["scoped_prices"].path),
            scoped_rates_path=Path(bindings["scoped_rates"].path),
            canonical_source_sha256s=tuple(
                sorted(item.sha256 for item in receipt.canonical_inputs)
            ),
            full_report_path=Path(receipt.full_quality.report.path),
            unisolated_global_error_codes=(),
        )
    scoped_report = validate_data_cache(
        prices_path=Path(bindings["scoped_prices"].path),
        rates_path=Path(bindings["scoped_rates"].path),
        expected_price_tickers=list(policy.required_price_tickers),
        expected_rate_series=list(policy.required_rate_series),
        quality_config=config,
        as_of=receipt.as_of,
        requested_window=(receipt.requested_start, receipt.as_of),
        requested_window_authority=authority,
        checked_at=receipt.generated_at,
    )
    expected_scoped = _quality_binding(
        scoped_report,
        Path(receipt.scoped_quality.report.path),
        isolated_codes=(),
    )
    if expected_scoped != receipt.scoped_quality:
        raise DataQualityCapabilityContractError(
            "DQ_CAPABILITY_SCOPED_REPORT_MISMATCH", receipt.receipt_id
        )
    if (
        render_data_quality_report(scoped_report).encode("utf-8")
        != Path(receipt.scoped_quality.report.path).read_bytes()
    ):
        raise DataQualityCapabilityContractError(
            "DQ_CAPABILITY_SCOPED_REPORT_MISMATCH", "scoped report bytes differ"
        )
    observed_authority_id = None if authority is None else authority.authority_id
    if observed_authority_id != receipt.requested_window_authority_id:
        raise DataQualityCapabilityContractError(
            "DQ_CAPABILITY_WINDOW_AUTHORITY_MISMATCH", receipt.receipt_id
        )
    return receipt


def _verify_policy_binding(
    receipt: ConsumerDataCapabilityReceipt,
    *,
    policy: ConsumerDataCapabilityPolicy,
    policy_path: Path,
    data_quality_policy_path: Path,
) -> None:
    expected = (
        receipt.policy_id == policy.policy_id
        and receipt.policy_version == policy.policy_version
        and receipt.policy_path == policy_path.as_posix()
        and receipt.policy_sha256 == sha256_path(policy_path)
        and receipt.data_quality_policy_path == data_quality_policy_path.as_posix()
        and receipt.data_quality_policy_sha256 == sha256_path(data_quality_policy_path)
        and receipt.capability_id == policy.capability_id
        and receipt.capability_version == policy.capability_version
        and receipt.consumer_id == policy.consumer_id
        and receipt.consumer_version == policy.consumer_version
        and receipt.requested_start == policy.requested_start
        and receipt.required_price_tickers == policy.required_price_tickers
        and receipt.required_rate_series == policy.required_rate_series
        and receipt.required_price_fields == policy.required_price_fields
    )
    if not expected:
        raise DataQualityCapabilityContractError(
            "DQ_CAPABILITY_POLICY_BINDING_MISMATCH", receipt.receipt_id
        )


def _requested_window_authority(
    *,
    policy: ConsumerDataCapabilityPolicy,
    policy_sha256: str,
    as_of: date,
    scoped_prices_path: Path,
    scoped_rates_path: Path,
    canonical_source_sha256s: tuple[str, ...],
    full_report_path: Path,
    unisolated_global_error_codes: tuple[str, ...],
) -> DataQualityRequestedWindowAuthority:
    return DataQualityRequestedWindowAuthority(
        policy_id=policy.policy_id,
        policy_version=policy.policy_version,
        policy_sha256=policy_sha256,
        capability_id=policy.capability_id,
        capability_version=policy.capability_version,
        consumer_id=policy.consumer_id,
        consumer_version=policy.consumer_version,
        requested_window_start=policy.requested_start,
        requested_window_end=as_of,
        as_of=as_of,
        prices_sha256=sha256_path(scoped_prices_path),
        rates_sha256=sha256_path(scoped_rates_path),
        expected_price_tickers=policy.required_price_tickers,
        expected_rate_series=policy.required_rate_series,
        canonical_source_sha256s=canonical_source_sha256s,
        full_report_sha256=sha256_path(full_report_path),
        unisolated_global_error_codes=unisolated_global_error_codes,
    )


def _classify_global_errors(
    report: DataQualityReport,
    *,
    policy: ConsumerDataCapabilityPolicy,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    required = set(policy.required_price_tickers)
    allowed = set(policy.allowed_global_error_codes)
    isolated: set[str] = set()
    unisolated: set[str] = set()
    for issue in report.issues:
        if issue.severity != Severity.ERROR:
            continue
        affected = set(issue.affected_instruments)
        if issue.code in allowed and affected and affected.isdisjoint(required):
            isolated.add(issue.code)
        else:
            unisolated.add(issue.code)
    return tuple(sorted(isolated)), tuple(sorted(unisolated))


def _quality_binding(
    report: DataQualityReport,
    report_path: Path,
    *,
    isolated_codes: tuple[str, ...],
) -> CapabilityQualityBinding:
    issues = tuple(
        CapabilityIssueBinding(
            severity=issue.severity.value,
            code=issue.code,
            rows=issue.rows,
            sample=issue.sample,
            source=issue.source,
            affected_instruments=issue.affected_instruments,
            isolated_from_capability=(
                issue.severity == Severity.ERROR and issue.code in isolated_codes
            ),
        )
        for issue in report.issues
    )
    return CapabilityQualityBinding(
        status=report.status,
        error_count=report.error_count,
        warning_count=report.warning_count,
        report=_path_binding(
            "data_quality_report",
            report_path,
            row_count=len(report_path.read_text(encoding="utf-8").splitlines()),
        ),
        issues=issues,
    )


def _project_prices(
    frame: pd.DataFrame,
    *,
    policy: ConsumerDataCapabilityPolicy,
    as_of: date,
) -> pd.DataFrame:
    required_columns = list(policy.required_price_fields)
    if any(column not in frame.columns for column in required_columns):
        return pd.DataFrame(columns=required_columns)
    dates = pd.to_datetime(frame["date"], errors="coerce")
    selected = frame.loc[
        frame["ticker"].astype(str).isin(policy.required_price_tickers)
        & dates.between(pd.Timestamp(policy.requested_start), pd.Timestamp(as_of)),
        required_columns,
    ].copy()
    selected["date"] = pd.to_datetime(selected["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    return selected.sort_values(["date", "ticker"], kind="stable").reset_index(drop=True)


def _project_rates(
    frame: pd.DataFrame,
    *,
    policy: ConsumerDataCapabilityPolicy,
    as_of: date,
) -> pd.DataFrame:
    columns = [
        column for column in ("date", "series", "value", "source") if column in frame.columns
    ]
    if any(column not in frame.columns for column in ("date", "series", "value")):
        return pd.DataFrame(columns=("date", "series", "value", "source"))
    dates = pd.to_datetime(frame["date"], errors="coerce")
    selected = frame.loc[
        frame["series"].astype(str).isin(policy.required_rate_series)
        & dates.between(pd.Timestamp(policy.requested_start), pd.Timestamp(as_of)),
        columns,
    ].copy()
    selected["date"] = pd.to_datetime(selected["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    return selected.sort_values(["date", "series"], kind="stable").reset_index(drop=True)


def _dataframe_csv_bytes(frame: pd.DataFrame) -> bytes:
    return frame.to_csv(index=False, lineterminator="\n").encode("utf-8")


def _read_snapshot_csv(snapshot: DataFileSnapshot) -> pd.DataFrame:
    if snapshot.content is None:
        return pd.DataFrame()
    try:
        return pd.read_csv(io.BytesIO(snapshot.content), low_memory=False)
    except (OSError, ValueError, pd.errors.ParserError):
        return pd.DataFrame()


def _read_path_csv(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(path, low_memory=False)
    except (OSError, ValueError, pd.errors.ParserError) as exc:
        raise DataQualityCapabilityContractError(
            "DQ_CAPABILITY_SOURCE_READ_FAILED", f"{path}: {exc}"
        ) from exc


def _quality_snapshot_roles(
    snapshots: Mapping[str, DataFileSnapshot],
) -> dict[str, DataFileSnapshot]:
    role_map = {
        "canonical_prices": "prices",
        "canonical_rates": "rates",
        "canonical_manifest": "manifest",
        "canonical_backtest_manifest": "backtest_manifest",
        "canonical_secondary_prices": "secondary_prices",
    }
    return {role_map[key]: value for key, value in snapshots.items()}


def _snapshot_binding(role: str, snapshot: DataFileSnapshot) -> CapabilityFileBinding:
    if snapshot.content is None or snapshot.sha256 is None:
        raise DataQualityCapabilityContractError(
            "DQ_CAPABILITY_CANONICAL_INPUT_MISSING", f"{role}: {snapshot.path}"
        )
    return CapabilityFileBinding(
        role=role,
        path=snapshot.path.as_posix(),
        sha256=snapshot.sha256,
        size_bytes=len(snapshot.content),
        row_count=_csv_row_count(snapshot.content),
    )


def _path_binding(role: str, path: Path, *, row_count: int) -> CapabilityFileBinding:
    return CapabilityFileBinding(
        role=role,
        path=path.as_posix(),
        sha256=sha256_path(path),
        size_bytes=path.stat().st_size,
        row_count=row_count,
    )


def _verify_file_binding(binding: CapabilityFileBinding) -> None:
    path = Path(binding.path)
    if (
        not path.is_file()
        or sha256_path(path) != binding.sha256
        or path.stat().st_size != binding.size_bytes
    ):
        raise DataQualityCapabilityContractError(
            "DQ_CAPABILITY_FILE_BINDING_MISMATCH", f"{binding.role}: {path}"
        )


def _csv_row_count(content: bytes) -> int:
    try:
        return len(pd.read_csv(io.BytesIO(content), low_memory=False))
    except (OSError, ValueError, pd.errors.ParserError):
        return 0


__all__ = [
    "ConsumerDataCapabilityBuildResult",
    "DEFAULT_CAPABILITY_POLICY_PATH",
    "build_consumer_data_capability",
    "load_reviewed_consumer_data_capability_policy",
    "verify_consumer_data_capability_receipt",
]
