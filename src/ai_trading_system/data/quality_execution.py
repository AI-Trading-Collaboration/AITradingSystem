from __future__ import annotations

import csv
import hashlib
import io
import json
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Final

from ai_trading_system.config import (
    PROJECT_ROOT,
    DataQualityConfig,
    UniverseConfig,
    configured_price_tickers,
    configured_rate_series,
)
from ai_trading_system.contracts.data_quality import DataQualityEvidence
from ai_trading_system.contracts.data_quality_execution import (
    DAILY_DEFAULT_DATA_QUALITY_EXECUTION_PROFILE_ID,
    MANUAL_DATA_QUALITY_EXECUTION_PROFILE_ID,
    DataQualityDateWindow,
    DataQualityExecutionContractError,
    DataQualityExecutionReceipt,
    DataQualityImplementationSourceBinding,
    DataQualityInputBinding,
    DataQualityInvocationParameter,
    DataQualityPolicyBinding,
    DataQualityReportBinding,
    DataQualityValidatorBinding,
    VerifiedDataQualityPreflight,
    _build_verified_data_quality_preflight,
)
from ai_trading_system.contracts.status import PolicyRole
from ai_trading_system.data.immutable_publish import (
    DataPublicationError,
    read_contained_artifact_bytes,
    write_contained_artifact_bytes,
)
from ai_trading_system.data.quality import (
    MANIFEST_REQUIRED_COLUMNS,
    PRICE_REQUIRED_COLUMNS,
    RATE_REQUIRED_COLUMNS,
    DataFileSnapshot,
    DataFileSummary,
    DataQualityIssue,
    DataQualityReport,
    DownloadPublicationResolution,
    Severity,
    capture_data_file_snapshots,
    render_data_quality_report,
    resolve_download_publication_observation,
    validate_data_cache,
)
from ai_trading_system.yaml_loader import safe_load_yaml_text

VALIDATOR_ID: Final = "aits.validate-data"
VALIDATOR_VERSION: Final = "quality_execution.run_canonical_data_quality_execution.v1"
VALIDATOR_ENTRYPOINT: Final = (
    "ai_trading_system.data.quality_execution:run_canonical_data_quality_execution"
)
DATA_QUALITY_CONTRACT_ID: Final = "cached_market_macro_validation"
DEFAULT_POLICY_PATH: Final = Path("config/data_quality.yaml")
_EXECUTION_SOURCE_PATH: Final = Path("src/ai_trading_system/data/quality_execution.py")
_QUALITY_SOURCE_PATH: Final = Path("src/ai_trading_system/data/quality.py")
_IMMUTABLE_PUBLISH_SOURCE_PATH: Final = Path("src/ai_trading_system/data/immutable_publish.py")
_DAILY_DEFAULT_UNIVERSE_PATH: Final = Path("config/universe.yaml")
_DAILY_DEFAULT_PRICES_PATH: Final = "data/raw/prices_daily.csv"
_DAILY_DEFAULT_RATES_PATH: Final = "data/raw/rates_daily.csv"
_DAILY_DEFAULT_SECONDARY_PRICES_PATH: Final = "data/raw/prices_marketstack_daily.csv"
_DAILY_DEFAULT_MANIFEST_PATH: Final = "data/raw/download_manifest.csv"
_SUPPORTED_INPUT_ROLES: Final = frozenset(
    {"prices", "rates", "secondary_prices", "backtest_manifest"}
)
_SUPPORTED_EXECUTION_PROFILE_IDS: Final = frozenset(
    {
        DAILY_DEFAULT_DATA_QUALITY_EXECUTION_PROFILE_ID,
        MANUAL_DATA_QUALITY_EXECUTION_PROFILE_ID,
    }
)
_CANONICAL_INVOCATION_NAMES: Final = frozenset(
    {
        "as_of",
        "backtest_manifest_path",
        "evaluated_window",
        "execution_profile_id",
        "execution_profile_config_path",
        "execution_profile_config_sha256",
        "expected_price_tickers",
        "expected_rate_series",
        "manifest_path",
        "policy_path",
        "prices_path",
        "rates_path",
        "requested_window",
        "require_secondary_prices",
        "secondary_prices_path",
    }
)


class DataQualityExecutionError(DataQualityExecutionContractError):
    """Stable data-domain error raised by the canonical runner and verifier."""


@dataclass(frozen=True)
class ReviewedDataQualityPolicy:
    config: DataQualityConfig
    binding: DataQualityPolicyBinding


@dataclass(frozen=True)
class CanonicalDataQualityExecutionRequest:
    as_of: date
    requested_window: DataQualityDateWindow
    prices_path: Path
    rates_path: Path
    manifest_path: Path
    expected_price_tickers: tuple[str, ...]
    expected_rate_series: tuple[str, ...]
    execution_profile_id: str = MANUAL_DATA_QUALITY_EXECUTION_PROFILE_ID
    evaluated_window: DataQualityDateWindow | None = None
    secondary_prices_path: Path | None = None
    require_secondary_prices: bool = False
    backtest_manifest_path: Path | None = None
    policy_path: Path = DEFAULT_POLICY_PATH

    def __post_init__(self) -> None:
        if not isinstance(self.as_of, date) or isinstance(self.as_of, datetime):
            raise DataQualityExecutionError("DQ_AS_OF_MISMATCH", "as_of must be a date")
        if not isinstance(self.requested_window, DataQualityDateWindow):
            raise DataQualityExecutionError(
                "DQ_WINDOW_INVALID", "requested_window must use DataQualityDateWindow"
            )
        evaluated = self.evaluated_window
        if evaluated is not None and not isinstance(evaluated, DataQualityDateWindow):
            raise DataQualityExecutionError(
                "DQ_WINDOW_INVALID", "evaluated_window must use DataQualityDateWindow"
            )
        if self.requested_window.end > self.as_of or (
            evaluated is not None and evaluated.end > self.as_of
        ):
            raise DataQualityExecutionError(
                "DQ_WINDOW_MISMATCH", "execution windows cannot extend beyond as_of"
            )
        if evaluated is not None and not self.requested_window.contains(evaluated):
            raise DataQualityExecutionError(
                "DQ_WINDOW_MISMATCH", "evaluated_window must be within requested_window"
            )
        object.__setattr__(
            self,
            "expected_price_tickers",
            _validated_text_tuple(self.expected_price_tickers, "expected_price_tickers"),
        )
        object.__setattr__(
            self,
            "expected_rate_series",
            _validated_text_tuple(self.expected_rate_series, "expected_rate_series"),
        )
        if self.require_secondary_prices and self.secondary_prices_path is None:
            raise DataQualityExecutionError(
                "DQ_INPUT_MISSING",
                "secondary_prices_path is required when require_secondary_prices=true",
            )
        if self.execution_profile_id not in _SUPPORTED_EXECUTION_PROFILE_IDS:
            raise DataQualityExecutionError(
                "DQ_RECEIPT_FIELDS_INVALID",
                f"unsupported execution_profile_id={self.execution_profile_id!r}",
            )


@dataclass(frozen=True)
class CanonicalDataQualityExecutionResult:
    receipt: DataQualityExecutionReceipt
    receipt_path: Path
    report: DataQualityReport
    report_path: Path


@dataclass(frozen=True)
class _ObservedFile:
    path: str
    absolute_path: Path
    exists: bool
    sha256: str | None
    size_bytes: int | None
    row_count: int | None
    columns: tuple[str, ...] = ()


@dataclass(frozen=True)
class _ManifestRows:
    path: str
    absolute_path: Path
    exists: bool
    sha256: str | None
    rows: tuple[Mapping[str, str], ...]
    fieldnames: tuple[str, ...]


@dataclass(frozen=True)
class _ManifestMatch:
    source_ids: tuple[str, ...]
    record_refs: tuple[str, ...]
    error_code: str | None = None
    error_message: str | None = None


@dataclass(frozen=True)
class _ExecutionProfileBinding:
    config_path: str | None
    config_sha256: str | None
    config_snapshot: DataFileSnapshot | None


@dataclass(frozen=True)
class _ParsedReport:
    status: str
    checked_at: datetime
    as_of: date
    error_count: int
    warning_count: int
    info_count: int
    issue_codes: tuple[str, ...]
    blocking_issue_codes: tuple[str, ...]
    expected_price_tickers: tuple[str, ...]
    expected_rate_series: tuple[str, ...]


def load_reviewed_data_quality_policy(
    policy_path: Path = DEFAULT_POLICY_PATH,
    *,
    project_root: Path = PROJECT_ROOT,
) -> ReviewedDataQualityPolicy:
    relative_path, absolute_path = _repo_path(project_root, policy_path)
    snapshot = capture_data_file_snapshots({"policy": absolute_path})["policy"]
    if not snapshot.exists:
        raise DataQualityExecutionError("DQ_POLICY_MISSING", relative_path)
    if snapshot.content is None:
        raise DataQualityExecutionError(
            "DQ_POLICY_NOT_REVIEWED",
            f"cannot read reviewed policy {relative_path}: {snapshot.read_error}",
        )
    try:
        payload = safe_load_yaml_text(snapshot.content.decode("utf-8"))
        config = DataQualityConfig.model_validate(payload)
    except Exception as exc:
        raise DataQualityExecutionError(
            "DQ_POLICY_NOT_REVIEWED", f"cannot load reviewed policy {relative_path}: {exc}"
        ) from exc
    governance = config.governance
    if governance is None or governance.status != "REVIEWED":
        raise DataQualityExecutionError("DQ_POLICY_NOT_REVIEWED", relative_path)
    try:
        binding = DataQualityPolicyBinding(
            policy_id=governance.policy_id,
            policy_version=governance.policy_version,
            status=governance.status,
            owner=governance.owner,
            role=PolicyRole(governance.role),
            path=relative_path,
            sha256=snapshot.sha256 or "",
        )
    except (ValueError, DataQualityExecutionContractError) as exc:
        if isinstance(exc, DataQualityExecutionContractError):
            raise DataQualityExecutionError(exc.code, exc.message) from exc
        raise DataQualityExecutionError("DQ_POLICY_ID_MISMATCH", str(exc)) from exc
    return ReviewedDataQualityPolicy(config=config, binding=binding)


def run_canonical_data_quality_execution(
    request: CanonicalDataQualityExecutionRequest,
    *,
    project_root: Path = PROJECT_ROOT,
) -> CanonicalDataQualityExecutionResult:
    """Run the only canonical validator call and materialize immutable report/receipt bytes."""

    if not isinstance(request, CanonicalDataQualityExecutionRequest):
        raise DataQualityExecutionError(
            "DQ_RECEIPT_FIELDS_INVALID", "request must be CanonicalDataQualityExecutionRequest"
        )
    root = project_root.resolve()
    policy = load_reviewed_data_quality_policy(request.policy_path, project_root=root)
    request_paths = _resolve_request_paths(request, root)
    execution_profile = _build_execution_profile_binding(request, request_paths, root)
    download_publication_resolution: DownloadPublicationResolution = (
        resolve_download_publication_observation(output_dir=request_paths["manifest"][1].parent)
    )
    file_snapshots = capture_data_file_snapshots(
        {role: path for role, (_, path) in request_paths.items()}
    )
    validator_sources = _capture_validator_sources(root)
    validator_binding = _validator_binding(validator_sources)
    started_at = _utc_now()

    # The canonical runner intentionally has exactly one validator call. All
    # bindings below are mechanically projected from that returned report and
    # its actual source bytes.
    report = validate_data_cache(
        prices_path=request_paths["prices"][1],
        rates_path=request_paths["rates"][1],
        expected_price_tickers=list(request.expected_price_tickers),
        expected_rate_series=list(request.expected_rate_series),
        quality_config=policy.config,
        as_of=request.as_of,
        manifest_path=request_paths["manifest"][1],
        backtest_manifest_path=(
            None
            if "backtest_manifest" not in request_paths
            else request_paths["backtest_manifest"][1]
        ),
        secondary_prices_path=(
            None
            if "secondary_prices" not in request_paths
            else request_paths["secondary_prices"][1]
        ),
        require_secondary_prices=request.require_secondary_prices,
        file_snapshots=file_snapshots,
        requested_window=(
            request.requested_window.start,
            request.requested_window.end,
        ),
        download_publication_resolution=download_publication_resolution,
    )

    manifest = _read_manifest_rows(
        request_paths["manifest"][0],
        request_paths["manifest"][1],
        file_snapshots["manifest"],
    )
    inputs, provenance_issues = _bindings_from_report(
        request=request,
        request_paths=request_paths,
        report=report,
        manifest=manifest,
        file_snapshots=file_snapshots,
    )
    if provenance_issues:
        report = replace(report, issues=(*report.issues, *provenance_issues))

    evaluated_window = _derive_evaluated_window(request, report)
    if request.evaluated_window is not None and request.evaluated_window != evaluated_window:
        report = replace(
            report,
            issues=(
                *report.issues,
                _provenance_issue(
                    "DQ_WINDOW_MISMATCH",
                    (
                        "explicit evaluated_window differs from captured common coverage: "
                        f"expected={request.evaluated_window.to_dict()} "
                        f"actual={evaluated_window.to_dict()}"
                    ),
                ),
            ),
        )

    # A profile config changed while the validator was running must fail before
    # any canonical report or receipt becomes discoverable.
    _assert_execution_profile_config_unchanged(execution_profile, root)
    report_bytes = render_data_quality_report(report).encode("utf-8")
    report_sha256 = hashlib.sha256(report_bytes).hexdigest()
    report_relative = f"outputs/data_quality/reports/{report_sha256}/data_quality_report.md"
    report_path = _write_immutable_bytes(
        root,
        report_relative,
        report_bytes,
        "DQ_REPORT_SHA_MISMATCH",
    )

    report_binding = DataQualityReportBinding(
        path=report_relative,
        sha256=report_sha256,
        size_bytes=len(report_bytes),
        status=report.status,
        error_count=report.error_count,
        warning_count=report.warning_count,
        info_count=report.info_count,
        issue_codes=tuple(sorted({issue.code for issue in report.issues})),
        blocking_issue_codes=tuple(
            sorted({issue.code for issue in report.issues if issue.severity == Severity.ERROR})
        ),
    )
    ended_at = _utc_now()
    if not started_at <= report.checked_at <= ended_at:
        raise DataQualityExecutionError(
            "DQ_EXECUTION_CHRONOLOGY_INVALID",
            "expected started_at <= report.checked_at <= ended_at",
        )
    if report.as_of != request.as_of or report.checked_at.date() < request.as_of:
        raise DataQualityExecutionError(
            "DQ_AS_OF_MISMATCH",
            (
                f"request_as_of={request.as_of.isoformat()} "
                f"report_as_of={report.as_of.isoformat()} "
                f"checked_at={report.checked_at.isoformat()}"
            ),
        )
    evidence = DataQualityEvidence(
        contract_id=DATA_QUALITY_CONTRACT_ID,
        policy_id=policy.binding.policy_id,
        policy_version=policy.binding.policy_version,
        status=report.status,
        passed=report.status in {"PASS", "PASS_WITH_WARNINGS"},
        checked_at=report.checked_at,
        as_of=report.as_of,
        report_path=report_binding.path,
        report_sha256=report_binding.sha256,
        error_count=report.error_count,
        warning_count=report.warning_count,
        checked_input_count=len(inputs),
        blocking_issues=report_binding.blocking_issue_codes,
    )
    _assert_file_unchanged(
        policy.binding.path,
        root / Path(policy.binding.path),
        expected_sha256=policy.binding.sha256,
        mismatch_code="DQ_POLICY_SHA_MISMATCH",
    )
    _assert_validator_sources_unchanged(validator_sources)
    _assert_execution_profile_config_unchanged(execution_profile, root)
    receipt = DataQualityExecutionReceipt(
        run_id=_run_id(started_at),
        contract_id=DATA_QUALITY_CONTRACT_ID,
        started_at=started_at,
        ended_at=ended_at,
        checked_at=report.checked_at,
        as_of=request.as_of,
        requested_window=request.requested_window,
        evaluated_window=evaluated_window,
        policy=policy.binding,
        validator=validator_binding,
        invocation=_invocation_bindings(
            request,
            request_paths,
            policy.binding.path,
            evaluated_window=evaluated_window,
            execution_profile=execution_profile,
        ),
        inputs=inputs,
        report=report_binding,
        data_quality_evidence=evidence,
        dq_execution_provenance_verified=True,
        consumer_cutover_allowed=False,
        production_effect="none",
    )
    receipt_relative = f"outputs/data_quality/executions/{receipt.receipt_id}/receipt.json"
    receipt_path = _write_immutable_bytes(
        root,
        receipt_relative,
        receipt.canonical_bytes,
        "DQ_RECEIPT_ID_MISMATCH",
    )
    return CanonicalDataQualityExecutionResult(
        receipt=receipt,
        receipt_path=receipt_path,
        report=report,
        report_path=report_path,
    )


def verify_data_quality_execution_receipt(
    receipt_path: Path,
    *,
    expected_as_of: date,
    expected_policy_path: Path,
    expected_input_roles: Collection[str],
    project_root: Path = PROJECT_ROOT,
) -> VerifiedDataQualityPreflight:
    """Re-read every bound byte and return an unforgeable strict-PASS capability."""

    root = project_root.resolve()
    receipt_relative, receipt_absolute = _repo_path(root, receipt_path)
    if receipt_absolute != root / Path(receipt_relative):
        raise DataQualityExecutionError(
            "DQ_RECEIPT_FIELDS_INVALID", "receipt path changed during containment resolution"
        )
    receipt_bytes = _read_secure_output_bytes(
        root,
        receipt_relative,
        missing_code="DQ_RECEIPT_MISSING",
    )
    try:
        receipt = DataQualityExecutionReceipt.from_json_bytes(receipt_bytes)
    except DataQualityExecutionContractError as exc:
        raise DataQualityExecutionError(exc.code, exc.message) from exc
    expected_receipt_path = f"outputs/data_quality/executions/{receipt.receipt_id}/receipt.json"
    if receipt_relative != expected_receipt_path:
        raise DataQualityExecutionError(
            "DQ_RECEIPT_ID_MISMATCH",
            f"expected={expected_receipt_path} actual={receipt_relative}",
        )
    if not isinstance(expected_as_of, date) or isinstance(expected_as_of, datetime):
        raise DataQualityExecutionError("DQ_AS_OF_MISMATCH", "expected_as_of must be a date")
    if receipt.as_of != expected_as_of:
        raise DataQualityExecutionError(
            "DQ_AS_OF_MISMATCH",
            f"expected={expected_as_of.isoformat()} actual={receipt.as_of.isoformat()}",
        )

    expected_policy_relative, _ = _repo_path(root, expected_policy_path)
    if receipt.policy.path != expected_policy_relative:
        raise DataQualityExecutionError(
            "DQ_POLICY_PATH_MISMATCH",
            f"expected={expected_policy_relative} actual={receipt.policy.path}",
        )
    observed_policy = load_reviewed_data_quality_policy(
        Path(expected_policy_relative), project_root=root
    )
    _verify_policy_binding(receipt.policy, observed_policy.binding)
    _verify_validator_binding(receipt.validator, root)

    roles = _validated_expected_roles(expected_input_roles)
    receipt_roles = tuple(item.role for item in receipt.inputs)
    if len(receipt_roles) != len(set(receipt_roles)) or set(receipt_roles) != set(roles):
        raise DataQualityExecutionError(
            "DQ_INPUT_SET_MISMATCH",
            f"expected={sorted(roles)} actual={sorted(receipt_roles)}",
        )
    observed_inputs = {item.role: _verify_input_binding(item, root) for item in receipt.inputs}
    _verify_invocation(receipt, observed_inputs, root)
    parsed_report = _verify_report_binding(receipt, root)
    _verify_report_invocation_projection(receipt, parsed_report)

    verified_at = _utc_now()
    try:
        preflight = _build_verified_data_quality_preflight(
            receipt=receipt,
            receipt_path=receipt_relative,
            receipt_sha256=hashlib.sha256(receipt_bytes).hexdigest(),
            receipt_size_bytes=len(receipt_bytes),
            verified_at=verified_at,
        )
        return preflight.assert_strict_passed()
    except DataQualityExecutionContractError as exc:
        raise DataQualityExecutionError(exc.code, exc.message) from exc


def _validated_text_tuple(values: Sequence[str], field: str) -> tuple[str, ...]:
    if isinstance(values, (str, bytes)) or not values:
        raise DataQualityExecutionError("DQ_RECEIPT_FIELDS_INVALID", f"{field} is required")
    normalized = tuple(values)
    if any(not isinstance(item, str) or not item or item != item.strip() for item in normalized):
        raise DataQualityExecutionError(
            "DQ_RECEIPT_FIELDS_INVALID", f"{field} contains invalid values"
        )
    if len(set(normalized)) != len(normalized):
        raise DataQualityExecutionError(
            "DQ_RECEIPT_FIELDS_INVALID", f"{field} contains duplicate values"
        )
    return normalized


def _validated_expected_roles(values: Collection[str]) -> tuple[str, ...]:
    if isinstance(values, (str, bytes)):
        raise DataQualityExecutionError(
            "DQ_INPUT_SET_MISMATCH", "expected_input_roles must be a collection"
        )
    normalized = tuple(values)
    if (
        not normalized
        or any(not isinstance(item, str) or not item or item != item.strip() for item in normalized)
        or len(set(normalized)) != len(normalized)
        or not set(normalized).issubset(_SUPPORTED_INPUT_ROLES)
    ):
        raise DataQualityExecutionError(
            "DQ_INPUT_SET_MISMATCH",
            "expected_input_roles must be unique supported non-empty strings",
        )
    return normalized


def _repo_path(project_root: Path, value: Path) -> tuple[str, Path]:
    root = project_root.resolve()
    candidate = value if value.is_absolute() else root / value
    absolute = candidate.resolve(strict=False)
    try:
        relative = absolute.relative_to(root)
    except ValueError as exc:
        raise DataQualityExecutionError(
            "DQ_RECEIPT_FIELDS_INVALID", f"path is outside project root: {value}"
        ) from exc
    relative_text = relative.as_posix()
    if not relative_text or relative_text == ".":
        raise DataQualityExecutionError("DQ_RECEIPT_FIELDS_INVALID", "file path is required")
    return relative_text, absolute


def _resolve_request_paths(
    request: CanonicalDataQualityExecutionRequest,
    root: Path,
) -> dict[str, tuple[str, Path]]:
    paths = {
        "prices": _repo_path(root, request.prices_path),
        "rates": _repo_path(root, request.rates_path),
        "manifest": _repo_path(root, request.manifest_path),
    }
    if request.secondary_prices_path is not None:
        paths["secondary_prices"] = _repo_path(root, request.secondary_prices_path)
    if request.backtest_manifest_path is not None:
        paths["backtest_manifest"] = _repo_path(root, request.backtest_manifest_path)
    return paths


def _build_execution_profile_binding(
    request: CanonicalDataQualityExecutionRequest,
    request_paths: Mapping[str, tuple[str, Path]],
    root: Path,
) -> _ExecutionProfileBinding:
    if request.execution_profile_id == MANUAL_DATA_QUALITY_EXECUTION_PROFILE_ID:
        return _ExecutionProfileBinding(None, None, None)
    _require_daily_default_profile_shape(
        prices_path=request_paths["prices"][0],
        rates_path=request_paths["rates"][0],
        manifest_path=request_paths["manifest"][0],
        secondary_prices_path=(
            request_paths["secondary_prices"][0] if "secondary_prices" in request_paths else None
        ),
        backtest_manifest_path=(
            request_paths["backtest_manifest"][0] if "backtest_manifest" in request_paths else None
        ),
        policy_path=_repo_path(root, request.policy_path)[0],
        require_secondary_prices=request.require_secondary_prices,
    )
    snapshot, universe = _capture_daily_default_universe(root)
    expected_tickers = tuple(configured_price_tickers(universe, include_full_ai_chain=False))
    expected_rates = tuple(configured_rate_series(universe))
    if request.expected_price_tickers != expected_tickers:
        raise DataQualityExecutionError(
            "DQ_RECEIPT_FIELDS_INVALID",
            "daily_default.v1 expected_price_tickers must equal the configured core universe",
        )
    if request.expected_rate_series != expected_rates:
        raise DataQualityExecutionError(
            "DQ_RECEIPT_FIELDS_INVALID",
            "daily_default.v1 expected_rate_series must equal the configured rate universe",
        )
    if snapshot.sha256 is None:
        raise DataQualityExecutionError(
            "DQ_RECEIPT_FIELDS_INVALID", "daily-default universe checksum is missing"
        )
    return _ExecutionProfileBinding(
        _DAILY_DEFAULT_UNIVERSE_PATH.as_posix(),
        snapshot.sha256,
        snapshot,
    )


def _capture_daily_default_universe(root: Path) -> tuple[DataFileSnapshot, UniverseConfig]:
    expected_path = _DAILY_DEFAULT_UNIVERSE_PATH.as_posix()
    content = _read_daily_default_profile_config_bytes(root, expected_path)
    snapshot = DataFileSnapshot(root / _DAILY_DEFAULT_UNIVERSE_PATH, True, content)
    try:
        decoded = content.decode("utf-8")
        payload = safe_load_yaml_text(decoded)
        if not isinstance(payload, Mapping):
            raise ValueError("universe config root must be a mapping")
        universe = UniverseConfig.model_validate(payload)
    except (UnicodeDecodeError, ValueError) as exc:
        raise DataQualityExecutionError(
            "DQ_RECEIPT_FIELDS_INVALID",
            f"invalid daily-default profile config: {expected_path}",
        ) from exc
    return snapshot, universe


def _read_daily_default_profile_config_bytes(root: Path, relative_path: str) -> bytes:
    try:
        return read_contained_artifact_bytes(root=root, relative_path=relative_path)
    except DataPublicationError as exc:
        code = (
            "DQ_INPUT_MISSING"
            if exc.code == "CONTAINED_ARTIFACT_MISSING"
            else "DQ_RECEIPT_FIELDS_INVALID"
        )
        raise DataQualityExecutionError(
            code,
            f"secure daily-default profile config read failed: {exc}",
        ) from exc


def _require_daily_default_profile_shape(
    *,
    prices_path: object,
    rates_path: object,
    manifest_path: object,
    secondary_prices_path: object,
    backtest_manifest_path: object,
    policy_path: object,
    require_secondary_prices: object,
) -> None:
    expected = {
        "prices_path": _DAILY_DEFAULT_PRICES_PATH,
        "rates_path": _DAILY_DEFAULT_RATES_PATH,
        "manifest_path": _DAILY_DEFAULT_MANIFEST_PATH,
        "secondary_prices_path": _DAILY_DEFAULT_SECONDARY_PRICES_PATH,
        "backtest_manifest_path": None,
        "policy_path": DEFAULT_POLICY_PATH.as_posix(),
        "require_secondary_prices": True,
    }
    observed = {
        "prices_path": prices_path,
        "rates_path": rates_path,
        "manifest_path": manifest_path,
        "secondary_prices_path": secondary_prices_path,
        "backtest_manifest_path": backtest_manifest_path,
        "policy_path": policy_path,
        "require_secondary_prices": require_secondary_prices,
    }
    mismatches = [name for name, value in expected.items() if observed[name] != value]
    if mismatches:
        raise DataQualityExecutionError(
            "DQ_RECEIPT_FIELDS_INVALID",
            f"daily_default.v1 profile shape mismatch: {','.join(sorted(mismatches))}",
        )


def _assert_execution_profile_config_unchanged(
    binding: _ExecutionProfileBinding,
    root: Path,
) -> None:
    if binding.config_snapshot is None:
        return
    if binding.config_path is None or binding.config_sha256 is None:
        raise DataQualityExecutionError(
            "DQ_RECEIPT_FIELDS_INVALID", "incomplete execution profile binding"
        )
    observed = _read_daily_default_profile_config_bytes(root, binding.config_path)
    if hashlib.sha256(observed).hexdigest() != binding.config_sha256:
        raise DataQualityExecutionError("DQ_INPUT_SHA_MISMATCH", binding.config_path)


def _run_id(started_at: datetime) -> str:
    return "dq-run-" + started_at.astimezone(UTC).strftime("%Y%m%dT%H%M%S.%fZ")


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _capture_validator_sources(root: Path) -> dict[str, DataFileSnapshot]:
    paths = {
        source_path.as_posix(): root / source_path
        for source_path in (
            _EXECUTION_SOURCE_PATH,
            _QUALITY_SOURCE_PATH,
            _IMMUTABLE_PUBLISH_SOURCE_PATH,
        )
    }
    snapshots = capture_data_file_snapshots(paths)
    for source_path, snapshot in snapshots.items():
        if not snapshot.exists or snapshot.content is None:
            raise DataQualityExecutionError(
                "DQ_VALIDATOR_IMPLEMENTATION_MISSING",
                f"{source_path}: {snapshot.read_error or 'missing'}",
            )
    return snapshots


def _validator_binding(
    snapshots: Mapping[str, DataFileSnapshot],
) -> DataQualityValidatorBinding:
    sources: list[DataQualityImplementationSourceBinding] = []
    for source_path in (
        _EXECUTION_SOURCE_PATH,
        _QUALITY_SOURCE_PATH,
        _IMMUTABLE_PUBLISH_SOURCE_PATH,
    ):
        relative = source_path.as_posix()
        snapshot = snapshots.get(relative)
        if snapshot is None or not snapshot.exists or snapshot.sha256 is None:
            raise DataQualityExecutionError("DQ_VALIDATOR_IMPLEMENTATION_MISSING", relative)
        sources.append(
            DataQualityImplementationSourceBinding(
                path=relative,
                sha256=snapshot.sha256,
            )
        )
    return DataQualityValidatorBinding(
        validator_id=VALIDATOR_ID,
        validator_version=VALIDATOR_VERSION,
        entrypoint=VALIDATOR_ENTRYPOINT,
        implementation_sources=tuple(sources),
    )


def _assert_file_unchanged(
    relative_path: str,
    absolute_path: Path,
    *,
    expected_sha256: str,
    mismatch_code: str,
) -> None:
    observed = capture_data_file_snapshots({"observed": absolute_path})["observed"]
    if not observed.exists or observed.sha256 != expected_sha256:
        raise DataQualityExecutionError(mismatch_code, relative_path)


def _assert_validator_sources_unchanged(
    snapshots: Mapping[str, DataFileSnapshot],
) -> None:
    for relative_path, snapshot in snapshots.items():
        expected_sha256 = snapshot.sha256
        if expected_sha256 is None:
            raise DataQualityExecutionError("DQ_VALIDATOR_IMPLEMENTATION_MISSING", relative_path)
        _assert_file_unchanged(
            relative_path,
            snapshot.path,
            expected_sha256=expected_sha256,
            mismatch_code="DQ_VALIDATOR_SHA_MISMATCH",
        )


def _observed_from_snapshot(
    relative_path: str,
    absolute_path: Path,
    snapshot: DataFileSnapshot,
) -> _ObservedFile:
    if not snapshot.exists:
        return _ObservedFile(relative_path, absolute_path, False, None, None, None)
    if snapshot.content is None:
        return _ObservedFile(relative_path, absolute_path, False, None, None, None)
    columns, row_count = _inspect_csv_content(snapshot.content)
    return _ObservedFile(
        path=relative_path,
        absolute_path=absolute_path,
        exists=True,
        sha256=snapshot.sha256,
        size_bytes=len(snapshot.content),
        row_count=row_count,
        columns=columns,
    )


def _observe_generic_file(
    relative_path: str,
    absolute_path: Path,
    snapshot: DataFileSnapshot | None = None,
) -> _ObservedFile:
    observed = snapshot or capture_data_file_snapshots({"input": absolute_path})["input"]
    if not observed.exists or observed.content is None:
        return _ObservedFile(relative_path, absolute_path, False, None, None, None)
    return _ObservedFile(
        path=relative_path,
        absolute_path=absolute_path,
        exists=True,
        sha256=observed.sha256,
        size_bytes=len(observed.content),
        row_count=1,
    )


def _bindings_from_report(
    *,
    request: CanonicalDataQualityExecutionRequest,
    request_paths: Mapping[str, tuple[str, Path]],
    report: DataQualityReport,
    manifest: _ManifestRows,
    file_snapshots: Mapping[str, DataFileSnapshot],
) -> tuple[tuple[DataQualityInputBinding, ...], tuple[DataQualityIssue, ...]]:
    observed: list[tuple[str, _ObservedFile, str, str, tuple[str, ...], bool]] = [
        (
            "prices",
            _observed_from_snapshot(*request_paths["prices"], file_snapshots["prices"]),
            "prices_daily.v1",
            "primary_market_prices",
            PRICE_REQUIRED_COLUMNS,
            True,
        ),
        (
            "rates",
            _observed_from_snapshot(*request_paths["rates"], file_snapshots["rates"]),
            "rates_daily.v1",
            "primary_macro_rates",
            RATE_REQUIRED_COLUMNS,
            True,
        ),
    ]
    if "secondary_prices" in request_paths:
        observed.append(
            (
                "secondary_prices",
                _observed_from_snapshot(
                    *request_paths["secondary_prices"],
                    file_snapshots["secondary_prices"],
                ),
                "prices_daily.v1",
                "secondary_market_prices",
                PRICE_REQUIRED_COLUMNS,
                True,
            )
        )
    if "backtest_manifest" in request_paths:
        observed.append(
            (
                "backtest_manifest",
                _observe_generic_file(
                    *request_paths["backtest_manifest"],
                    file_snapshots["backtest_manifest"],
                ),
                "backtest_input_manifest.json.v1",
                "validation_context",
                (),
                False,
            )
        )

    bindings: list[DataQualityInputBinding] = []
    issues: list[DataQualityIssue] = []
    if not manifest.exists:
        issues.append(_provenance_issue("DQ_MANIFEST_MISSING", manifest.path))
    summary_by_role = {
        "prices": report.price_summary,
        "rates": report.rate_summary,
        "secondary_prices": report.secondary_price_summary,
    }
    for role, item, schema_id, source_role, required_columns, needs_manifest in observed:
        if not item.exists:
            issues.append(_provenance_issue("DQ_INPUT_MISSING", item.path, role=role))
        elif required_columns and not set(required_columns).issubset(item.columns):
            issues.append(
                _provenance_issue(
                    "DQ_INPUT_SET_MISMATCH",
                    f"{item.path} missing schema columns",
                    role=role,
                )
            )
        summary = summary_by_role.get(role)
        if (
            summary is not None
            and item.exists
            and (summary.sha256 != item.sha256 or summary.rows != item.row_count)
        ):
            issues.append(
                _provenance_issue(
                    "DQ_INPUT_SHA_MISMATCH",
                    f"validator summary differs from captured bytes for {item.path}",
                    role=role,
                )
            )
        match = _match_manifest(item, manifest) if needs_manifest and item.exists else None
        if match is not None and match.error_code is not None:
            issues.append(
                _provenance_issue(
                    match.error_code,
                    match.error_message or item.path,
                    role=role,
                )
            )
        bindings.append(
            DataQualityInputBinding(
                role=role,
                path=item.path,
                exists=item.exists,
                schema_id=schema_id,
                source_role=source_role,
                sha256=item.sha256,
                size_bytes=item.size_bytes,
                row_count=item.row_count,
                manifest_path=manifest.path if needs_manifest and manifest.exists else None,
                manifest_sha256=(manifest.sha256 if needs_manifest and manifest.exists else None),
                matched_source_ids=() if match is None else match.source_ids,
                matched_record_refs=() if match is None else match.record_refs,
            )
        )
    return tuple(bindings), tuple(issues)


def _provenance_issue(code: str, message: str, *, role: str | None = None) -> DataQualityIssue:
    return DataQualityIssue(
        severity=Severity.ERROR,
        code=code,
        message=message,
        source="D0B canonical execution provenance",
        sample=role,
    )


def _derive_evaluated_window(
    request: CanonicalDataQualityExecutionRequest,
    report: DataQualityReport,
) -> DataQualityDateWindow:
    required_summaries: list[tuple[str, DataFileSummary | None]] = [
        ("prices", report.price_summary),
        ("rates", report.rate_summary),
    ]
    if request.require_secondary_prices:
        required_summaries.append(("secondary_prices", report.secondary_price_summary))

    start = request.requested_window.start
    end = request.requested_window.end
    for role, summary in required_summaries:
        if (
            summary is None
            or not summary.exists
            or summary.min_date is None
            or summary.max_date is None
        ):
            raise DataQualityExecutionError(
                "DQ_WINDOW_MISMATCH",
                f"required captured input has no date coverage: role={role}",
            )
        start = max(start, summary.min_date)
        end = min(end, summary.max_date)
    if start > end:
        # Contract v1 requires a non-empty DataQualityDateWindow. An empty common
        # intersection therefore cannot be encoded honestly in a FAIL receipt;
        # version the schema before changing this fail-closed boundary.
        raise DataQualityExecutionError(
            "DQ_WINDOW_MISMATCH",
            (
                "captured required inputs have no common coverage within requested_window: "
                f"requested={request.requested_window.to_dict()}"
            ),
        )
    return DataQualityDateWindow(start, end)


def _inspect_csv_content(content: bytes) -> tuple[tuple[str, ...], int]:
    try:
        handle = io.StringIO(content.decode("utf-8-sig"), newline="")
        reader = csv.reader(handle)
        header = next(reader, [])
        row_count = sum(1 for _ in reader)
    except (UnicodeError, csv.Error):
        return (), 0
    if len(header) != len(set(header)):
        return (), row_count
    return tuple(header), row_count


def _read_manifest_rows(
    relative_path: str,
    absolute_path: Path,
    snapshot: DataFileSnapshot | None = None,
) -> _ManifestRows:
    observed = snapshot or capture_data_file_snapshots({"manifest": absolute_path})["manifest"]
    if not observed.exists or observed.content is None:
        return _ManifestRows(relative_path, absolute_path, False, None, (), ())
    raw_sha = observed.sha256
    try:
        handle = io.StringIO(observed.content.decode("utf-8-sig"), newline="")
        reader = csv.DictReader(handle)
        fieldnames = tuple(reader.fieldnames or ())
        if (
            not fieldnames
            or len(fieldnames) != len(set(fieldnames))
            or not set(MANIFEST_REQUIRED_COLUMNS).issubset(fieldnames)
        ):
            return _ManifestRows(relative_path, absolute_path, True, raw_sha, (), fieldnames)
        rows: list[Mapping[str, str]] = []
        for raw_row in reader:
            if None in raw_row or any(value is None for value in raw_row.values()):
                return _ManifestRows(relative_path, absolute_path, True, raw_sha, (), fieldnames)
            rows.append({field: raw_row[field] for field in fieldnames})
    except (UnicodeError, csv.Error):
        return _ManifestRows(relative_path, absolute_path, True, raw_sha, (), ())
    return _ManifestRows(relative_path, absolute_path, True, raw_sha, tuple(rows), fieldnames)


def _match_manifest(item: _ObservedFile, manifest: _ManifestRows) -> _ManifestMatch:
    if not manifest.exists or manifest.sha256 is None:
        return _ManifestMatch((), (), "DQ_MANIFEST_MISSING", manifest.path)
    if not manifest.rows or item.sha256 is None or item.row_count is None:
        return _ManifestMatch(
            (),
            (),
            "DQ_MANIFEST_CURRENT_CHECKSUM_MISSING",
            f"no readable manifest row for {item.path}",
        )
    candidates = [
        row
        for row in manifest.rows
        if row.get("checksum_sha256") == item.sha256
        and _manifest_output_matches(item.absolute_path, row.get("output_path", ""), manifest)
    ]
    if not candidates:
        return _ManifestMatch(
            (),
            (),
            "DQ_MANIFEST_CURRENT_CHECKSUM_MISSING",
            f"current checksum has no path-matched manifest row for {item.path}",
        )
    valid_rows: list[Mapping[str, str]] = []
    for row in candidates:
        try:
            manifest_count = int(row.get("row_count", ""))
        except ValueError:
            manifest_count = -1
        if manifest_count != item.row_count:
            continue
        valid_rows.append(row)
    if not valid_rows:
        return _ManifestMatch(
            (),
            (),
            "DQ_INPUT_ROW_COUNT_MISMATCH",
            f"manifest row_count differs for {item.path}",
        )
    source_ids = tuple(sorted({row.get("source_id", "") for row in valid_rows}))
    if any(not source_id or source_id != source_id.strip() for source_id in source_ids):
        return _ManifestMatch(
            (), (), "DQ_SOURCE_ID_UNREVIEWED", f"invalid source_id for {item.path}"
        )
    record_refs = tuple(sorted({_manifest_record_ref(row) for row in valid_rows}))
    return _ManifestMatch(source_ids, record_refs)


def _manifest_output_matches(
    input_path: Path,
    output_path: str,
    manifest: _ManifestRows,
) -> bool:
    if not output_path or output_path != output_path.strip():
        return False
    candidate = Path(output_path)
    if not candidate.is_absolute():
        project_root = manifest.absolute_path
        for _ in Path(manifest.path).parts:
            project_root = project_root.parent
        candidate = project_root / candidate
    return candidate.resolve(strict=False) == input_path.resolve(strict=False)


def _manifest_record_ref(row: Mapping[str, str]) -> str:
    canonical = json.dumps(
        dict(row), ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return f"manifest_record_{hashlib.sha256(canonical).hexdigest()}"


def _invocation_bindings(
    request: CanonicalDataQualityExecutionRequest,
    request_paths: Mapping[str, tuple[str, Path]],
    policy_path: str,
    *,
    evaluated_window: DataQualityDateWindow,
    execution_profile: _ExecutionProfileBinding,
) -> tuple[DataQualityInvocationParameter, ...]:
    values: dict[str, object] = {
        "as_of": request.as_of.isoformat(),
        "backtest_manifest_path": (
            request_paths["backtest_manifest"][0] if "backtest_manifest" in request_paths else None
        ),
        "evaluated_window": evaluated_window.to_dict(),
        "execution_profile_id": request.execution_profile_id,
        "execution_profile_config_path": execution_profile.config_path,
        "execution_profile_config_sha256": execution_profile.config_sha256,
        "expected_price_tickers": list(request.expected_price_tickers),
        "expected_rate_series": list(request.expected_rate_series),
        "manifest_path": request_paths["manifest"][0],
        "policy_path": policy_path,
        "prices_path": request_paths["prices"][0],
        "rates_path": request_paths["rates"][0],
        "requested_window": request.requested_window.to_dict(),
        "require_secondary_prices": request.require_secondary_prices,
        "secondary_prices_path": (
            request_paths["secondary_prices"][0] if "secondary_prices" in request_paths else None
        ),
    }
    return tuple(
        DataQualityInvocationParameter.from_value(name, value) for name, value in values.items()
    )


def _write_immutable_bytes(
    root: Path,
    relative_path: str,
    content: bytes,
    mismatch_code: str,
) -> Path:
    try:
        result = write_contained_artifact_bytes(
            root=root,
            relative_path=relative_path,
            content=content,
            immutable=True,
        )
    except DataPublicationError as exc:
        raise DataQualityExecutionError(
            (
                mismatch_code
                if exc.code == "IMMUTABLE_ARTIFACT_COLLISION"
                else "DQ_RECEIPT_FIELDS_INVALID"
            ),
            f"secure canonical output write failed: {exc}",
        ) from exc
    if result.sha256 != hashlib.sha256(content).hexdigest() or result.size_bytes != len(content):
        raise DataQualityExecutionError(mismatch_code, "secure output attestation mismatch")
    return result.path


def _read_secure_output_bytes(
    root: Path,
    relative_path: str,
    *,
    missing_code: str,
) -> bytes:
    try:
        return read_contained_artifact_bytes(root=root, relative_path=relative_path)
    except DataPublicationError as exc:
        code = (
            missing_code
            if exc.code in {"CONTAINED_ARTIFACT_MISSING", "ARTIFACT_BOUND_DIRECTORY_FAILED"}
            else "DQ_RECEIPT_FIELDS_INVALID"
        )
        raise DataQualityExecutionError(
            code,
            f"secure canonical output read failed: {exc}",
        ) from exc


def _verify_policy_binding(
    claimed: DataQualityPolicyBinding,
    observed: DataQualityPolicyBinding,
) -> None:
    if claimed.policy_id != observed.policy_id or claimed.role is not observed.role:
        raise DataQualityExecutionError("DQ_POLICY_ID_MISMATCH", claimed.policy_id)
    if claimed.policy_version != observed.policy_version:
        raise DataQualityExecutionError("DQ_POLICY_VERSION_MISMATCH", claimed.policy_version)
    if claimed.status != "REVIEWED" or claimed.owner != observed.owner:
        raise DataQualityExecutionError("DQ_POLICY_NOT_REVIEWED", claimed.status)
    if claimed.path != observed.path:
        raise DataQualityExecutionError("DQ_POLICY_PATH_MISMATCH", claimed.path)
    if claimed.sha256 != observed.sha256:
        raise DataQualityExecutionError("DQ_POLICY_SHA_MISMATCH", claimed.path)


def _verify_validator_binding(claimed: DataQualityValidatorBinding, root: Path) -> None:
    if claimed.validator_id != VALIDATOR_ID or claimed.validator_version != VALIDATOR_VERSION:
        raise DataQualityExecutionError(
            "DQ_VALIDATOR_ID_MISMATCH",
            f"{claimed.validator_id}@{claimed.validator_version}",
        )
    if claimed.entrypoint != VALIDATOR_ENTRYPOINT:
        raise DataQualityExecutionError("DQ_VALIDATOR_ENTRYPOINT_MISMATCH", claimed.entrypoint)
    expected_paths = {
        _EXECUTION_SOURCE_PATH.as_posix(),
        _QUALITY_SOURCE_PATH.as_posix(),
        _IMMUTABLE_PUBLISH_SOURCE_PATH.as_posix(),
    }
    claimed_paths = {item.path for item in claimed.implementation_sources}
    if claimed_paths != expected_paths:
        raise DataQualityExecutionError(
            "DQ_VALIDATOR_IMPLEMENTATION_MISSING",
            f"expected={sorted(expected_paths)} actual={sorted(claimed_paths)}",
        )
    for source in claimed.implementation_sources:
        absolute = root / Path(source.path)
        snapshot = capture_data_file_snapshots({"source": absolute})["source"]
        if not snapshot.exists or snapshot.content is None:
            raise DataQualityExecutionError("DQ_VALIDATOR_IMPLEMENTATION_MISSING", source.path)
        if snapshot.sha256 != source.sha256:
            raise DataQualityExecutionError("DQ_VALIDATOR_SHA_MISMATCH", source.path)


def _verify_input_binding(
    binding: DataQualityInputBinding,
    root: Path,
) -> _ObservedFile:
    relative, absolute = _repo_path(root, Path(binding.path))
    if relative != binding.path:
        raise DataQualityExecutionError("DQ_INPUT_SET_MISMATCH", binding.path)
    expected_schema, expected_source_role = _expected_input_contract(binding.role)
    if binding.schema_id != expected_schema or binding.source_role != expected_source_role:
        raise DataQualityExecutionError(
            "DQ_INPUT_SET_MISMATCH",
            f"input contract mismatch for role={binding.role}",
        )
    if not absolute.is_file():
        raise DataQualityExecutionError("DQ_INPUT_MISSING", binding.path)
    if not binding.exists:
        raise DataQualityExecutionError("DQ_INPUT_MISSING", binding.path)
    if binding.role == "backtest_manifest":
        observed = _observe_generic_file(relative, absolute)
    else:
        observed = _inspect_csv_file(relative, absolute)
    if observed.sha256 != binding.sha256:
        raise DataQualityExecutionError("DQ_INPUT_SHA_MISMATCH", binding.path)
    if observed.size_bytes != binding.size_bytes:
        raise DataQualityExecutionError("DQ_INPUT_SIZE_MISMATCH", binding.path)
    if observed.row_count != binding.row_count:
        raise DataQualityExecutionError("DQ_INPUT_ROW_COUNT_MISMATCH", binding.path)
    required_columns = _required_columns_for_role(binding.role)
    if required_columns and not set(required_columns).issubset(observed.columns):
        raise DataQualityExecutionError(
            "DQ_INPUT_SET_MISMATCH", f"schema mismatch for {binding.role}"
        )
    if binding.role != "backtest_manifest":
        if binding.manifest_path is None or binding.manifest_sha256 is None:
            raise DataQualityExecutionError("DQ_MANIFEST_MISSING", binding.path)
        manifest_relative, manifest_absolute = _repo_path(root, Path(binding.manifest_path))
        manifest = _read_manifest_rows(manifest_relative, manifest_absolute)
        if not manifest.exists:
            raise DataQualityExecutionError("DQ_MANIFEST_MISSING", manifest_relative)
        if manifest.sha256 != binding.manifest_sha256:
            raise DataQualityExecutionError("DQ_MANIFEST_SHA_MISMATCH", manifest_relative)
        match = _match_manifest(observed, manifest)
        if match.error_code is not None:
            raise DataQualityExecutionError(match.error_code, match.error_message or binding.path)
        if match.source_ids != binding.matched_source_ids:
            raise DataQualityExecutionError("DQ_SOURCE_ID_UNREVIEWED", binding.path)
        if match.record_refs != binding.matched_record_refs:
            raise DataQualityExecutionError("DQ_MANIFEST_CURRENT_CHECKSUM_MISSING", binding.path)
    return observed


def _inspect_csv_file(relative: str, absolute: Path) -> _ObservedFile:
    snapshot = capture_data_file_snapshots({"input": absolute})["input"]
    if not snapshot.exists or snapshot.content is None:
        raise DataQualityExecutionError("DQ_INPUT_MISSING", relative)
    try:
        columns, row_count = _inspect_csv_content(snapshot.content)
    except (UnicodeError, csv.Error) as exc:
        raise DataQualityExecutionError(
            "DQ_INPUT_ROW_COUNT_MISMATCH", f"cannot inspect {relative}: {exc}"
        ) from exc
    if len(columns) != len(set(columns)):
        raise DataQualityExecutionError(
            "DQ_INPUT_SET_MISMATCH", f"duplicate CSV columns: {relative}"
        )
    return _ObservedFile(
        relative,
        absolute,
        True,
        snapshot.sha256,
        len(snapshot.content),
        row_count,
        columns,
    )


def _required_columns_for_role(role: str) -> tuple[str, ...]:
    if role in {"prices", "secondary_prices"}:
        return PRICE_REQUIRED_COLUMNS
    if role == "rates":
        return RATE_REQUIRED_COLUMNS
    return ()


def _expected_input_contract(role: str) -> tuple[str, str]:
    contracts = {
        "prices": ("prices_daily.v1", "primary_market_prices"),
        "rates": ("rates_daily.v1", "primary_macro_rates"),
        "secondary_prices": ("prices_daily.v1", "secondary_market_prices"),
        "backtest_manifest": (
            "backtest_input_manifest.json.v1",
            "validation_context",
        ),
    }
    try:
        return contracts[role]
    except KeyError as exc:
        raise DataQualityExecutionError(
            "DQ_INPUT_SET_MISMATCH", f"unsupported input role={role}"
        ) from exc


def _invocation_payload(receipt: DataQualityExecutionReceipt) -> dict[str, object]:
    return {item.name: json.loads(item.value_json) for item in receipt.invocation}


def verify_daily_default_execution_profile_receipt(
    receipt: DataQualityExecutionReceipt,
    *,
    project_root: Path = PROJECT_ROOT,
) -> None:
    """Prove that a receipt is bound to the reviewed daily-default input profile."""

    if not isinstance(receipt, DataQualityExecutionReceipt):
        raise DataQualityExecutionError(
            "DQ_RECEIPT_FIELDS_INVALID", "receipt must be DataQualityExecutionReceipt"
        )
    invocation = _invocation_payload(receipt)
    if set(invocation) != _CANONICAL_INVOCATION_NAMES:
        raise DataQualityExecutionError(
            "DQ_RECEIPT_FIELDS_INVALID", "canonical invocation parameter set mismatch"
        )
    if invocation.get("execution_profile_id") != DAILY_DEFAULT_DATA_QUALITY_EXECUTION_PROFILE_ID:
        raise DataQualityExecutionError(
            "DQ_RECEIPT_FIELDS_INVALID", "receipt is not daily_default.v1"
        )
    _verify_execution_profile_invocation(
        invocation,
        observed_input_paths={item.role: item.path for item in receipt.inputs},
        project_root=project_root.resolve(),
    )


def _verify_invocation(
    receipt: DataQualityExecutionReceipt,
    observed_inputs: Mapping[str, _ObservedFile],
    project_root: Path,
) -> None:
    invocation = _invocation_payload(receipt)
    if set(invocation) != _CANONICAL_INVOCATION_NAMES:
        raise DataQualityExecutionError(
            "DQ_RECEIPT_FIELDS_INVALID", "canonical invocation parameter set mismatch"
        )
    expected_values: dict[str, object] = {
        "as_of": receipt.as_of.isoformat(),
        "backtest_manifest_path": (
            observed_inputs["backtest_manifest"].path
            if "backtest_manifest" in observed_inputs
            else None
        ),
        "evaluated_window": receipt.evaluated_window.to_dict(),
        "policy_path": receipt.policy.path,
        "prices_path": observed_inputs["prices"].path,
        "rates_path": observed_inputs["rates"].path,
        "requested_window": receipt.requested_window.to_dict(),
        "secondary_prices_path": (
            observed_inputs["secondary_prices"].path
            if "secondary_prices" in observed_inputs
            else None
        ),
    }
    manifest_paths = {
        item.manifest_path for item in receipt.inputs if item.role != "backtest_manifest"
    }
    if len(manifest_paths) != 1 or invocation.get("manifest_path") not in manifest_paths:
        raise DataQualityExecutionError(
            "DQ_INPUT_SET_MISMATCH", "invocation manifest_path mismatch"
        )
    for name, value in expected_values.items():
        if invocation.get(name) != value:
            code = "DQ_WINDOW_MISMATCH" if "window" in name else "DQ_INPUT_SET_MISMATCH"
            if name == "as_of":
                code = "DQ_AS_OF_MISMATCH"
            elif name == "policy_path":
                code = "DQ_POLICY_PATH_MISMATCH"
            raise DataQualityExecutionError(code, f"invocation {name} mismatch")
    if invocation.get("execution_profile_id") not in _SUPPORTED_EXECUTION_PROFILE_IDS:
        raise DataQualityExecutionError(
            "DQ_RECEIPT_FIELDS_INVALID", "unsupported invocation execution_profile_id"
        )
    _verify_execution_profile_invocation(
        invocation,
        observed_input_paths={role: item.path for role, item in observed_inputs.items()},
        project_root=project_root,
    )
    if not isinstance(invocation.get("require_secondary_prices"), bool):
        raise DataQualityExecutionError(
            "DQ_RECEIPT_FIELDS_INVALID", "require_secondary_prices must be boolean"
        )
    if invocation["require_secondary_prices"] is True and "secondary_prices" not in observed_inputs:
        raise DataQualityExecutionError("DQ_INPUT_MISSING", "secondary_prices")
    for name in ("expected_price_tickers", "expected_rate_series"):
        value = invocation.get(name)
        if (
            not isinstance(value, list)
            or not value
            or any(not isinstance(item, str) or not item for item in value)
            or len(set(value)) != len(value)
        ):
            raise DataQualityExecutionError(
                "DQ_RECEIPT_FIELDS_INVALID", f"invalid invocation {name}"
            )


def _verify_execution_profile_invocation(
    invocation: Mapping[str, object],
    *,
    observed_input_paths: Mapping[str, str],
    project_root: Path,
) -> None:
    profile_id = invocation.get("execution_profile_id")
    config_path = invocation.get("execution_profile_config_path")
    config_sha256 = invocation.get("execution_profile_config_sha256")
    if profile_id == MANUAL_DATA_QUALITY_EXECUTION_PROFILE_ID:
        if config_path is not None or config_sha256 is not None:
            raise DataQualityExecutionError(
                "DQ_RECEIPT_FIELDS_INVALID",
                "manual.v1 cannot carry a daily-default profile config binding",
            )
        return
    if profile_id != DAILY_DEFAULT_DATA_QUALITY_EXECUTION_PROFILE_ID:
        raise DataQualityExecutionError(
            "DQ_RECEIPT_FIELDS_INVALID", "unsupported invocation execution_profile_id"
        )
    _require_daily_default_profile_shape(
        prices_path=observed_input_paths.get("prices"),
        rates_path=observed_input_paths.get("rates"),
        manifest_path=invocation.get("manifest_path"),
        secondary_prices_path=observed_input_paths.get("secondary_prices"),
        backtest_manifest_path=observed_input_paths.get("backtest_manifest"),
        policy_path=invocation.get("policy_path"),
        require_secondary_prices=invocation.get("require_secondary_prices"),
    )
    expected_config_path = _DAILY_DEFAULT_UNIVERSE_PATH.as_posix()
    if config_path != expected_config_path or not isinstance(config_sha256, str):
        raise DataQualityExecutionError(
            "DQ_RECEIPT_FIELDS_INVALID", "daily-default profile config binding is incomplete"
        )
    snapshot, universe = _capture_daily_default_universe(project_root)
    if snapshot.sha256 != config_sha256:
        raise DataQualityExecutionError(
            "DQ_INPUT_SHA_MISMATCH", "daily-default universe config checksum mismatch"
        )
    expected_tickers = configured_price_tickers(universe, include_full_ai_chain=False)
    expected_rates = configured_rate_series(universe)
    if invocation.get("expected_price_tickers") != expected_tickers:
        raise DataQualityExecutionError(
            "DQ_RECEIPT_FIELDS_INVALID",
            "daily-default ticker universe differs from the bound core universe",
        )
    if invocation.get("expected_rate_series") != expected_rates:
        raise DataQualityExecutionError(
            "DQ_RECEIPT_FIELDS_INVALID",
            "daily-default rate universe differs from the bound universe config",
        )


def _verify_report_binding(
    receipt: DataQualityExecutionReceipt,
    root: Path,
) -> _ParsedReport:
    relative, absolute = _repo_path(root, Path(receipt.report.path))
    if relative != receipt.report.path or absolute != root / Path(relative):
        raise DataQualityExecutionError("DQ_REPORT_MISSING", receipt.report.path)
    content = _read_secure_output_bytes(
        root,
        relative,
        missing_code="DQ_REPORT_MISSING",
    )
    if hashlib.sha256(content).hexdigest() != receipt.report.sha256:
        raise DataQualityExecutionError("DQ_REPORT_SHA_MISMATCH", receipt.report.path)
    if len(content) != receipt.report.size_bytes:
        raise DataQualityExecutionError("DQ_REPORT_SHA_MISMATCH", "report size mismatch")
    parsed = _parse_report(content)
    if parsed.status != receipt.report.status:
        raise DataQualityExecutionError("DQ_REPORT_STATUS_CONFLICT", receipt.report.path)
    if (
        parsed.error_count != receipt.report.error_count
        or parsed.warning_count != receipt.report.warning_count
        or parsed.info_count != receipt.report.info_count
    ):
        raise DataQualityExecutionError("DQ_REPORT_COUNT_MISMATCH", receipt.report.path)
    if (
        parsed.issue_codes != receipt.report.issue_codes
        or parsed.blocking_issue_codes != receipt.report.blocking_issue_codes
    ):
        raise DataQualityExecutionError("DQ_REPORT_COUNT_MISMATCH", "report issue projection")
    if parsed.as_of != receipt.as_of or parsed.checked_at != receipt.checked_at:
        raise DataQualityExecutionError("DQ_AS_OF_MISMATCH", "report chronology mismatch")
    return parsed


def _parse_report(content: bytes) -> _ParsedReport:
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise DataQualityExecutionError("DQ_REPORT_STATUS_CONFLICT", "report is not UTF-8") from exc
    lines = text.splitlines()
    status = _report_value(lines, "- 状态：")
    try:
        checked_at = datetime.fromisoformat(
            _report_value(lines, "- 检查时间：").replace("Z", "+00:00")
        )
        as_of = date.fromisoformat(_report_value(lines, "- 评估日期："))
        error_count = int(_report_value(lines, "- 错误数："))
        warning_count = int(_report_value(lines, "- 警告数："))
        info_count = int(_report_value(lines, "- 信息数："))
    except ValueError as exc:
        raise DataQualityExecutionError(
            "DQ_REPORT_COUNT_MISMATCH", "invalid report header"
        ) from exc
    if checked_at.tzinfo is None or checked_at.utcoffset() is None:
        raise DataQualityExecutionError(
            "DQ_EXECUTION_CHRONOLOGY_INVALID", "report checked_at lacks timezone"
        )
    checked_at = checked_at.astimezone(UTC)
    issue_lines = _report_issue_lines(lines)
    issue_codes: set[str] = set()
    blocking_codes: set[str] = set()
    observed_counts = {"错误": 0, "警告": 0, "信息": 0}
    for line in issue_lines:
        parts = line.split("|")
        if len(parts) < 5:
            raise DataQualityExecutionError("DQ_REPORT_COUNT_MISMATCH", "invalid issue row")
        severity = parts[1].strip()
        code = parts[3].strip()
        if severity not in observed_counts or not code or code == "Code":
            continue
        observed_counts[severity] += 1
        issue_codes.add(code)
        if severity == "错误":
            blocking_codes.add(code)
    if (
        observed_counts["错误"] != error_count
        or observed_counts["警告"] != warning_count
        or observed_counts["信息"] != info_count
    ):
        raise DataQualityExecutionError(
            "DQ_REPORT_COUNT_MISMATCH", "report header/table counts differ"
        )
    derived_status = "FAIL" if error_count else "PASS_WITH_WARNINGS" if warning_count else "PASS"
    if status != derived_status:
        raise DataQualityExecutionError("DQ_REPORT_STATUS_CONFLICT", status)
    return _ParsedReport(
        status=status,
        checked_at=checked_at,
        as_of=as_of,
        error_count=error_count,
        warning_count=warning_count,
        info_count=info_count,
        issue_codes=tuple(sorted(issue_codes)),
        blocking_issue_codes=tuple(sorted(blocking_codes)),
        expected_price_tickers=_split_report_list(
            _report_section_value(lines, "## 预期覆盖范围", "- 价格标的：")
        ),
        expected_rate_series=_split_report_list(
            _report_section_value(lines, "## 预期覆盖范围", "- FRED 宏观序列：")
        ),
    )


def _report_value(lines: Sequence[str], prefix: str) -> str:
    matches = [line[len(prefix) :] for line in lines if line.startswith(prefix)]
    if len(matches) != 1 or not matches[0]:
        raise DataQualityExecutionError(
            "DQ_REPORT_STATUS_CONFLICT", f"missing or duplicate report field {prefix}"
        )
    return matches[0]


def _report_section_value(lines: Sequence[str], heading: str, prefix: str) -> str:
    try:
        start = lines.index(heading) + 1
    except ValueError as exc:
        raise DataQualityExecutionError(
            "DQ_REPORT_STATUS_CONFLICT", f"missing report section {heading}"
        ) from exc
    section: list[str] = []
    for line in lines[start:]:
        if line.startswith("## "):
            break
        section.append(line)
    return _report_value(section, prefix)


def _report_issue_lines(lines: Sequence[str]) -> tuple[str, ...]:
    try:
        start = lines.index("## 问题") + 1
    except ValueError as exc:
        raise DataQualityExecutionError(
            "DQ_REPORT_COUNT_MISMATCH", "report issue section missing"
        ) from exc
    result: list[str] = []
    for line in lines[start:]:
        if line.startswith("## "):
            break
        if line.startswith("| ") and not line.startswith("|---"):
            result.append(line)
    return tuple(result)


def _split_report_list(value: str) -> tuple[str, ...]:
    values = tuple(part.strip() for part in value.split(",") if part.strip())
    if not values:
        raise DataQualityExecutionError(
            "DQ_REPORT_COUNT_MISMATCH", "report expected-input list is empty"
        )
    return values


def _verify_report_invocation_projection(
    receipt: DataQualityExecutionReceipt,
    report: _ParsedReport,
) -> None:
    invocation = _invocation_payload(receipt)
    price_tickers = _json_string_tuple(
        invocation.get("expected_price_tickers"), "expected_price_tickers"
    )
    rate_series = _json_string_tuple(invocation.get("expected_rate_series"), "expected_rate_series")
    if price_tickers != report.expected_price_tickers:
        raise DataQualityExecutionError(
            "DQ_REPORT_COUNT_MISMATCH", "price ticker projection mismatch"
        )
    if rate_series != report.expected_rate_series:
        raise DataQualityExecutionError(
            "DQ_REPORT_COUNT_MISMATCH", "rate series projection mismatch"
        )


def _json_string_tuple(value: object, field: str) -> tuple[str, ...]:
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise DataQualityExecutionError(
            "DQ_RECEIPT_FIELDS_INVALID", f"{field} must be a string list"
        )
    return tuple(value)
