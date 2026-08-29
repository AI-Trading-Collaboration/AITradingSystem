from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Literal, Self

import pandas as pd
from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.contracts.data_quality_execution import DataQualityDateWindow
from ai_trading_system.data.download_publication import (
    DownloadArtifactCandidate,
    DownloadReplayInputCandidate,
    DownloadSourceBinding,
    ValidatedDownloadPublication,
    publish_download_transaction,
)
from ai_trading_system.data.quality_execution import (
    CanonicalDataQualityExecutionRequest,
    CanonicalDataQualityExecutionResult,
    run_canonical_data_quality_execution,
)
from ai_trading_system.platform.artifacts import sha256_path, write_bytes_atomic
from ai_trading_system.trading_calendar import is_us_equity_trading_day
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_EXACT_SIGNAL_PACKAGE_ADMISSION_POLICY_PATH = Path(
    "config/research/qc_qqq_options_exact_signal_package_admission_v1.yaml"
)

_SHA256 = frozenset("0123456789abcdef")
_EXPECTED_MAPPING = {
    "constructive": "LONG_CALL",
    "defensive": "FLAT",
    "neutral": "FLAT",
    "risk_off": "FLAT",
    "risk_on": "LONG_CALL",
}
_EXPECTED_PROHIBITIONS = (
    "CROSS_SESSION_FORWARD_FILL",
    "FLAT_GAP_FILL",
    "MANUAL_CSV_ROWS",
    "POC_REWRAP",
    "TRAINING_WINDOW_REDUCTION",
    "TREND_POLICY_CHANGE_FOR_OPTIONS",
    "WARM_START_DIAGNOSTIC",
)


class ExactSignalPackageAdmissionError(ValueError):
    def __init__(self, reason_code: str, detail: str) -> None:
        super().__init__(f"{reason_code}: {detail}")
        self.reason_code = reason_code
        self.detail = detail


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


def _sha256(value: str, field: str) -> str:
    if len(value) != 64 or any(character not in _SHA256 for character in value):
        raise ValueError(f"{field} must be lowercase SHA-256")
    return value


def _canonical_json_bytes(value: object) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + "\n"
    ).encode("utf-8")


def _file_sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


class ResearchWindowPolicy(_StrictModel):
    calendar: Literal["XNYS"]
    requested_start: date
    requested_end: date
    evaluated_start: date
    evaluated_end: date
    expected_session_count: int

    @model_validator(mode="after")
    def validate_exact_window(self) -> Self:
        expected = (
            date(2021, 2, 22),
            date(2025, 12, 2),
            date(2021, 2, 22),
            date(2025, 12, 2),
            1202,
        )
        observed = (
            self.requested_start,
            self.requested_end,
            self.evaluated_start,
            self.evaluated_end,
            self.expected_session_count,
        )
        if observed != expected:
            raise ValueError("primary 1202-session window drifted")
        return self


class DataQualityPolicy(_StrictModel):
    policy_path: str
    policy_sha256: str
    input_prices_path: str
    input_rates_path: str
    input_secondary_prices_path: str
    expected_price_tickers: tuple[str, ...]
    expected_rate_series: tuple[str, ...]
    require_secondary_prices: Literal[True]
    materialization: Literal["EXACT_DATE_AND_IDENTITY_FILTER_ONLY"]
    publication_source_kind: Literal["LEGACY_LOCAL_CACHE_IMPORT"]
    origin_status: Literal["OPAQUE_LEGACY"]
    canonical_execution_required: Literal[True]
    exact_pass_required: Literal[True]

    @field_validator("policy_sha256")
    @classmethod
    def validate_hash(cls, value: str) -> str:
        return _sha256(value, "policy_sha256")

    @model_validator(mode="after")
    def validate_scope(self) -> Self:
        if self.expected_price_tickers != ("QQQ", "SGOV", "TQQQ"):
            raise ValueError("exact price scope drifted")
        if self.expected_rate_series != ("DGS10", "DGS2", "DTWEXBGS"):
            raise ValueError("exact rate scope drifted")
        return self


class ProducerPolicy(_StrictModel):
    producer_id: Literal["first_layer_composer_v2"]
    producer_policy_path: str
    producer_policy_sha256: str
    coverage_policy_path: str
    coverage_policy_sha256: str
    output_relative_path: Literal["models/first_layer_composer_v2_predictions.csv"]
    date_column: Literal["date"]
    state_column: Literal["trend_state"]
    model_id_column: Literal["model_id"]
    expected_model_id: Literal["first_layer_composer_v2"]
    expected_research_window_id: Literal["exact_three_asset_validated"]
    allowed_states: tuple[str, ...]

    @field_validator("producer_policy_sha256", "coverage_policy_sha256")
    @classmethod
    def validate_hashes(cls, value: str) -> str:
        return _sha256(value, "producer authority hash")

    @model_validator(mode="after")
    def validate_states(self) -> Self:
        if self.allowed_states != tuple(sorted(_EXPECTED_MAPPING)):
            raise ValueError("producer state enum drifted")
        return self


class AuthorityPolicy(_StrictModel):
    exact_freeze_admission_path: str
    exact_freeze_admission_sha256: str
    signal_export_policy_path: str
    signal_export_policy_sha256: str

    @field_validator("exact_freeze_admission_sha256", "signal_export_policy_sha256")
    @classmethod
    def validate_hashes(cls, value: str) -> str:
        return _sha256(value, "authority hash")


class SafetyPolicy(_StrictModel):
    scope: Literal["NON_EXECUTABLE_DATA_RESEARCH"]
    quantconnect_project_mutation_allowed: Literal[False]
    quantconnect_backtest_allowed: Literal[False]
    provider_query_allowed: Literal[False]
    raw_option_payload_allowed: Literal[False]
    orders: Literal[0]
    fills: Literal[0]
    positions: Literal[0]
    paper_allowed: Literal[False]
    live_allowed: Literal[False]
    production_allowed: Literal[False]
    broker_action: Literal["none"]


class ExactSignalPackageAdmissionPolicy(_StrictModel):
    schema_version: Literal["qc_qqq_options_exact_signal_package_admission_policy.v1"]
    policy_id: Literal["qc_qqq_options_exact_signal_package_admission_v1"]
    policy_version: Literal["1.0.0"]
    status: Literal["OWNER_AUTHORIZED_NON_EXECUTABLE_DATA_RESEARCH"]
    owner: Literal["project_owner"]
    owner_decision_id: Literal[
        "owner_decision:TRADING-2542I:2026-08-29:"
        "authorize_real_dq_existing_producer_regeneration_v1"
    ]
    research_window: ResearchWindowPolicy
    data_quality: DataQualityPolicy
    producer: ProducerPolicy
    signal_mapping: dict[str, Literal["LONG_CALL", "FLAT"]]
    authorities: AuthorityPolicy
    prohibited_transformations: tuple[str, ...]
    safety: SafetyPolicy
    review_condition: str

    @model_validator(mode="after")
    def validate_freeze(self) -> Self:
        if self.signal_mapping != _EXPECTED_MAPPING:
            raise ValueError("frozen five-state mapping drifted")
        if self.prohibited_transformations != _EXPECTED_PROHIBITIONS:
            raise ValueError("prohibited transformation inventory drifted")
        if not self.review_condition.strip():
            raise ValueError("review_condition is required")
        return self


@dataclass(frozen=True)
class LoadedAdmissionPolicy:
    policy: ExactSignalPackageAdmissionPolicy
    path: Path
    file_sha256: str


class ArtifactBinding(_StrictModel):
    path: str
    sha256: str
    size_bytes: int
    row_count: int | None = None

    @field_validator("sha256")
    @classmethod
    def validate_hash(cls, value: str) -> str:
        return _sha256(value, "artifact hash")


class SignalSourceAudit(_StrictModel):
    source: ArtifactBinding
    expected_session_count: int
    in_window_row_count: int
    unique_session_count: int
    first_observed_session: date | None
    last_observed_session: date | None
    missing_session_count: int
    missing_sessions: tuple[date, ...]
    duplicate_session_count: int
    duplicate_excess_row_count: int
    duplicate_sessions: tuple[date, ...]
    unexpected_session_count: int
    invalid_state_row_count: int
    invalid_identity_row_count: int
    invalid_timing_row_count: int
    blocker_codes: tuple[str, ...]
    admission_status: Literal["PASS", "REJECT"]

    @model_validator(mode="after")
    def validate_status(self) -> Self:
        expected = "PASS" if not self.blocker_codes else "REJECT"
        if self.admission_status != expected:
            raise ValueError("signal audit status conflicts with blockers")
        return self


class ExactSignalPackageAdmissionReceipt(_StrictModel):
    schema_version: Literal["qc_qqq_options_exact_signal_package_admission_receipt.v1"]
    policy_id: str
    policy_version: str
    policy_file_sha256: str
    generated_at_utc: datetime
    authorization_state: Literal["EXACT_PREAUTHORIZED"]
    data_quality_status: Literal["PASS", "FAIL"]
    data_quality_receipt: ArtifactBinding
    data_quality_report: ArtifactBinding
    source_audit: SignalSourceAudit
    package_generation_status: Literal[
        "NOT_RUN_SOURCE_REJECTED", "READY_FOR_EXISTING_2483_WRITER"
    ]
    quantconnect_status: Literal["NOT_AUTHORIZED_NOT_RUN"]
    orders: Literal[0]
    fills: Literal[0]
    positions: Literal[0]
    production_effect: Literal["none"]
    broker_action: Literal["none"]

    @field_validator("policy_file_sha256")
    @classmethod
    def validate_policy_hash(cls, value: str) -> str:
        return _sha256(value, "policy_file_sha256")

    @model_validator(mode="after")
    def validate_terminal(self) -> Self:
        ready = self.data_quality_status == "PASS" and self.source_audit.admission_status == "PASS"
        expected = "READY_FOR_EXISTING_2483_WRITER" if ready else "NOT_RUN_SOURCE_REJECTED"
        if self.package_generation_status != expected:
            raise ValueError("package status conflicts with DQ/source admission")
        return self

    @property
    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.model_dump(mode="json"))


@dataclass(frozen=True)
class ExactWindowDataQualityRun:
    publication: ValidatedDownloadPublication
    execution: CanonicalDataQualityExecutionResult
    lineage_path: Path


def load_exact_signal_package_admission_policy(
    path: Path = DEFAULT_EXACT_SIGNAL_PACKAGE_ADMISSION_POLICY_PATH,
    *,
    project_root: Path = PROJECT_ROOT,
) -> LoadedAdmissionPolicy:
    resolved = _bound_input_file(path, project_root)
    try:
        content = resolved.read_bytes()
        payload = safe_load_yaml_path(resolved)
        if not isinstance(payload, dict):
            raise TypeError("policy root must be a mapping")
        policy = ExactSignalPackageAdmissionPolicy.model_validate(payload, strict=False)
        _verify_policy_authorities(policy, project_root)
    except (OSError, TypeError, ValueError) as exc:
        raise ExactSignalPackageAdmissionError(
            "EXACT_SIGNAL_ADMISSION_POLICY_INVALID", str(exc)
        ) from exc
    return LoadedAdmissionPolicy(policy=policy, path=resolved, file_sha256=_file_sha256(content))


def run_exact_window_canonical_data_quality(
    *,
    loaded_policy: LoadedAdmissionPolicy,
    output_root: Path,
    project_root: Path = PROJECT_ROOT,
    published_at: datetime | None = None,
) -> ExactWindowDataQualityRun:
    policy = loaded_policy.policy
    root = project_root.resolve()
    if output_root.exists():
        raise ExactSignalPackageAdmissionError(
            "EXACT_WINDOW_OUTPUT_ALREADY_EXISTS", str(output_root)
        )
    output_root.mkdir(parents=True, exist_ok=False)
    dq_root = output_root / "canonical_download"
    source_specs = (
        (
            "prices",
            policy.data_quality.input_prices_path,
            "prices_daily.csv",
            "ticker",
            policy.data_quality.expected_price_tickers,
        ),
        (
            "rates",
            policy.data_quality.input_rates_path,
            "rates_daily.csv",
            "series",
            policy.data_quality.expected_rate_series,
        ),
        (
            "secondary_prices",
            policy.data_quality.input_secondary_prices_path,
            "prices_marketstack_daily.csv",
            "ticker",
            policy.data_quality.expected_price_tickers,
        ),
    )
    artifacts: list[DownloadArtifactCandidate] = []
    bindings: list[DownloadSourceBinding] = []
    lineage_inputs: list[dict[str, Any]] = []
    for role, source_relative, filename, identity_column, identities in source_specs:
        source_path = _bound_input_file(Path(source_relative), root)
        source_bytes = source_path.read_bytes()
        frame = pd.read_csv(source_path)
        projected = _project_exact_window(
            frame,
            identity_column=identity_column,
            identities=identities,
            start=policy.research_window.requested_start,
            end=policy.research_window.requested_end,
            role=role,
        )
        content = projected.to_csv(index=False, lineterminator="\n").encode("utf-8")
        event_id = f"trading-2542i-{role}-exact-window-local-cache"
        row_keys = tuple(
            sorted(
                (str(identity), str(day))
                for identity, day in projected[[identity_column, "date"]].itertuples(
                    index=False, name=None
                )
            )
        )
        artifacts.append(
            DownloadArtifactCandidate(
                role=role,
                filename=filename,
                content=content,
                row_count=len(projected),
                source_event_ids=(event_id,),
            )
        )
        bindings.append(
            DownloadSourceBinding(
                source_event_id=event_id,
                artifact_role=role,
                source_kind="LEGACY_LOCAL_CACHE_IMPORT",
                source_id="exact_window_local_cache_projection",
                provider="AITradingSystem local filesystem",
                endpoint=source_relative,
                request_parameters={
                    "cache_relative_path": filename,
                    "cache_sha256": _file_sha256(content),
                    "cache_size_bytes": len(content),
                    "cache_row_count": len(projected),
                    "manifest_relative_path": "download_manifest.csv",
                    "manifest_sha256": None,
                    "manifest_size_bytes": None,
                    "manifest_row_count": None,
                    "manifest_binding_status": "MISSING",
                    "raw_provider_provenance": False,
                    "origin_lineage_complete": False,
                    "origin_status": "OPAQUE_LEGACY",
                    "data_quality_provenance": False,
                    "projection_rule": policy.data_quality.materialization,
                    "upstream_path": source_relative,
                    "upstream_sha256": _file_sha256(source_bytes),
                    "upstream_row_count": len(frame),
                    "requested_start": policy.research_window.requested_start.isoformat(),
                    "requested_end": policy.research_window.requested_end.isoformat(),
                    "identity_scope": list(identities),
                },
                winning_row_count=len(row_keys),
                allocation_mode="REMAINDER",
                winning_row_keys=row_keys,
                replay_inputs=(
                    DownloadReplayInputCandidate(
                        input_role="legacy_local_cache_bytes",
                        filename=filename,
                        content=content,
                        row_count=len(projected),
                    ),
                ),
            )
        )
        lineage_inputs.append(
            {
                "role": role,
                "upstream_path": source_relative,
                "upstream_sha256": _file_sha256(source_bytes),
                "upstream_size_bytes": len(source_bytes),
                "upstream_row_count": len(frame),
                "projected_sha256": _file_sha256(content),
                "projected_size_bytes": len(content),
                "projected_row_count": len(projected),
                "identity_scope": list(identities),
            }
        )
    publication = publish_download_transaction(
        output_dir=dq_root,
        requested_start=policy.research_window.requested_start,
        requested_end=policy.research_window.requested_end,
        artifacts=tuple(artifacts),
        source_bindings=tuple(bindings),
        published_at=published_at or datetime.now(UTC),
    )
    lineage_payload = {
        "schema_version": "qc_qqq_options_exact_window_cache_lineage.v1",
        "policy_id": policy.policy_id,
        "policy_file_sha256": loaded_policy.file_sha256,
        "requested_start": policy.research_window.requested_start.isoformat(),
        "requested_end": policy.research_window.requested_end.isoformat(),
        "materialization": policy.data_quality.materialization,
        "publication_source_kind": policy.data_quality.publication_source_kind,
        "origin_status": policy.data_quality.origin_status,
        "raw_provider_provenance_claimed": False,
        "inputs": lineage_inputs,
        "publication_transaction_id": publication.transaction_id,
        "publication_transaction_sha256": publication.transaction_manifest_sha256,
        "production_effect": "none",
        "broker_action": "none",
    }
    lineage_path = output_root / "exact_window_cache_lineage.json"
    write_bytes_atomic(lineage_path, _canonical_json_bytes(lineage_payload))
    request = CanonicalDataQualityExecutionRequest(
        as_of=policy.research_window.requested_end,
        requested_window=DataQualityDateWindow(
            start=policy.research_window.requested_start,
            end=policy.research_window.requested_end,
        ),
        evaluated_window=DataQualityDateWindow(
            start=policy.research_window.evaluated_start,
            end=policy.research_window.evaluated_end,
        ),
        prices_path=publication.legacy_prices_path,
        rates_path=publication.legacy_rates_path,
        manifest_path=publication.legacy_manifest_path,
        secondary_prices_path=publication.legacy_secondary_prices_path,
        require_secondary_prices=policy.data_quality.require_secondary_prices,
        expected_price_tickers=policy.data_quality.expected_price_tickers,
        expected_rate_series=policy.data_quality.expected_rate_series,
        policy_path=Path(policy.data_quality.policy_path),
    )
    execution = run_canonical_data_quality_execution(request, project_root=root)
    return ExactWindowDataQualityRun(
        publication=publication,
        execution=execution,
        lineage_path=lineage_path,
    )


def audit_first_layer_signal_source(
    *,
    loaded_policy: LoadedAdmissionPolicy,
    source_path: Path,
    project_root: Path = PROJECT_ROOT,
) -> SignalSourceAudit:
    policy = loaded_policy.policy
    root = project_root.resolve()
    resolved = _bound_input_file(source_path, root)
    content = resolved.read_bytes()
    frame = pd.read_csv(resolved)
    required = {
        policy.producer.date_column,
        policy.producer.state_column,
        policy.producer.model_id_column,
        "research_window_id",
        "known_at",
        "available_at",
        "decision_at",
    }
    missing_columns = sorted(required - set(frame.columns))
    if missing_columns:
        raise ExactSignalPackageAdmissionError(
            "SIGNAL_SOURCE_SCHEMA_INVALID", ",".join(missing_columns)
        )
    parsed = pd.to_datetime(frame[policy.producer.date_column], errors="coerce")
    frame = frame.assign(_session=parsed.dt.date)
    start = policy.research_window.evaluated_start
    end = policy.research_window.evaluated_end
    scoped = frame.loc[
        frame["_session"].map(lambda value: value is not None and start <= value <= end)
    ]
    expected_sessions = _xnys_sessions(start, end)
    expected_set = set(expected_sessions)
    actual_sessions = tuple(
        sorted(
            value
            for value in set(scoped["_session"].dropna())
            if isinstance(value, date)
        )
    )
    counts = scoped.groupby("_session", dropna=True).size()
    duplicate_sessions: tuple[date, ...] = tuple(
        sorted(
            day
            for day, count in counts.items()
            if isinstance(day, date) and int(count) > 1
        )
    )
    missing_sessions = tuple(sorted(expected_set - set(actual_sessions)))
    unexpected_sessions = tuple(sorted(set(actual_sessions) - expected_set))
    allowed_states = set(policy.producer.allowed_states)
    invalid_state_rows = int((~scoped[policy.producer.state_column].isin(allowed_states)).sum())
    invalid_identity = int(
        (
            (scoped[policy.producer.model_id_column] != policy.producer.expected_model_id)
            | (scoped["research_window_id"] != policy.producer.expected_research_window_id)
        ).sum()
    )
    invalid_timing = _invalid_timing_rows(scoped)
    blockers: list[str] = []
    first_observed = actual_sessions[0] if actual_sessions else None
    if first_observed is None or first_observed > start:
        blockers.append("SOURCE_PRIMARY_START_NOT_COVERED")
    if missing_sessions or unexpected_sessions:
        blockers.append("SOURCE_SESSION_COVERAGE_MISMATCH")
    if duplicate_sessions:
        blockers.append("SOURCE_DUPLICATE_SESSION")
    if invalid_state_rows:
        blockers.append("SOURCE_STATE_INVALID")
    if invalid_identity:
        blockers.append("SOURCE_IDENTITY_INVALID")
    if invalid_timing:
        blockers.append("SOURCE_TIMING_INVALID")
    return SignalSourceAudit(
        source=ArtifactBinding(
            path=resolved.relative_to(root).as_posix(),
            sha256=_file_sha256(content),
            size_bytes=len(content),
            row_count=len(frame),
        ),
        expected_session_count=len(expected_sessions),
        in_window_row_count=len(scoped),
        unique_session_count=len(actual_sessions),
        first_observed_session=first_observed,
        last_observed_session=actual_sessions[-1] if actual_sessions else None,
        missing_session_count=len(missing_sessions),
        missing_sessions=missing_sessions,
        duplicate_session_count=len(duplicate_sessions),
        duplicate_excess_row_count=int(sum(int(count) - 1 for count in counts if int(count) > 1)),
        duplicate_sessions=duplicate_sessions,
        unexpected_session_count=len(unexpected_sessions),
        invalid_state_row_count=invalid_state_rows,
        invalid_identity_row_count=invalid_identity,
        invalid_timing_row_count=invalid_timing,
        blocker_codes=tuple(blockers),
        admission_status="PASS" if not blockers else "REJECT",
    )


def build_exact_signal_package_admission_receipt(
    *,
    loaded_policy: LoadedAdmissionPolicy,
    data_quality: CanonicalDataQualityExecutionResult,
    source_audit: SignalSourceAudit,
    generated_at_utc: datetime | None = None,
) -> ExactSignalPackageAdmissionReceipt:
    status: Literal["PASS", "FAIL"] = "PASS" if data_quality.report.status == "PASS" else "FAIL"
    ready = status == "PASS" and source_audit.admission_status == "PASS"
    return ExactSignalPackageAdmissionReceipt(
        schema_version="qc_qqq_options_exact_signal_package_admission_receipt.v1",
        policy_id=loaded_policy.policy.policy_id,
        policy_version=loaded_policy.policy.policy_version,
        policy_file_sha256=loaded_policy.file_sha256,
        generated_at_utc=generated_at_utc or datetime.now(UTC),
        authorization_state="EXACT_PREAUTHORIZED",
        data_quality_status=status,
        data_quality_receipt=_path_binding(data_quality.receipt_path),
        data_quality_report=_path_binding(data_quality.report_path),
        source_audit=source_audit,
        package_generation_status=(
            "READY_FOR_EXISTING_2483_WRITER" if ready else "NOT_RUN_SOURCE_REJECTED"
        ),
        quantconnect_status="NOT_AUTHORIZED_NOT_RUN",
        orders=0,
        fills=0,
        positions=0,
        production_effect="none",
        broker_action="none",
    )


def write_exact_signal_package_admission_receipt(
    receipt: ExactSignalPackageAdmissionReceipt,
    *,
    output_root: Path,
) -> tuple[Path, Path]:
    json_path = output_root / "exact_signal_package_admission_receipt.json"
    markdown_path = output_root / "exact_signal_package_admission_report.md"
    write_bytes_atomic(json_path, receipt.canonical_bytes)
    write_bytes_atomic(markdown_path, _render_receipt_markdown(receipt).encode("utf-8"))
    return json_path, markdown_path


def _verify_policy_authorities(policy: ExactSignalPackageAdmissionPolicy, root: Path) -> None:
    bindings = (
        (policy.data_quality.policy_path, policy.data_quality.policy_sha256),
        (policy.producer.producer_policy_path, policy.producer.producer_policy_sha256),
        (policy.producer.coverage_policy_path, policy.producer.coverage_policy_sha256),
        (
            policy.authorities.exact_freeze_admission_path,
            policy.authorities.exact_freeze_admission_sha256,
        ),
        (
            policy.authorities.signal_export_policy_path,
            policy.authorities.signal_export_policy_sha256,
        ),
    )
    for relative, expected in bindings:
        observed = sha256_path(_bound_input_file(Path(relative), root))
        if observed != expected:
            raise ValueError(f"authority drifted: {relative}")


def _bound_input_file(path: Path, root: Path) -> Path:
    resolved_root = root.resolve()
    candidate = path if path.is_absolute() else resolved_root / path
    resolved = candidate.resolve(strict=True)
    try:
        resolved.relative_to(resolved_root)
    except ValueError as exc:
        raise ValueError(f"path escapes project root: {path}") from exc
    if not resolved.is_file() or candidate.is_symlink():
        raise ValueError(f"path must be a non-symlink regular file: {path}")
    return resolved


def _project_exact_window(
    frame: pd.DataFrame,
    *,
    identity_column: str,
    identities: tuple[str, ...],
    start: date,
    end: date,
    role: str,
) -> pd.DataFrame:
    required = {identity_column, "date"}
    if not required.issubset(frame.columns):
        raise ExactSignalPackageAdmissionError(
            "EXACT_WINDOW_SOURCE_SCHEMA_INVALID", f"{role}: {sorted(required - set(frame.columns))}"
        )
    parsed = pd.to_datetime(frame["date"], errors="coerce")
    mask = (
        frame[identity_column].astype(str).isin(identities)
        & parsed.notna()
        & (parsed.dt.date >= start)
        & (parsed.dt.date <= end)
    )
    projected = frame.loc[mask].copy()
    projected["date"] = parsed.loc[mask].dt.strftime("%Y-%m-%d")
    projected = projected.sort_values(
        [identity_column, "date"], kind="stable"
    ).reset_index(drop=True)
    if projected.empty:
        raise ExactSignalPackageAdmissionError("EXACT_WINDOW_SOURCE_EMPTY", role)
    if projected.duplicated([identity_column, "date"]).any():
        raise ExactSignalPackageAdmissionError("EXACT_WINDOW_SOURCE_DUPLICATE_KEY", role)
    if set(projected[identity_column].astype(str)) != set(identities):
        raise ExactSignalPackageAdmissionError("EXACT_WINDOW_SOURCE_SCOPE_INCOMPLETE", role)
    return projected


def _xnys_sessions(start: date, end: date) -> tuple[date, ...]:
    sessions = tuple(
        timestamp.date()
        for timestamp in pd.date_range(start=start, end=end, freq="D")
        if is_us_equity_trading_day(timestamp.date())
    )
    return sessions


def _invalid_timing_rows(scoped: pd.DataFrame) -> int:
    known = pd.to_datetime(scoped["known_at"], errors="coerce").dt.date
    available = pd.to_datetime(scoped["available_at"], errors="coerce").dt.date
    decision = pd.to_datetime(scoped["decision_at"], errors="coerce").dt.date
    sessions = scoped["_session"]
    invalid = known.isna() | available.isna() | decision.isna()
    invalid |= known != sessions
    invalid |= available != sessions
    invalid |= decision.map(
        lambda value: value is None or not is_us_equity_trading_day(value)
    )
    invalid |= decision <= sessions
    return int(invalid.sum())


def _path_binding(path: Path) -> ArtifactBinding:
    content = path.read_bytes()
    return ArtifactBinding(
        path=path.resolve().relative_to(PROJECT_ROOT.resolve()).as_posix(),
        sha256=_file_sha256(content),
        size_bytes=len(content),
        row_count=None,
    )


def _render_receipt_markdown(receipt: ExactSignalPackageAdmissionReceipt) -> str:
    audit = receipt.source_audit
    blockers = "、".join(f"`{item}`" for item in audit.blocker_codes) or "无"
    return "\n".join(
        (
            "# QQQ Options exact signal package admission",
            "",
            f"- DQ：`{receipt.data_quality_status}`",
            f"- signal admission：`{audit.admission_status}`",
            f"- package：`{receipt.package_generation_status}`",
            f"- exact sessions：`{audit.unique_session_count}/{audit.expected_session_count}`",
            f"- missing sessions：`{audit.missing_session_count}`",
            f"- duplicate sessions / excess rows：`{audit.duplicate_session_count}` / "
            f"`{audit.duplicate_excess_row_count}`",
            f"- observed range：`{audit.first_observed_session}` .. "
            f"`{audit.last_observed_session}`",
            f"- blockers：{blockers}",
            "- QuantConnect：`NOT_AUTHORIZED_NOT_RUN`",
            "- orders/fills/positions：`0/0/0`",
            "- production/broker：`none/none`",
            "",
            "该结果只判定现有趋势 producer 是否满足冻结的 exact signal 输入合同；"
            "不会修改趋势 policy、补齐缺失 session、生成手工 FLAT 行或触发期权回测。",
            "",
        )
    )


__all__ = [
    "DEFAULT_EXACT_SIGNAL_PACKAGE_ADMISSION_POLICY_PATH",
    "ExactSignalPackageAdmissionError",
    "ExactSignalPackageAdmissionPolicy",
    "ExactSignalPackageAdmissionReceipt",
    "ExactWindowDataQualityRun",
    "LoadedAdmissionPolicy",
    "SignalSourceAudit",
    "audit_first_layer_signal_source",
    "build_exact_signal_package_admission_receipt",
    "load_exact_signal_package_admission_policy",
    "run_exact_window_canonical_data_quality",
    "write_exact_signal_package_admission_receipt",
]
