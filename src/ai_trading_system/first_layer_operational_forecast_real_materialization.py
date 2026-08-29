from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
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
    verify_data_quality_execution_receipt,
)
from ai_trading_system.first_layer_operational_forecast import (
    OperationalForecastResult,
    build_operational_forecast_series,
    load_operational_forecast_policy,
    write_operational_forecast_artifacts,
)
from ai_trading_system.platform.artifacts import sha256_path, write_bytes_atomic
from ai_trading_system.qqq_options_research.exact_signal_package_admission import (
    SignalSourceAudit,
    audit_first_layer_signal_source,
    build_exact_signal_package_admission_receipt,
    load_exact_signal_package_admission_policy,
    write_exact_signal_package_admission_receipt,
)
from ai_trading_system.qqq_options_research.qc_project_adapter_v2 import (
    LoadedQQQOptionsSignalPackage,
    load_qqq_options_signal_package_for_qc,
)
from ai_trading_system.qqq_options_research.signal_package_v2 import (
    NormalizedDailySignalInput,
    SignalSourceArtifact,
    build_qqq_options_signal_package,
    canonical_normalized_signal_source_bytes,
    write_qqq_options_signal_package,
)
from ai_trading_system.trading_calendar import (
    US_EQUITY_MARKET_TIMEZONE,
    us_equity_market_session,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_REAL_MATERIALIZATION_POLICY_PATH = Path(
    "config/research/first_layer_operational_forecast_real_materialization_v1.yaml"
)

_SHA256_CHARS = frozenset("0123456789abcdef")
_EXPECTED_SCOPE_ROWS = (
    (
        "training_proxy_history",
        date(2018, 1, 2),
        date(2025, 12, 2),
        ("QQQ", "SHY", "TQQQ"),
    ),
    (
        "exact_sgov_history",
        date(2020, 5, 28),
        date(2025, 12, 2),
        ("SGOV",),
    ),
    (
        "primary_evaluation",
        date(2021, 2, 22),
        date(2025, 12, 2),
        ("QQQ", "SGOV", "TQQQ"),
    ),
)
_EXPECTED_RATE_SERIES = ("DGS10", "DGS2", "DTWEXBGS")
_EXPECTED_MAPPING = {
    "constructive": "LONG_CALL",
    "defensive": "FLAT",
    "neutral": "FLAT",
    "risk_off": "FLAT",
    "risk_on": "LONG_CALL",
}
_DQ_INPUT_ROLES = ("prices", "rates", "secondary_prices")


class RealMaterializationError(ValueError):
    def __init__(self, reason_code: str, detail: str) -> None:
        super().__init__(f"{reason_code}: {detail}")
        self.reason_code = reason_code
        self.detail = detail


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


def _sha256(value: str, field: str) -> str:
    if len(value) != 64 or any(character not in _SHA256_CHARS for character in value):
        raise ValueError(f"{field} must be lowercase SHA-256")
    return value


class AuthorityBinding(_StrictModel):
    path: str
    sha256: str

    @field_validator("sha256")
    @classmethod
    def validate_hash(cls, value: str) -> str:
        return _sha256(value, "authority sha256")


class AuthorityPolicy(_StrictModel):
    operational_forecast_policy: AuthorityBinding
    data_quality_policy: AuthorityBinding
    xnys_special_closure_policy: AuthorityBinding
    exact_signal_admission_policy: AuthorityBinding
    signal_export_policy: AuthorityBinding
    project_adapter_policy: AuthorityBinding


class InputPolicy(_StrictModel):
    prices_path: str
    rates_path: str
    secondary_prices_path: str
    price_value_column: Literal["adj_close"]
    rate_value_column: Literal["value"]
    source_kind: Literal["LEGACY_LOCAL_CACHE_IMPORT"]
    origin_status: Literal["OPAQUE_LEGACY"]
    provider_query_allowed: Literal[False]


class DQScopePolicy(_StrictModel):
    scope_id: str
    requested_start: date
    requested_end: date
    expected_price_tickers: tuple[str, ...]
    expected_rate_series: tuple[str, ...]
    require_secondary_prices: bool

    @model_validator(mode="after")
    def validate_scope(self) -> Self:
        if self.requested_start > self.requested_end:
            raise ValueError("DQ scope window is reversed")
        if self.expected_rate_series != _EXPECTED_RATE_SERIES:
            raise ValueError("required rate series drifted")
        return self


class ProducerExecutionPolicy(_StrictModel):
    evaluation_start: date
    evaluation_end: date
    expected_session_count: int
    required_dq_scope_ids: tuple[str, ...]
    exact_signal_mapping: dict[str, Literal["LONG_CALL", "FLAT"]]
    initial_cash_usd: Decimal
    package_run_id: Literal["trading_2542i_operational_forecast_real_v3"]
    package_lineage_id: Literal["trading-2542i-operational-forecast-real-v3"]
    output_root: str
    package_output_root: str
    canonical_replay_required: Literal[True]

    @model_validator(mode="after")
    def validate_execution(self) -> Self:
        if (
            self.evaluation_start,
            self.evaluation_end,
            self.expected_session_count,
        ) != (date(2021, 2, 22), date(2025, 12, 2), 1202):
            raise ValueError("primary evaluation window drifted")
        if self.required_dq_scope_ids != tuple(row[0] for row in _EXPECTED_SCOPE_ROWS):
            raise ValueError("required DQ scope inventory drifted")
        if self.exact_signal_mapping != _EXPECTED_MAPPING:
            raise ValueError("five-state option mapping drifted")
        if self.initial_cash_usd != Decimal("100000"):
            raise ValueError("initial cash drifted")
        return self


class SafetyPolicy(_StrictModel):
    scope: Literal["NON_EXECUTABLE_DATA_RESEARCH"]
    real_cache_materialization_allowed: Literal[True]
    manifest_generation_allowed_after_all_gates_pass: Literal[True]
    quantconnect_backtest_allowed_in_this_materialization_wave: Literal[False]
    maximum_quantconnect_backtests_in_this_materialization_wave: Literal[0]
    provider_query_allowed: Literal[False]
    paid_purchase_allowed: Literal[False]
    raw_option_payload_allowed: Literal[False]
    local_option_repricing_allowed: Literal[False]
    trend_model_redesign_allowed: Literal[False]
    gap_fill_allowed: Literal[False]
    orders_outside_qc_simulation: Literal[0]
    fills_outside_qc_simulation: Literal[0]
    positions_outside_qc_simulation: Literal[0]
    paper_allowed: Literal[False]
    live_allowed: Literal[False]
    production_allowed: Literal[False]
    broker_action: Literal["none"]


class RealMaterializationPolicy(_StrictModel):
    schema_version: Literal[
        "first_layer_operational_forecast_real_materialization_policy.v1"
    ]
    policy_id: Literal["first_layer_operational_forecast_real_materialization_v1"]
    policy_version: Literal["1.2.0"]
    status: Literal["OWNER_AUTHORIZED_NON_EXECUTABLE_DATA_RESEARCH"]
    owner: Literal["project_owner"]
    owner_decision_id: Literal[
        "owner_decision:TRADING-2542I:2026-08-29:"
        "authorize_real_materialization_and_conditional_qc_v1"
    ]
    authorization_state: Literal["EXACT_PREAUTHORIZED"]
    authorities: AuthorityPolicy
    inputs: InputPolicy
    dq_scopes: tuple[DQScopePolicy, ...]
    producer_execution: ProducerExecutionPolicy
    safety: SafetyPolicy
    review_condition: str

    @model_validator(mode="after")
    def validate_frozen_scopes(self) -> Self:
        observed = tuple(
            (
                scope.scope_id,
                scope.requested_start,
                scope.requested_end,
                scope.expected_price_tickers,
            )
            for scope in self.dq_scopes
        )
        if observed != _EXPECTED_SCOPE_ROWS:
            raise ValueError("segmented DQ scope contract drifted")
        if tuple(scope.require_secondary_prices for scope in self.dq_scopes) != (
            True,
            False,
            True,
        ):
            raise ValueError("segmented secondary-source requirement drifted")
        if not self.review_condition.strip():
            raise ValueError("review_condition is required")
        return self


@dataclass(frozen=True)
class LoadedRealMaterializationPolicy:
    policy: RealMaterializationPolicy
    path: Path
    file_sha256: str


@dataclass(frozen=True)
class ScopedDQRun:
    scope: DQScopePolicy
    publication: ValidatedDownloadPublication
    execution: CanonicalDataQualityExecutionResult
    lineage_path: Path


@dataclass(frozen=True)
class RealMaterializationResult:
    output_root: Path
    forecast: OperationalForecastResult
    dq_runs: tuple[ScopedDQRun, ...]
    source_audit: SignalSourceAudit
    package_root: Path
    replay: LoadedQQQOptionsSignalPackage
    receipt_path: Path
    replay_receipt_path: Path


def load_real_materialization_policy(
    path: Path = DEFAULT_REAL_MATERIALIZATION_POLICY_PATH,
    *,
    project_root: Path = PROJECT_ROOT,
) -> LoadedRealMaterializationPolicy:
    resolved = _bound_file(path, project_root)
    try:
        content = resolved.read_bytes()
        payload = safe_load_yaml_path(resolved)
        if not isinstance(payload, dict):
            raise TypeError("policy root must be a mapping")
        policy = RealMaterializationPolicy.model_validate(payload, strict=False)
        for binding in (
            policy.authorities.operational_forecast_policy,
            policy.authorities.data_quality_policy,
            policy.authorities.xnys_special_closure_policy,
            policy.authorities.exact_signal_admission_policy,
            policy.authorities.signal_export_policy,
            policy.authorities.project_adapter_policy,
        ):
            if sha256_path(_bound_file(Path(binding.path), project_root)) != binding.sha256:
                raise ValueError(f"authority drifted: {binding.path}")
    except (OSError, TypeError, ValueError) as exc:
        raise RealMaterializationError("REAL_MATERIALIZATION_POLICY_INVALID", str(exc)) from exc
    return LoadedRealMaterializationPolicy(
        policy=policy,
        path=resolved,
        file_sha256=hashlib.sha256(content).hexdigest(),
    )


def run_real_operational_forecast_materialization(
    *,
    loaded_policy: LoadedRealMaterializationPolicy,
    repository_code_sha: str,
    output_root: Path | None = None,
    project_root: Path = PROJECT_ROOT,
) -> RealMaterializationResult:
    if len(repository_code_sha) not in {40, 64} or any(
        character not in _SHA256_CHARS for character in repository_code_sha
    ):
        raise RealMaterializationError(
            "REAL_MATERIALIZATION_CODE_IDENTITY_INVALID", repository_code_sha
        )
    root = project_root.resolve()
    policy = loaded_policy.policy
    configured_output = _bound_output(Path(policy.producer_execution.output_root), root)
    target = configured_output if output_root is None else _bound_output(output_root, root)
    if target != configured_output:
        raise RealMaterializationError(
            "REAL_MATERIALIZATION_OUTPUT_POLICY_MISMATCH", str(target)
        )
    package_parent = _bound_output(Path(policy.producer_execution.package_output_root), root)
    package_target = package_parent / policy.producer_execution.package_run_id
    if target.exists() or package_target.exists():
        raise RealMaterializationError(
            "REAL_MATERIALIZATION_OUTPUT_EXISTS", f"{target}; {package_target}"
        )
    target.mkdir(parents=True, exist_ok=False)

    dq_runs = tuple(
        _run_scoped_dq(
            scope=scope,
            policy=policy,
            output_root=target / "data_quality" / scope.scope_id,
            project_root=root,
        )
        for scope in policy.dq_scopes
    )
    dq_by_id = {run.scope.scope_id: run for run in dq_runs}
    if any(run.execution.report.status != "PASS" for run in dq_runs):
        raise RealMaterializationError("REAL_MATERIALIZATION_DQ_NOT_PASS", "segmented DQ")

    prices, rates = _build_model_inputs(policy=policy, dq_by_id=dq_by_id)
    dq_identity_payload = {
        "schema_version": "first_layer_operational_forecast_segmented_dq_identity.v1",
        "policy_file_sha256": loaded_policy.file_sha256,
        "repository_code_sha": repository_code_sha,
        "scopes": [
            {
                "scope_id": run.scope.scope_id,
                "receipt": _artifact_binding(run.execution.receipt_path, root),
                "report": _artifact_binding(run.execution.report_path, root),
                "lineage": _artifact_binding(run.lineage_path, root),
            }
            for run in dq_runs
        ],
    }
    dq_identity_sha256 = _canonical_sha256(dq_identity_payload)
    write_bytes_atomic(
        target / "segmented_dq_identity.json",
        _canonical_json_bytes({**dq_identity_payload, "content_sha256": dq_identity_sha256}),
    )

    operational_policy = load_operational_forecast_policy(
        Path(policy.authorities.operational_forecast_policy.path), project_root=root
    )
    forecast = build_operational_forecast_series(
        loaded_policy=operational_policy,
        prices=prices,
        rates=rates,
        data_quality_status="PASS",
        data_quality_identity_sha256=dq_identity_sha256,
    )
    predictions_path, fit_audit_path, operational_receipt_path = (
        write_operational_forecast_artifacts(forecast, output_root=target / "producer")
    )

    admission_policy = load_exact_signal_package_admission_policy(
        Path(policy.authorities.exact_signal_admission_policy.path), project_root=root
    )
    source_audit = audit_first_layer_signal_source(
        loaded_policy=admission_policy,
        source_path=predictions_path,
        project_root=root,
    )
    if source_audit.admission_status != "PASS":
        raise RealMaterializationError(
            "REAL_MATERIALIZATION_SOURCE_REJECTED", ",".join(source_audit.blocker_codes)
        )
    evaluation_dq = dq_by_id["primary_evaluation"].execution
    admission_receipt = build_exact_signal_package_admission_receipt(
        loaded_policy=admission_policy,
        data_quality=evaluation_dq,
        source_audit=source_audit,
    )
    admission_root = target / "source_admission"
    admission_root.mkdir(parents=True, exist_ok=False)
    admission_json_path, admission_report_path = write_exact_signal_package_admission_receipt(
        admission_receipt,
        output_root=admission_root,
    )
    if admission_receipt.package_generation_status != "READY_FOR_EXISTING_2483_WRITER":
        raise RealMaterializationError(
            "REAL_MATERIALIZATION_PACKAGE_GATE_NOT_READY",
            admission_receipt.package_generation_status,
        )

    normalized_signals = _normalized_signals(
        forecast.predictions,
        mapping=policy.producer_execution.exact_signal_mapping,
    )
    normalized_bytes = canonical_normalized_signal_source_bytes(normalized_signals)
    normalized_path = target / "normalized_signal_source.json"
    write_bytes_atomic(normalized_path, normalized_bytes)
    created_at = datetime.now(UTC)
    package = build_qqq_options_signal_package(
        run_id=policy.producer_execution.package_run_id,
        signals=normalized_signals,
        source_artifact=SignalSourceArtifact(
            artifact_id="first-layer-operational-forecast-real-v3",
            locator=normalized_path.relative_to(root).as_posix(),
            sha256=hashlib.sha256(normalized_bytes).hexdigest(),
            byte_count=len(normalized_bytes),
            export_classification="EXPORT_ALLOWED_DERIVED",
        ),
        source_artifact_bytes=normalized_bytes,
        data_quality_receipt_path=evaluation_dq.receipt_path,
        expected_data_quality_as_of=policy.producer_execution.evaluation_end,
        expected_data_quality_policy_path=Path(policy.authorities.data_quality_policy.path),
        expected_data_quality_input_roles=_DQ_INPUT_ROLES,
        requested_start=policy.producer_execution.evaluation_start,
        requested_end=policy.producer_execution.evaluation_end,
        evaluated_start=policy.producer_execution.evaluation_start,
        evaluated_end=policy.producer_execution.evaluation_end,
        initial_cash_usd=policy.producer_execution.initial_cash_usd,
        created_at_utc=created_at,
        producer_version=operational_policy.policy.policy_id,
        repository_code_sha=repository_code_sha,
        lineage_id=policy.producer_execution.package_lineage_id,
        policy_path=Path(policy.authorities.signal_export_policy.path),
        project_root=root,
    )
    package_root = write_qqq_options_signal_package(package, output_root=package_parent)
    replay = load_qqq_options_signal_package_for_qc(
        package_root,
        adapter_policy_path=Path(policy.authorities.project_adapter_policy.path),
        signal_policy_path=Path(policy.authorities.signal_export_policy.path),
        project_root=root,
    )
    replay_payload = {
        "schema_version": "trading_2483_signal_package_manifest_replay_receipt.v1",
        "status": "PASS",
        "run_id": package.run_manifest.run_id,
        "repository_code_sha": repository_code_sha,
        "daily_signal_count": len(replay.package.daily_signals),
        "requested_start": policy.producer_execution.evaluation_start.isoformat(),
        "requested_end": policy.producer_execution.evaluation_end.isoformat(),
        "evaluated_start": policy.producer_execution.evaluation_start.isoformat(),
        "evaluated_end": policy.producer_execution.evaluation_end.isoformat(),
        "package_receipt_sha256": replay.file_sha256s["package_receipt.json"],
        "signal_index_sha256": replay.file_sha256s["signal_index.json"],
        "run_manifest_sha256": replay.file_sha256s["run_manifest.json"],
        "canonical_reconstruction_match": True,
        "quantconnect_dispatch_allowed_by_this_receipt": False,
        "production_effect": "none",
        "broker_action": "none",
    }
    replay_receipt_path = target / "manifest_replay_receipt.json"
    write_bytes_atomic(replay_receipt_path, _canonical_json_bytes(replay_payload))

    receipt_payload = {
        "schema_version": "first_layer_operational_forecast_real_materialization_receipt.v1",
        "status": "PASS",
        "authorization_state": policy.authorization_state,
        "owner_decision_id": policy.owner_decision_id,
        "policy_id": policy.policy_id,
        "policy_version": policy.policy_version,
        "policy_file_sha256": loaded_policy.file_sha256,
        "repository_code_sha": repository_code_sha,
        "segmented_dq_identity_sha256": dq_identity_sha256,
        "dq_scope_statuses": {
            run.scope.scope_id: run.execution.report.status for run in dq_runs
        },
        "operational_predictions": _artifact_binding(predictions_path, root),
        "operational_fit_audit": _artifact_binding(fit_audit_path, root),
        "operational_receipt": _artifact_binding(operational_receipt_path, root),
        "exact_source_admission_receipt": _artifact_binding(admission_json_path, root),
        "exact_source_admission_report": _artifact_binding(admission_report_path, root),
        "normalized_signal_source": _artifact_binding(normalized_path, root),
        "manifest_replay_receipt": _artifact_binding(replay_receipt_path, root),
        "prediction_row_count": len(forecast.predictions),
        "unique_session_count": int(forecast.predictions["date"].nunique()),
        "evaluation_proxy_row_count": int(
            forecast.predictions["cash_reference_source"].ne("SGOV").sum()
        ),
        "source_admission_status": source_audit.admission_status,
        "package_manifest_replay_status": "PASS",
        "quantconnect_status": "AUTHORIZED_LATER_WAVE_NOT_RUN",
        "orders_outside_qc_simulation": 0,
        "fills_outside_qc_simulation": 0,
        "positions_outside_qc_simulation": 0,
        "production_effect": "none",
        "broker_action": "none",
    }
    receipt_path = target / "real_materialization_receipt.json"
    write_bytes_atomic(receipt_path, _canonical_json_bytes(receipt_payload))
    return RealMaterializationResult(
        output_root=target,
        forecast=forecast,
        dq_runs=dq_runs,
        source_audit=source_audit,
        package_root=package_root,
        replay=replay,
        receipt_path=receipt_path,
        replay_receipt_path=replay_receipt_path,
    )


def _run_scoped_dq(
    *,
    scope: DQScopePolicy,
    policy: RealMaterializationPolicy,
    output_root: Path,
    project_root: Path,
) -> ScopedDQRun:
    output_root.mkdir(parents=True, exist_ok=False)
    source_specs: list[tuple[str, str, str, str, tuple[str, ...]]] = [
        (
            "prices",
            policy.inputs.prices_path,
            "prices_daily.csv",
            "ticker",
            scope.expected_price_tickers,
        ),
        (
            "rates",
            policy.inputs.rates_path,
            "rates_daily.csv",
            "series",
            scope.expected_rate_series,
        ),
    ]
    if scope.require_secondary_prices:
        source_specs.append(
            (
                "secondary_prices",
                policy.inputs.secondary_prices_path,
                "prices_marketstack_daily.csv",
                "ticker",
                scope.expected_price_tickers,
            )
        )
    artifacts: list[DownloadArtifactCandidate] = []
    bindings: list[DownloadSourceBinding] = []
    lineage_inputs: list[dict[str, Any]] = []
    for role, relative, filename, identity_column, identities in source_specs:
        source_path = _bound_file(Path(relative), project_root)
        source_bytes = source_path.read_bytes()
        source_frame = pd.read_csv(source_path)
        projected = _project_scope(
            source_frame,
            identity_column=identity_column,
            identities=identities,
            start=scope.requested_start,
            end=scope.requested_end,
            role=role,
        )
        content = projected.to_csv(index=False, lineterminator="\n").encode("utf-8")
        event_id = f"trading-2542i-{scope.scope_id}-{role}-local-cache"
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
                source_kind=policy.inputs.source_kind,
                source_id=f"operational_forecast_{scope.scope_id}_local_cache_projection",
                provider="AITradingSystem local filesystem",
                endpoint=relative,
                request_parameters={
                    "scope_id": scope.scope_id,
                    "requested_start": scope.requested_start.isoformat(),
                    "requested_end": scope.requested_end.isoformat(),
                    "identity_scope": list(identities),
                    "projection_rule": "EXACT_DATE_AND_IDENTITY_FILTER_ONLY",
                    "origin_status": policy.inputs.origin_status,
                    "raw_provider_provenance": False,
                    "provider_query_performed": False,
                    "upstream_path": relative,
                    "upstream_sha256": hashlib.sha256(source_bytes).hexdigest(),
                    "upstream_row_count": len(source_frame),
                    "cache_relative_path": filename,
                    "cache_sha256": hashlib.sha256(content).hexdigest(),
                    "cache_size_bytes": len(content),
                    "cache_row_count": len(projected),
                    "manifest_relative_path": "download_manifest.csv",
                    "manifest_sha256": None,
                    "manifest_size_bytes": None,
                    "manifest_row_count": None,
                    "manifest_binding_status": "MISSING",
                    "origin_lineage_complete": False,
                    "data_quality_provenance": False,
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
                "upstream_path": relative,
                "upstream_sha256": hashlib.sha256(source_bytes).hexdigest(),
                "upstream_size_bytes": len(source_bytes),
                "upstream_row_count": len(source_frame),
                "projected_sha256": hashlib.sha256(content).hexdigest(),
                "projected_size_bytes": len(content),
                "projected_row_count": len(projected),
                "identity_scope": list(identities),
            }
        )
    publication = publish_download_transaction(
        output_dir=output_root / "canonical_download",
        requested_start=scope.requested_start,
        requested_end=scope.requested_end,
        artifacts=tuple(artifacts),
        source_bindings=tuple(bindings),
        published_at=datetime.now(UTC),
    )
    lineage_payload = {
        "schema_version": "first_layer_operational_forecast_scoped_cache_lineage.v1",
        "scope_id": scope.scope_id,
        "requested_start": scope.requested_start.isoformat(),
        "requested_end": scope.requested_end.isoformat(),
        "source_kind": policy.inputs.source_kind,
        "origin_status": policy.inputs.origin_status,
        "provider_query_performed": False,
        "inputs": lineage_inputs,
        "publication_transaction_id": publication.transaction_id,
        "publication_transaction_sha256": publication.transaction_manifest_sha256,
        "production_effect": "none",
        "broker_action": "none",
    }
    lineage_path = output_root / "cache_lineage.json"
    write_bytes_atomic(lineage_path, _canonical_json_bytes(lineage_payload))
    window = DataQualityDateWindow(start=scope.requested_start, end=scope.requested_end)
    execution = run_canonical_data_quality_execution(
        CanonicalDataQualityExecutionRequest(
            as_of=scope.requested_end,
            requested_window=window,
            evaluated_window=window,
            prices_path=publication.legacy_prices_path,
            rates_path=publication.legacy_rates_path,
            manifest_path=publication.legacy_manifest_path,
            secondary_prices_path=publication.legacy_secondary_prices_path,
            require_secondary_prices=scope.require_secondary_prices,
            expected_price_tickers=scope.expected_price_tickers,
            expected_rate_series=scope.expected_rate_series,
            policy_path=Path(policy.authorities.data_quality_policy.path),
        ),
        project_root=project_root,
    )
    if execution.report.status != "PASS":
        raise RealMaterializationError(
            "REAL_MATERIALIZATION_DQ_SCOPE_FAILED",
            f"scope={scope.scope_id}; receipt={execution.receipt_path}",
        )
    verify_data_quality_execution_receipt(
        execution.receipt_path,
        expected_as_of=scope.requested_end,
        expected_policy_path=Path(policy.authorities.data_quality_policy.path),
        expected_input_roles=(
            _DQ_INPUT_ROLES
            if scope.require_secondary_prices
            else ("prices", "rates")
        ),
        project_root=project_root,
    )
    return ScopedDQRun(
        scope=scope,
        publication=publication,
        execution=execution,
        lineage_path=lineage_path,
    )


def _project_scope(
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
        raise RealMaterializationError(
            "REAL_MATERIALIZATION_SOURCE_SCHEMA_INVALID",
            f"{role}: {sorted(required - set(frame.columns))}",
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
        raise RealMaterializationError("REAL_MATERIALIZATION_SOURCE_EMPTY", role)
    if projected.duplicated([identity_column, "date"]).any():
        raise RealMaterializationError("REAL_MATERIALIZATION_SOURCE_DUPLICATE_KEY", role)
    if set(projected[identity_column].astype(str)) != set(identities):
        raise RealMaterializationError("REAL_MATERIALIZATION_SOURCE_SCOPE_INCOMPLETE", role)
    return projected


def _build_model_inputs(
    *,
    policy: RealMaterializationPolicy,
    dq_by_id: dict[str, ScopedDQRun],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    training_prices = pd.read_csv(
        dq_by_id["training_proxy_history"].publication.legacy_prices_path
    )
    sgov_prices = pd.read_csv(dq_by_id["exact_sgov_history"].publication.legacy_prices_path)
    combined = pd.concat((training_prices, sgov_prices), ignore_index=True)
    value_column = policy.inputs.price_value_column
    required_price_columns = {"date", "ticker", value_column}
    if not required_price_columns.issubset(combined.columns):
        raise RealMaterializationError(
            "REAL_MATERIALIZATION_PRICE_SCHEMA_INVALID",
            str(sorted(required_price_columns - set(combined.columns))),
        )
    if combined.duplicated(["ticker", "date"]).any():
        raise RealMaterializationError(
            "REAL_MATERIALIZATION_PRICE_DUPLICATE_KEY", "ticker/date"
        )
    prices = combined.pivot(index="date", columns="ticker", values=value_column)
    prices.index = pd.to_datetime(prices.index)
    prices = prices.sort_index()

    rate_rows = pd.read_csv(
        dq_by_id["training_proxy_history"].publication.legacy_rates_path
    )
    value = policy.inputs.rate_value_column
    required_rate_columns = {"date", "series", value}
    if not required_rate_columns.issubset(rate_rows.columns):
        raise RealMaterializationError(
            "REAL_MATERIALIZATION_RATE_SCHEMA_INVALID",
            str(sorted(required_rate_columns - set(rate_rows.columns))),
        )
    if rate_rows.duplicated(["series", "date"]).any():
        raise RealMaterializationError("REAL_MATERIALIZATION_RATE_DUPLICATE_KEY", "series/date")
    rates = rate_rows.pivot(index="date", columns="series", values=value)
    rates.index = pd.to_datetime(rates.index)
    return prices.sort_index(), rates.sort_index()


def _normalized_signals(
    predictions: pd.DataFrame,
    *,
    mapping: dict[str, Literal["LONG_CALL", "FLAT"]],
) -> tuple[NormalizedDailySignalInput, ...]:
    required = {"date", "trend_state"}
    if not required.issubset(predictions.columns):
        raise RealMaterializationError(
            "REAL_MATERIALIZATION_PREDICTION_SCHEMA_INVALID",
            str(sorted(required - set(predictions.columns))),
        )
    signals: list[NormalizedDailySignalInput] = []
    for day_text, state_text in predictions[["date", "trend_state"]].itertuples(
        index=False, name=None
    ):
        session = date.fromisoformat(str(day_text))
        state = str(state_text)
        if state not in mapping:
            raise RealMaterializationError("REAL_MATERIALIZATION_STATE_INVALID", state)
        market_session = us_equity_market_session(session)
        if market_session.close_time is None:
            raise RealMaterializationError(
                "REAL_MATERIALIZATION_SESSION_CLOSE_UNKNOWN", session.isoformat()
            )
        close_local = datetime.combine(
            session, market_session.close_time, tzinfo=US_EQUITY_MARKET_TIMEZONE
        )
        cutoff = close_local.astimezone(UTC)
        signals.append(
            NormalizedDailySignalInput(
                signal_session=session,
                source_data_cutoff_utc=cutoff,
                generated_at_utc=cutoff + timedelta(minutes=1),
                signal=mapping[state],
            )
        )
    return tuple(signals)


def _artifact_binding(path: Path, root: Path) -> dict[str, Any]:
    content = path.read_bytes()
    return {
        "path": path.resolve().relative_to(root.resolve()).as_posix(),
        "sha256": hashlib.sha256(content).hexdigest(),
        "size_bytes": len(content),
    }


def _canonical_json_bytes(value: object) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False)
        + "\n"
    ).encode("utf-8")


def _canonical_sha256(value: object) -> str:
    return hashlib.sha256(_canonical_json_bytes(value)).hexdigest()


def _bound_file(path: Path, project_root: Path) -> Path:
    root = project_root.resolve()
    candidate = path if path.is_absolute() else root / path
    resolved = candidate.resolve(strict=True)
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise ValueError(f"path escapes project root: {path}") from exc
    if candidate.is_symlink() or not resolved.is_file():
        raise ValueError(f"path must be a non-symlink regular file: {path}")
    return resolved


def _bound_output(path: Path, project_root: Path) -> Path:
    root = project_root.resolve()
    candidate = path if path.is_absolute() else root / path
    resolved = candidate.resolve(strict=False)
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise RealMaterializationError(
            "REAL_MATERIALIZATION_OUTPUT_OUTSIDE_REPOSITORY", str(path)
        ) from exc
    if candidate.is_symlink():
        raise RealMaterializationError("REAL_MATERIALIZATION_OUTPUT_SYMLINK", str(path))
    return resolved


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run the governed first-layer operational forecast real materialization."
    )
    parser.add_argument("--repository-code-sha", required=True)
    parser.add_argument(
        "--policy", type=Path, default=DEFAULT_REAL_MATERIALIZATION_POLICY_PATH
    )
    args = parser.parse_args(argv)
    loaded = load_real_materialization_policy(args.policy)
    result = run_real_operational_forecast_materialization(
        loaded_policy=loaded,
        repository_code_sha=str(args.repository_code_sha),
    )
    print(
        json.dumps(
            {
                "status": "PASS",
                "output_root": result.output_root.relative_to(PROJECT_ROOT).as_posix(),
                "prediction_rows": len(result.forecast.predictions),
                "source_admission": result.source_audit.admission_status,
                "manifest_replay": "PASS",
                "package_root": result.package_root.relative_to(PROJECT_ROOT).as_posix(),
                "receipt": result.receipt_path.relative_to(PROJECT_ROOT).as_posix(),
                "quantconnect": "AUTHORIZED_LATER_WAVE_NOT_RUN",
                "production_effect": "none",
                "broker_action": "none",
            },
            ensure_ascii=False,
            sort_keys=True,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "DEFAULT_REAL_MATERIALIZATION_POLICY_PATH",
    "DQScopePolicy",
    "LoadedRealMaterializationPolicy",
    "RealMaterializationError",
    "RealMaterializationPolicy",
    "RealMaterializationResult",
    "ScopedDQRun",
    "load_real_materialization_policy",
    "run_real_operational_forecast_materialization",
]
