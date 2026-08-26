from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.qqq_options_research import (
    growth_action_value_mandatory_veto_real_source_adapter_contract_freeze_admission as s7,
)
from ai_trading_system.yaml_loader import load_strict_yaml_text

DEFAULT_MANDATORY_VETO_PIT_RECEIPT_ADAPTER_CONTRACT_PATH = Path(
    "config/research/"
    "qc_qqq_options_growth_action_value_mandatory_veto_"
    "pit_receipt_adapter_contract_v1.yaml"
)

_S7_PATH = (
    "config/research/"
    "qc_qqq_options_growth_action_value_mandatory_veto_"
    "real_source_adapter_contract_freeze_admission_v1.yaml"
)
_S7_FILE_SHA256 = "d4e431350c0220934d48482e1cfd02287b06f291f8903f58901d75735d8b1636"
_S7_CANONICAL_SHA256 = (
    "3344d14fd7b94b6951a8f676e77674c50b1dbe38820f83b6c45f96d4727a8405"
)
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_VETO_IDS = (
    "broad_market_risk_off_veto",
    "realized_volatility_veto",
    "scheduled_event_risk_veto",
    "underlying_trend_break_veto",
)
_IMPLEMENTED_CANDIDATE_IDS = (
    "fmp_spy_qqq_eod_adjusted_v1",
    "cboe_vix_index_daily_v1",
    "federal_reserve_fomc_schedule_capture_v1",
    "bls_release_schedule_capture_v1",
    "bea_release_schedule_capture_v1",
)
_ADAPTER_SURFACE = (
    (
        "FmpPricePITReceiptAdapter",
        "PRICE",
        "fmp_spy_qqq_eod_adjusted_v1",
        "FmpPriceProvider",
        "Financial Modeling Prep",
        "fmp_eod_daily_prices",
        "https://financialmodelingprep.com/stable/historical-price-eod/"
        "non-split-adjusted; https://financialmodelingprep.com/stable/"
        "historical-price-eod/dividend-adjusted",
        "America/New_York",
        ("SPY", "QQQ"),
        (),
        ("symbol", "from", "to", "interval", "raw_price_mode", "adjusted_price_mode"),
        (
            ("interval", "daily"),
            ("raw_price_mode", "non-split-adjusted"),
            ("adjusted_price_mode", "dividend-adjusted"),
        ),
        (
            "schema_version",
            "candidate_id",
            "provider",
            "source_id",
            "endpoint",
            "request_parameters",
            "ticker",
            "provider_symbol_alias",
            "adjustment_basis",
            "adjustment_vintage",
            "session_timezone",
            "available_at",
            "downloaded_at",
            "row_count",
            "checksum",
        ),
        (
            "session",
            "ticker",
            "provider_symbol_alias",
            "raw_close",
            "dividend_adjusted_close",
            "available_at",
        ),
    ),
    (
        "CboeVixPITReceiptAdapter",
        "VIX",
        "cboe_vix_index_daily_v1",
        "CboeVixPriceProvider",
        "Cboe Global Markets",
        "cboe_vix_daily_prices",
        "https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv",
        "America/Chicago",
        ("VIX",),
        (),
        ("ticker", "from", "to", "interval", "content"),
        (("interval", "daily"), ("content", "full_history_csv")),
        (
            "schema_version",
            "candidate_id",
            "provider",
            "source_id",
            "endpoint",
            "request_parameters",
            "ticker",
            "adjustment_basis",
            "session_timezone",
            "level_definition",
            "revision_policy",
            "available_at",
            "downloaded_at",
            "row_count",
            "checksum",
        ),
        ("session", "ticker", "close", "adjusted_close", "available_at"),
    ),
    (
        "FederalReserveFomcSchedulePITReceiptAdapter",
        "EVENT",
        "federal_reserve_fomc_schedule_capture_v1",
        "FederalReserveFomcScheduleAdapterPlanned",
        "Federal Reserve Board",
        "federal_reserve_fomc_calendar",
        "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm",
        "America/New_York",
        (),
        ("FOMC_RATE_DECISION",),
        ("event_types", "capture_mode", "coverage_start", "coverage_end"),
        (("capture_mode", "official_schedule"),),
        (
            "schema_version",
            "candidate_id",
            "authority",
            "source_id",
            "endpoint",
            "request_parameters",
            "session_timezone",
            "captured_at",
            "available_at",
            "coverage_through",
            "row_count",
            "checksum",
        ),
        (
            "stable_event_key",
            "event_type",
            "revision_id",
            "revision_action",
            "scheduled_for",
            "source_published_at",
            "captured_at",
            "available_at",
        ),
    ),
    (
        "BlsReleaseSchedulePITReceiptAdapter",
        "EVENT",
        "bls_release_schedule_capture_v1",
        "BlsReleaseScheduleAdapterPlanned",
        "Bureau of Labor Statistics",
        "bls_release_calendar",
        "https://www.bls.gov/schedule/news_release/",
        "America/New_York",
        (),
        ("CPI", "NONFARM_PAYROLLS"),
        ("event_types", "capture_mode", "coverage_start", "coverage_end"),
        (("capture_mode", "official_schedule"),),
        (
            "schema_version",
            "candidate_id",
            "authority",
            "source_id",
            "endpoint",
            "request_parameters",
            "session_timezone",
            "captured_at",
            "available_at",
            "coverage_through",
            "row_count",
            "checksum",
        ),
        (
            "stable_event_key",
            "event_type",
            "revision_id",
            "revision_action",
            "scheduled_for",
            "source_published_at",
            "captured_at",
            "available_at",
        ),
    ),
    (
        "BeaReleaseSchedulePITReceiptAdapter",
        "EVENT",
        "bea_release_schedule_capture_v1",
        "BeaReleaseScheduleAdapterPlanned",
        "Bureau of Economic Analysis",
        "bea_release_metadata",
        "https://apps.bea.gov/api/data",
        "America/New_York",
        (),
        ("PCE_PRICE_INDEX", "GDP_ADVANCE_ESTIMATE"),
        ("event_types", "capture_mode", "coverage_start", "coverage_end"),
        (("capture_mode", "official_schedule"),),
        (
            "schema_version",
            "candidate_id",
            "authority",
            "source_id",
            "endpoint",
            "request_parameters",
            "session_timezone",
            "captured_at",
            "available_at",
            "coverage_through",
            "row_count",
            "checksum",
        ),
        (
            "stable_event_key",
            "event_type",
            "revision_id",
            "revision_action",
            "scheduled_for",
            "source_published_at",
            "captured_at",
            "available_at",
        ),
    ),
)
_VETO_BINDING_SURFACE = (
    (
        "broad_market_risk_off_veto",
        "qqq_options_growth_action_value_broad_market_risk_off_v1",
        "evaluate_broad_market_risk_off",
        ("FmpPricePITReceiptAdapter",),
        ("fmp_spy_qqq_eod_adjusted_v1",),
        (),
    ),
    (
        "realized_volatility_veto",
        "volatility_compression_free_v1_successor_adapter",
        "evaluate_realized_volatility_veto",
        ("FmpPricePITReceiptAdapter", "CboeVixPITReceiptAdapter"),
        ("fmp_spy_qqq_eod_adjusted_v1", "cboe_vix_index_daily_v1"),
        (),
    ),
    (
        "scheduled_event_risk_veto",
        "official_macro_release_calendar_pit_v1",
        "evaluate_scheduled_event_risk",
        (
            "FederalReserveFomcSchedulePITReceiptAdapter",
            "BlsReleaseSchedulePITReceiptAdapter",
            "BeaReleaseSchedulePITReceiptAdapter",
        ),
        (
            "federal_reserve_fomc_schedule_capture_v1",
            "bls_release_schedule_capture_v1",
            "bea_release_schedule_capture_v1",
        ),
        (),
    ),
    (
        "underlying_trend_break_veto",
        "qqq_underlying_trend_break_v1",
        "evaluate_underlying_trend_break",
        ("FmpPricePITReceiptAdapter",),
        ("fmp_spy_qqq_eod_adjusted_v1",),
        (
            "replay_start",
            "initial_checkpoint_sha256",
            "target_start_checkpoint_sha256",
            "state_transition_lineage_sha256",
        ),
    ),
)


class MandatoryVetoPITReceiptAdapterContractError(ValueError):
    def __init__(self, reason_code: str, detail: str) -> None:
        super().__init__(f"{reason_code}: {detail}")
        self.reason_code = reason_code
        self.detail = detail


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


def _canonical_json_bytes(value: object) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False)
        + "\n"
    ).encode("utf-8")


class _CanonicalModel(_StrictModel):
    @property
    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.model_dump(mode="json"))

    @property
    def canonical_sha256(self) -> str:
        return hashlib.sha256(self.canonical_bytes).hexdigest()


def _bound_file(path: Path, *, root: Path, field: str) -> Path:
    resolved_root = root.resolve()
    candidate = path if path.is_absolute() else resolved_root / path
    try:
        relative = candidate.relative_to(resolved_root)
    except ValueError as exc:
        raise ValueError(f"{field} escapes project root") from exc
    if ".." in relative.parts:
        raise ValueError(f"{field} escapes project root")
    current = resolved_root
    for part in relative.parts:
        current /= part
        if current.exists() and current.is_symlink():
            raise ValueError(f"{field} cannot traverse a symlink")
    resolved = candidate.resolve(strict=True)
    try:
        resolved.relative_to(resolved_root)
    except ValueError as exc:
        raise ValueError(f"{field} escapes project root") from exc
    if not resolved.is_file() or candidate.is_symlink():
        raise ValueError(f"{field} must be a non-symlink regular file")
    return resolved


class S7ContractBinding(_StrictModel):
    path: Literal[
        "config/research/"
        "qc_qqq_options_growth_action_value_mandatory_veto_"
        "real_source_adapter_contract_freeze_admission_v1.yaml"
    ]
    artifact_id: Literal[
        "qc_qqq_options_growth_action_value_mandatory_veto_"
        "real_source_adapter_contract_freeze_admission_v1"
    ]
    file_sha256: str
    canonical_sha256: str
    role: Literal["OWNER_EXACT_FROZEN_ADAPTER_MANIFEST_INVENTORY_CONTRACT"]
    immutable: Literal[True]

    @field_validator("file_sha256", "canonical_sha256")
    @classmethod
    def validate_sha(cls, value: str) -> str:
        if not _SHA256.fullmatch(value):
            raise ValueError("invalid lowercase SHA-256")
        return value

    @model_validator(mode="after")
    def validate_identity(self) -> Self:
        if (self.path, self.file_sha256, self.canonical_sha256) != (
            _S7_PATH,
            _S7_FILE_SHA256,
            _S7_CANONICAL_SHA256,
        ):
            raise ValueError("S7 exact authority identity drifted")
        return self


class ImplementationScope(_StrictModel):
    instruction_ref: Literal[
        "owner_instruction:TRADING-2542G:S8:2026-08-26:"
        "continue_non_executable_pit_receipt_adapter_implementation_v1"
    ]
    predecessor_followup_authorization_replayed: Literal[True]
    caller_injected_mapping_and_rows_only: Literal[True]
    contract_loader_may_read_tracked_authority_files: Literal[True]
    adapter_network_provider_cache_or_market_file_io_allowed: Literal[False]
    checksum_algorithm: Literal["SHA256_CANONICAL_JSON_SORTED_KEYS_UTF8_LF_V1"]
    real_payload_execution_observed: Literal[False]
    authorization_state: Literal["STANDING_OWNER_SCOPE"]


class AdapterSpec(_StrictModel):
    adapter_id: str
    adapter_kind: Literal["PRICE", "VIX", "EVENT"]
    candidate_id: str
    predecessor_adapter_id: str
    provider_or_authority: str
    source_id: str
    endpoint: str
    timestamp_timezone: str
    allowed_tickers: tuple[str, ...]
    event_taxonomy: tuple[str, ...]
    request_parameter_keys: tuple[str, ...]
    fixed_request_parameters: dict[str, str]
    receipt_field_names: tuple[str, ...]
    row_field_names: tuple[str, ...]
    implementation_state: Literal["PURE_IN_MEMORY_SYNTHETIC_CONFORMANCE_IMPLEMENTED"]
    real_payload_execution_observed: Literal[False]
    adapter_implementation_admitted: Literal[False]


class VetoBinding(_StrictModel):
    veto_id: str
    producer_id: str
    callable_name: str
    receipt_adapter_ids: tuple[str, ...]
    primary_candidate_ids: tuple[str, ...]
    supplemental_receipt_fields: tuple[str, ...]
    synthetic_adapter_conformance_ready: Literal[True]
    adapter_implementation_admitted: Literal[False]
    real_source_identity_admitted: Literal[False]
    exact_1202_session_inventory_admitted: Literal[False]
    observed_inventory_lf_sha256: None
    observed_manifest_sha256: None


class AggregateState(_StrictModel):
    synthetic_adapter_conformance_ready_vetoes: tuple[str, ...]
    admitted_adapter_implementations: tuple[()]
    admitted_real_source_identities: tuple[()]
    admitted_exact_1202_session_inventories: tuple[()]
    observed_manifest_replays: tuple[()]
    terminal: Literal[
        "SYNTHETIC_PIT_RECEIPT_ADAPTER_CONFORMANCE_READY_4_OF_4_"
        "REAL_SOURCE_UNADMITTED_0_OF_4"
    ]
    next_legal_action: Literal[
        "OWNER_AUTHORIZED_EXACT_MANIFEST_REPLAY_AND_SEPARATE_SOURCE_INVENTORY_ADMISSION"
    ]


class AdapterSafety(_StrictModel):
    non_executable_data_research_only: Literal[True]
    pure_adapter_execution_on_injected_synthetic_payload_allowed: Literal[True]
    filesystem_market_data_read_allowed: Literal[False]
    network_io_allowed: Literal[False]
    provider_query_authorized: Literal[False]
    cache_read_authorized: Literal[False]
    real_data_read_authorized: Literal[False]
    real_payload_adapter_execution_authorized: Literal[False]
    real_source_admission_allowed: Literal[False]
    exact_inventory_admission_allowed: Literal[False]
    manifest_replay_allowed: Literal[False]
    veto_series_generation_allowed: Literal[False]
    r1_manifest_generation_allowed: Literal[False]
    real_dq_authorized: Literal[False]
    backtest_authorized: Literal[False]
    parameter_or_threshold_search_allowed: Literal[False]
    constant_false_fill_allowed: Literal[False]
    missing_as_clear_allowed: Literal[False]
    cross_date_fallback_allowed: Literal[False]
    orders_allowed: Literal[False]
    fills_allowed: Literal[False]
    positions_allowed: Literal[False]
    paper_allowed: Literal[False]
    live_allowed: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]


class MandatoryVetoPITReceiptAdapterContract(_CanonicalModel):
    schema_version: Literal[
        "growth_action_value_mandatory_veto_pit_receipt_adapter_contract.v1"
    ]
    policy_id: Literal[
        "qc_qqq_options_growth_action_value_mandatory_veto_"
        "pit_receipt_adapter_contract_v1"
    ]
    policy_version: Literal["1.0.0"]
    status: Literal[
        "SYNTHETIC_PIT_RECEIPT_ADAPTER_CONFORMANCE_READY_4_OF_4_"
        "REAL_SOURCE_UNADMITTED_0_OF_4"
    ]
    task_id: Literal[
        "TRADING-2542G_GROWTH_ACTION_VALUE_MANDATORY_VETO_SOURCE_CONTRACT_WAVE_V1"
    ]
    s7_contract_binding: S7ContractBinding
    implementation_scope: ImplementationScope
    adapter_specs: tuple[AdapterSpec, ...]
    veto_bindings: tuple[VetoBinding, ...]
    aggregate_state: AggregateState
    safety: AdapterSafety

    @model_validator(mode="after")
    def validate_exact_surface(self) -> Self:
        observed_adapters = tuple(
            (
                spec.adapter_id,
                spec.adapter_kind,
                spec.candidate_id,
                spec.predecessor_adapter_id,
                spec.provider_or_authority,
                spec.source_id,
                spec.endpoint,
                spec.timestamp_timezone,
                spec.allowed_tickers,
                spec.event_taxonomy,
                spec.request_parameter_keys,
                tuple(spec.fixed_request_parameters.items()),
                spec.receipt_field_names,
                spec.row_field_names,
            )
            for spec in self.adapter_specs
        )
        if observed_adapters != _ADAPTER_SURFACE:
            raise ValueError("PIT receipt adapter implementation surface drifted")
        observed_vetoes = tuple(
            (
                row.veto_id,
                row.producer_id,
                row.callable_name,
                row.receipt_adapter_ids,
                row.primary_candidate_ids,
                row.supplemental_receipt_fields,
            )
            for row in self.veto_bindings
        )
        if observed_vetoes != _VETO_BINDING_SURFACE:
            raise ValueError("PIT receipt veto binding surface drifted")
        if self.aggregate_state.synthetic_adapter_conformance_ready_vetoes != _VETO_IDS:
            raise ValueError("synthetic adapter conformance inventory drifted")
        return self

    def adapter_spec(self, adapter_id: str) -> AdapterSpec:
        for spec in self.adapter_specs:
            if spec.adapter_id == adapter_id:
                return spec
        raise KeyError(adapter_id)


@dataclass(frozen=True)
class MandatoryVetoPITReceiptAdapterContractLoadResult:
    policy: MandatoryVetoPITReceiptAdapterContract
    path: Path
    file_sha256: str
    canonical_sha256: str
    s7: s7.MandatoryVetoRealSourceAdapterContractFreezeAdmissionLoadResult
    terminal: Literal[
        "SYNTHETIC_PIT_RECEIPT_ADAPTER_CONFORMANCE_READY_4_OF_4_"
        "REAL_SOURCE_UNADMITTED_0_OF_4"
    ]


def load_mandatory_veto_pit_receipt_adapter_contract(
    *,
    path: Path = DEFAULT_MANDATORY_VETO_PIT_RECEIPT_ADAPTER_CONTRACT_PATH,
    project_root: Path = PROJECT_ROOT,
) -> MandatoryVetoPITReceiptAdapterContractLoadResult:
    try:
        resolved = _bound_file(path, root=project_root, field="pit_receipt_adapter_contract")
        raw = resolved.read_bytes()
        payload = load_strict_yaml_text(raw.decode("utf-8"), label=str(path))
        policy = MandatoryVetoPITReceiptAdapterContract.model_validate(payload)
        loaded_s7 = s7.load_mandatory_veto_real_source_adapter_contract_freeze_admission(
            project_root=project_root
        )
        binding = policy.s7_contract_binding
        if (loaded_s7.file_sha256, loaded_s7.canonical_sha256) != (
            binding.file_sha256,
            binding.canonical_sha256,
        ):
            raise ValueError("S7 loader exact identity drifted")
        bound_s7 = _bound_file(Path(binding.path), root=project_root, field="s7_binding")
        if hashlib.sha256(bound_s7.read_bytes()).hexdigest() != binding.file_sha256:
            raise ValueError("S7 bound file SHA-256 mismatch")

        predecessor_candidates = {
            candidate.candidate_id: candidate
            for candidate in loaded_s7.review.policy.source_candidates
        }
        if tuple(spec.candidate_id for spec in policy.adapter_specs) != _IMPLEMENTED_CANDIDATE_IDS:
            raise ValueError("implemented candidate order drifted")
        for spec in policy.adapter_specs:
            candidate = predecessor_candidates[spec.candidate_id]
            if (
                spec.provider_or_authority,
                spec.source_id,
                spec.predecessor_adapter_id,
                spec.endpoint,
                spec.timestamp_timezone,
            ) != (
                candidate.provider,
                candidate.source_id,
                candidate.adapter_id,
                candidate.endpoint,
                candidate.timestamp_timezone,
            ):
                raise ValueError(f"S7 candidate identity drifted for {spec.candidate_id}")
            if candidate.admitted or candidate.live_probe_performed:
                raise ValueError("S7 candidate unexpectedly carries observed admission")

        predecessor_rows = loaded_s7.policy.freeze_rows
        if tuple(
            (
                row.veto_id,
                row.producer_id,
                row.callable_name,
                row.primary_candidate_ids,
            )
            for row in predecessor_rows
        ) != tuple(
            (
                row.veto_id,
                row.producer_id,
                row.callable_name,
                row.primary_candidate_ids,
            )
            for row in policy.veto_bindings
        ):
            raise ValueError("S8 veto bindings do not replay S7 freeze rows exactly")

        adapter_by_id = {spec.adapter_id: spec for spec in policy.adapter_specs}
        review_by_veto = {row.veto_id: row for row in loaded_s7.review.policy.review_rows}
        for binding_row in policy.veto_bindings:
            covered_fields = set(binding_row.supplemental_receipt_fields)
            for adapter_id in binding_row.receipt_adapter_ids:
                adapter = adapter_by_id[adapter_id]
                covered_fields.update(adapter.receipt_field_names)
                covered_fields.update(adapter.row_field_names)
            required = set(review_by_veto[binding_row.veto_id].required_receipt_fields)
            if not required.issubset(covered_fields):
                missing = ",".join(sorted(required - covered_fields))
                raise ValueError(
                    f"S8 receipt implementation misses frozen fields for "
                    f"{binding_row.veto_id}: {missing}"
                )
    except (
        MandatoryVetoPITReceiptAdapterContractError,
        s7.MandatoryVetoRealSourceAdapterContractFreezeAdmissionError,
    ):
        raise
    except (KeyError, OSError, TypeError, UnicodeDecodeError, ValueError) as exc:
        raise MandatoryVetoPITReceiptAdapterContractError(
            "MANDATORY_VETO_PIT_RECEIPT_ADAPTER_CONTRACT_REJECTED", str(exc)
        ) from exc
    return MandatoryVetoPITReceiptAdapterContractLoadResult(
        policy=policy,
        path=resolved,
        file_sha256=hashlib.sha256(raw).hexdigest(),
        canonical_sha256=policy.canonical_sha256,
        s7=loaded_s7,
        terminal=(
            "SYNTHETIC_PIT_RECEIPT_ADAPTER_CONFORMANCE_READY_4_OF_4_"
            "REAL_SOURCE_UNADMITTED_0_OF_4"
        ),
    )


__all__ = [
    "AdapterSpec",
    "DEFAULT_MANDATORY_VETO_PIT_RECEIPT_ADAPTER_CONTRACT_PATH",
    "MandatoryVetoPITReceiptAdapterContract",
    "MandatoryVetoPITReceiptAdapterContractError",
    "MandatoryVetoPITReceiptAdapterContractLoadResult",
    "load_mandatory_veto_pit_receipt_adapter_contract",
]
