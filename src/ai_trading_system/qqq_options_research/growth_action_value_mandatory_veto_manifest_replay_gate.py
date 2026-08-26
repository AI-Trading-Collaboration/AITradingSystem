from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.platform.artifacts import write_bytes_atomic
from ai_trading_system.qqq_options_research import (
    growth_action_value_mandatory_veto_pit_receipt_adapter_contract as s8,
)
from ai_trading_system.qqq_options_research import growth_action_value_real_review as review
from ai_trading_system.qqq_options_research import (
    growth_action_value_veto_option_signal_architecture as architecture,
)
from ai_trading_system.yaml_loader import load_strict_yaml_text

DEFAULT_MANDATORY_VETO_MANIFEST_REPLAY_GATE_PATH = Path(
    "config/research/qc_qqq_options_growth_action_value_mandatory_veto_manifest_replay_gate_v1.yaml"
)

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_TERMINAL: Literal[
    "MANIFEST_REPLAY_BLOCKED_PRE_PROVIDER_QUERY_SOURCE_RECEIPT_CAPABILITY_INCOMPLETE"
] = "MANIFEST_REPLAY_BLOCKED_PRE_PROVIDER_QUERY_SOURCE_RECEIPT_CAPABILITY_INCOMPLETE"
_AUTHORITY_SURFACE = (
    (
        "growth_action_value_real_review_execution_v2",
        "config/research/qc_qqq_options_growth_action_value_real_review_execution_v2.yaml",
        "OWNER_EXACT_FROZEN_LEGACY_EXECUTION_CONTRACT",
    ),
    (
        "growth_action_value_veto_option_signal_architecture_v1",
        "config/research/"
        "qc_qqq_options_growth_action_value_veto_option_signal_architecture_v1.yaml",
        "SUCCESSOR_FOUR_VETO_AND_ACTION_GUARD_ARCHITECTURE",
    ),
    (
        "growth_action_value_mandatory_veto_pit_receipt_adapter_contract_v1",
        "config/research/"
        "qc_qqq_options_growth_action_value_mandatory_veto_"
        "pit_receipt_adapter_contract_v1.yaml",
        "SYNTHETIC_PIT_RECEIPT_ADAPTER_CONFORMANCE_AUTHORITY",
    ),
)
_LEGACY_MARKET_FIELDS = (
    "risk_off_veto",
    "volatility_veto",
    "event_risk_veto",
    "trend_break_veto",
)
_SUCCESSOR_VETO_IDS = (
    "broad_market_risk_off_veto",
    "realized_volatility_veto",
    "scheduled_event_risk_veto",
    "underlying_trend_break_veto",
)
_CAPABILITY_SURFACE = (
    (
        "fmp_spy_qqq_eod_adjusted_v1",
        "FmpPricePITReceiptAdapter",
        "Financial Modeling Prep",
        "https://financialmodelingprep.com/stable/historical-price-eod/"
        "non-split-adjusted; https://financialmodelingprep.com/stable/"
        "historical-price-eod/dividend-adjusted",
        "HISTORICAL_PER_ROW_AVAILABLE_AT_AND_ADJUSTMENT_VINTAGE",
        "FMP_HISTORICAL_ROW_AVAILABLE_AT_UNPROVEN",
    ),
    (
        "cboe_vix_index_daily_v1",
        "CboeVixPITReceiptAdapter",
        "Cboe Global Markets",
        "https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv",
        "HISTORICAL_PER_ROW_PUBLICATION_VINTAGE",
        "CBOE_VIX_HISTORICAL_PUBLICATION_VINTAGE_UNPROVEN",
    ),
    (
        "federal_reserve_fomc_schedule_capture_v1",
        "FederalReserveFomcSchedulePITReceiptAdapter",
        "Federal Reserve Board",
        "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm",
        "REPLAYABLE_SCHEDULE_REVISION_PUBLICATION_LEDGER",
        "FED_FOMC_REVISION_LEDGER_UNAVAILABLE",
    ),
    (
        "bls_release_schedule_capture_v1",
        "BlsReleaseSchedulePITReceiptAdapter",
        "Bureau of Labor Statistics",
        "https://www.bls.gov/schedule/news_release/",
        "REPLAYABLE_SCHEDULE_REVISION_PUBLICATION_LEDGER",
        "BLS_RELEASE_SCHEDULE_REVISION_LEDGER_UNAVAILABLE",
    ),
    (
        "bea_release_schedule_capture_v1",
        "BeaReleaseSchedulePITReceiptAdapter",
        "Bureau of Economic Analysis",
        "https://apps.bea.gov/api/data",
        "OFFICIAL_SCHEDULE_REVISION_PUBLICATION_AUTHORITY",
        "BEA_FROZEN_ENDPOINT_NOT_SCHEDULE_REVISION_AUTHORITY",
    ),
)


class MandatoryVetoManifestReplayGateError(ValueError):
    def __init__(self, reason_code: str, detail: str) -> None:
        super().__init__(f"{reason_code}: {detail}")
        self.reason_code = reason_code
        self.detail = detail


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


def _canonical_json_bytes(value: object) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + "\n"
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


class OwnerAuthorization(_StrictModel):
    instruction_ref: Literal[
        "owner_instruction:TRADING-2542G:S9:2026-08-26:"
        "authorize_manifest_replay_source_admission_continue_v1"
    ]
    authorization_state: Literal["STANDING_OWNER_SCOPE"]
    scope: Literal["NON_EXECUTABLE_DATA_RESEARCH_MANIFEST_REPLAY_AND_SOURCE_ADMISSION_ONLY"]
    manifest_replay_authorized: Literal[True]
    provider_query_condition: Literal["MANIFEST_REPLAY_PASS"]
    real_dq_condition: Literal["MANIFEST_REPLAY_PASS"]
    backtest_condition: Literal["MANIFEST_REPLAY_PASS"]


class AuthorityBinding(_StrictModel):
    authority_id: str
    path: str
    file_sha256: str
    canonical_sha256: str
    role: str
    immutable: Literal[True]

    @field_validator("file_sha256", "canonical_sha256")
    @classmethod
    def validate_sha(cls, value: str) -> str:
        if not _SHA256.fullmatch(value):
            raise ValueError("invalid lowercase SHA-256")
        return value


class ExactSessionInventory(_StrictModel):
    path: Literal[
        "inputs/research/qqq_options/"
        "trading_2537_exact_date_provider_catalog_attribution_correction_v2/"
        "run_scope.json"
    ]
    file_sha256: str
    exchange_calendar: Literal["XNYS"]
    requested_start: date
    requested_end: date
    expected_session_count: Literal[1202]
    first_session: date
    last_session: date
    ordered_session_ids_lf_sha256: str

    @field_validator("file_sha256", "ordered_session_ids_lf_sha256")
    @classmethod
    def validate_sha(cls, value: str) -> str:
        if not _SHA256.fullmatch(value):
            raise ValueError("invalid lowercase SHA-256")
        return value


class ReplayExecutorBinding(_StrictModel):
    path: Literal[
        "src/ai_trading_system/qqq_options_research/"
        "growth_action_value_mandatory_veto_manifest_replay_gate.py"
    ]
    file_sha256: str
    role: Literal["TRACKED_STATIC_MANIFEST_REPLAY_EXECUTOR"]
    immutable_for_report: Literal[True]

    @field_validator("file_sha256")
    @classmethod
    def validate_sha(cls, value: str) -> str:
        if not _SHA256.fullmatch(value):
            raise ValueError("invalid lowercase SHA-256")
        return value


def _validate_replay_executor(binding: ReplayExecutorBinding, *, project_root: Path) -> None:
    replay_executor = _bound_file(Path(binding.path), root=project_root, field="replay_executor")
    if hashlib.sha256(replay_executor.read_bytes()).hexdigest() != binding.file_sha256:
        raise ValueError("manifest replay executor file SHA-256 mismatch")


class SuccessorCompatibilityBridge(_StrictModel):
    legacy_execution_v2_market_veto_fields: tuple[str, ...]
    successor_mandatory_veto_ids: tuple[str, ...]
    legacy_non_market_field: Literal["tqqq_veto"]
    successor_non_market_guard: Literal["NO_LEVERAGE_ETF_ACTION_GUARD"]
    tqqq_veto_in_market_state_conjunction: Literal[False]
    compatibility_state: Literal["PASS"]


class SourceCapabilityRow(_StrictModel):
    candidate_id: str
    adapter_id: str
    provider_or_authority: str
    endpoint: str
    required_evidence: str
    capability_state: Literal["BLOCKED_PRE_QUERY"]
    reason_code: str
    provider_query_allowed: Literal[False]
    provider_query_attempt_count: Literal[0]


class SourceCapabilityGate(_StrictModel):
    required_terminal: Literal["PASS"]
    observed_terminal: Literal["BLOCKED_PRE_PROVIDER_QUERY"]
    rows: tuple[SourceCapabilityRow, ...]


class ReplayContract(_StrictModel):
    authority_identity_replay_state: Literal["PASS"]
    exact_session_inventory_replay_state: Literal["PASS"]
    successor_compatibility_replay_state: Literal["PASS"]
    source_capability_replay_state: Literal["BLOCKED"]
    manifest_replay_state: Literal["BLOCKED"]
    technical_validation_state: Literal["BLOCKED"]
    terminal: Literal[
        "MANIFEST_REPLAY_BLOCKED_PRE_PROVIDER_QUERY_SOURCE_RECEIPT_CAPABILITY_INCOMPLETE"
    ]
    next_legal_action: Literal[
        "PROVIDE_OR_REVIEW_HISTORICAL_PIT_SOURCE_RECEIPT_AUTHORITY_THEN_RERUN_MANIFEST_REPLAY"
    ]


class ActualCounters(_StrictModel):
    network_request_count: Literal[0]
    provider_query_attempt_count: Literal[0]
    cache_read_count: Literal[0]
    market_data_file_read_count: Literal[0]
    real_payload_adapter_execution_count: Literal[0]
    source_inventory_admission_count: Literal[0]
    veto_series_generation_count: Literal[0]
    real_dq_run_count: Literal[0]
    backtest_run_count: Literal[0]
    orders: Literal[0]
    fills: Literal[0]
    positions: Literal[0]
    production_effect_count: Literal[0]
    broker_action_count: Literal[0]


class Safety(_StrictModel):
    non_executable_data_research_only: Literal[True]
    static_tracked_authority_reads_allowed: Literal[True]
    exact_session_inventory_metadata_read_allowed: Literal[True]
    provider_query_allowed: Literal[False]
    network_io_allowed: Literal[False]
    cache_read_allowed: Literal[False]
    market_data_file_read_allowed: Literal[False]
    real_payload_adapter_execution_allowed: Literal[False]
    source_inventory_admission_allowed: Literal[False]
    veto_series_generation_allowed: Literal[False]
    real_dq_allowed: Literal[False]
    backtest_allowed: Literal[False]
    orders_allowed: Literal[False]
    fills_allowed: Literal[False]
    positions_allowed: Literal[False]
    paper_allowed: Literal[False]
    live_allowed: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]


class MandatoryVetoManifestReplayGate(_CanonicalModel):
    schema_version: Literal["growth_action_value_mandatory_veto_manifest_replay_gate.v1"]
    policy_id: Literal["qc_qqq_options_growth_action_value_mandatory_veto_manifest_replay_gate_v1"]
    policy_version: Literal["1.0.0"]
    status: Literal[
        "OWNER_AUTHORIZED_REPLAY_BLOCKED_PRE_PROVIDER_QUERY_SOURCE_RECEIPT_CAPABILITY_INCOMPLETE"
    ]
    task_id: Literal["TRADING-2542G_GROWTH_ACTION_VALUE_MANDATORY_VETO_SOURCE_CONTRACT_WAVE_V1"]
    owner_authorization: OwnerAuthorization
    authority_bindings: tuple[AuthorityBinding, ...]
    exact_session_inventory: ExactSessionInventory
    replay_executor: ReplayExecutorBinding
    successor_compatibility_bridge: SuccessorCompatibilityBridge
    source_capability_gate: SourceCapabilityGate
    replay_contract: ReplayContract
    actual_counters: ActualCounters
    safety: Safety

    @model_validator(mode="after")
    def validate_frozen_surface(self) -> Self:
        observed_authorities = tuple(
            (row.authority_id, row.path, row.role) for row in self.authority_bindings
        )
        if observed_authorities != _AUTHORITY_SURFACE:
            raise ValueError("manifest replay authority surface drifted")
        bridge = self.successor_compatibility_bridge
        if bridge.legacy_execution_v2_market_veto_fields != _LEGACY_MARKET_FIELDS:
            raise ValueError("legacy market veto bridge drifted")
        if bridge.successor_mandatory_veto_ids != _SUCCESSOR_VETO_IDS:
            raise ValueError("successor mandatory veto bridge drifted")
        observed_capabilities = tuple(
            (
                row.candidate_id,
                row.adapter_id,
                row.provider_or_authority,
                row.endpoint,
                row.required_evidence,
                row.reason_code,
            )
            for row in self.source_capability_gate.rows
        )
        if observed_capabilities != _CAPABILITY_SURFACE:
            raise ValueError("source capability blocker surface drifted")
        return self


class RepositoryReplayContext(_StrictModel):
    candidate_sha: str
    local_main_sha: str
    origin_main_sha: str
    branch_name: Literal["main"]
    worktree_audit_status: Literal["PASS"]

    @field_validator("candidate_sha", "local_main_sha", "origin_main_sha")
    @classmethod
    def validate_sha(cls, value: str) -> str:
        if not _SHA256.fullmatch(value):
            raise ValueError("invalid lowercase Git SHA-256")
        return value

    @model_validator(mode="after")
    def validate_identity(self) -> Self:
        if not (self.candidate_sha == self.local_main_sha == self.origin_main_sha):
            raise ValueError("candidate, local main, and origin main must be identical")
        return self


class ManifestReplayReport(_CanonicalModel):
    schema_version: Literal["growth_action_value_mandatory_veto_manifest_replay_report.v1"]
    policy_id: Literal["qc_qqq_options_growth_action_value_mandatory_veto_manifest_replay_gate_v1"]
    policy_file_sha256: str
    policy_canonical_sha256: str
    repository_context: RepositoryReplayContext
    authorization_state: Literal["STANDING_OWNER_SCOPE"]
    authority_identity_replay_state: Literal["PASS"]
    exact_session_inventory_replay_state: Literal["PASS"]
    successor_compatibility_replay_state: Literal["PASS"]
    source_capability_replay_state: Literal["BLOCKED"]
    manifest_replay_state: Literal["BLOCKED"]
    technical_validation_state: Literal["BLOCKED"]
    blocker_reason_codes: tuple[str, ...]
    actual_counters: ActualCounters
    terminal: Literal[
        "MANIFEST_REPLAY_BLOCKED_PRE_PROVIDER_QUERY_SOURCE_RECEIPT_CAPABILITY_INCOMPLETE"
    ]

    @field_validator("policy_file_sha256", "policy_canonical_sha256")
    @classmethod
    def validate_sha(cls, value: str) -> str:
        if not _SHA256.fullmatch(value):
            raise ValueError("invalid lowercase SHA-256")
        return value


@dataclass(frozen=True)
class MandatoryVetoManifestReplayGateLoadResult:
    policy: MandatoryVetoManifestReplayGate
    path: Path
    file_sha256: str
    canonical_sha256: str
    execution_v2: review.GrowthActionValueRealReviewExactFreezeLoadResult
    successor_architecture: architecture.VetoOptionSignalArchitectureLoadResult
    s8_contract: s8.MandatoryVetoPITReceiptAdapterContractLoadResult
    session_ids: tuple[str, ...]
    terminal: Literal[
        "MANIFEST_REPLAY_BLOCKED_PRE_PROVIDER_QUERY_SOURCE_RECEIPT_CAPABILITY_INCOMPLETE"
    ]


def _validate_exact_sessions(
    inventory: ExactSessionInventory, *, project_root: Path
) -> tuple[str, ...]:
    path = _bound_file(Path(inventory.path), root=project_root, field="session_inventory")
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != inventory.file_sha256:
        raise ValueError("exact session inventory file SHA-256 mismatch")
    payload = json.loads(raw.decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("exact session inventory must be an object")
    sessions_value = payload.get("session_ids")
    if not isinstance(sessions_value, list) or not all(
        isinstance(value, str) for value in sessions_value
    ):
        raise ValueError("exact session inventory session_ids must be strings")
    sessions = tuple(sessions_value)
    expected = (
        inventory.exchange_calendar,
        str(inventory.requested_start),
        str(inventory.requested_end),
        inventory.expected_session_count,
    )
    observed = (
        payload.get("exchange_calendar"),
        payload.get("requested_start"),
        payload.get("requested_end"),
        payload.get("expected_session_count"),
    )
    if observed != expected:
        raise ValueError("exact session inventory scope drifted")
    if len(sessions) != inventory.expected_session_count:
        raise ValueError("exact session inventory count drifted")
    if not sessions or sessions[0] != str(inventory.first_session):
        raise ValueError("exact session inventory first session drifted")
    if sessions[-1] != str(inventory.last_session):
        raise ValueError("exact session inventory last session drifted")
    if len(set(sessions)) != len(sessions) or tuple(sorted(sessions)) != sessions:
        raise ValueError("exact session inventory must be unique and ordered")
    observed_lf_sha = hashlib.sha256(("\n".join(sessions) + "\n").encode()).hexdigest()
    if observed_lf_sha != inventory.ordered_session_ids_lf_sha256:
        raise ValueError("exact session inventory LF SHA-256 mismatch")
    return sessions


def load_mandatory_veto_manifest_replay_gate(
    *,
    path: Path = DEFAULT_MANDATORY_VETO_MANIFEST_REPLAY_GATE_PATH,
    project_root: Path = PROJECT_ROOT,
) -> MandatoryVetoManifestReplayGateLoadResult:
    try:
        resolved = _bound_file(path, root=project_root, field="manifest_replay_gate")
        raw = resolved.read_bytes()
        payload = load_strict_yaml_text(raw.decode("utf-8"), label=str(path))
        policy = MandatoryVetoManifestReplayGate.model_validate(payload)
        loaded_execution = review.load_growth_action_value_real_review_exact_freeze(
            project_root=project_root
        )
        loaded_architecture = architecture.load_veto_option_signal_architecture(
            project_root=project_root
        )
        loaded_s8 = s8.load_mandatory_veto_pit_receipt_adapter_contract(project_root=project_root)
        actual_identities = (
            (
                loaded_execution.policy_file_sha256,
                loaded_execution.policy_canonical_sha256,
            ),
            (loaded_architecture.file_sha256, loaded_architecture.canonical_sha256),
            (loaded_s8.file_sha256, loaded_s8.canonical_sha256),
        )
        for binding, identity in zip(policy.authority_bindings, actual_identities, strict=True):
            bound = _bound_file(Path(binding.path), root=project_root, field=binding.authority_id)
            if hashlib.sha256(bound.read_bytes()).hexdigest() != binding.file_sha256:
                raise ValueError(f"{binding.authority_id} file SHA-256 mismatch")
            if (binding.file_sha256, binding.canonical_sha256) != identity:
                raise ValueError(f"{binding.authority_id} exact identity drifted")
        _validate_replay_executor(policy.replay_executor, project_root=project_root)
        observed_architecture = tuple(
            row.veto_id for row in loaded_architecture.architecture.mandatory_market_state_vetoes
        )
        if observed_architecture != _SUCCESSOR_VETO_IDS:
            raise ValueError("successor architecture veto set drifted")
        if (
            loaded_architecture.architecture.action_universe_constraints.historical_tqqq_veto_successor_role
            != policy.successor_compatibility_bridge.successor_non_market_guard
        ):
            raise ValueError("successor action guard drifted")
        session_ids = _validate_exact_sessions(
            policy.exact_session_inventory, project_root=project_root
        )
    except (
        MandatoryVetoManifestReplayGateError,
        review.GrowthActionValueRealReviewPolicyError,
        architecture.VetoOptionSignalArchitectureError,
        s8.MandatoryVetoPITReceiptAdapterContractError,
    ):
        raise
    except (KeyError, OSError, TypeError, UnicodeDecodeError, ValueError) as exc:
        raise MandatoryVetoManifestReplayGateError(
            "MANDATORY_VETO_MANIFEST_REPLAY_GATE_REJECTED", str(exc)
        ) from exc
    return MandatoryVetoManifestReplayGateLoadResult(
        policy=policy,
        path=resolved,
        file_sha256=hashlib.sha256(raw).hexdigest(),
        canonical_sha256=policy.canonical_sha256,
        execution_v2=loaded_execution,
        successor_architecture=loaded_architecture,
        s8_contract=loaded_s8,
        session_ids=session_ids,
        terminal=_TERMINAL,
    )


def run_mandatory_veto_manifest_replay(
    *,
    repository_context: RepositoryReplayContext,
    path: Path = DEFAULT_MANDATORY_VETO_MANIFEST_REPLAY_GATE_PATH,
    project_root: Path = PROJECT_ROOT,
) -> ManifestReplayReport:
    loaded = load_mandatory_veto_manifest_replay_gate(path=path, project_root=project_root)
    replay = loaded.policy.replay_contract
    return ManifestReplayReport(
        schema_version=("growth_action_value_mandatory_veto_manifest_replay_report.v1"),
        policy_id=loaded.policy.policy_id,
        policy_file_sha256=loaded.file_sha256,
        policy_canonical_sha256=loaded.canonical_sha256,
        repository_context=repository_context,
        authorization_state=loaded.policy.owner_authorization.authorization_state,
        authority_identity_replay_state=replay.authority_identity_replay_state,
        exact_session_inventory_replay_state=(replay.exact_session_inventory_replay_state),
        successor_compatibility_replay_state=(replay.successor_compatibility_replay_state),
        source_capability_replay_state=replay.source_capability_replay_state,
        manifest_replay_state=replay.manifest_replay_state,
        technical_validation_state=replay.technical_validation_state,
        blocker_reason_codes=tuple(
            row.reason_code for row in loaded.policy.source_capability_gate.rows
        ),
        actual_counters=loaded.policy.actual_counters,
        terminal=loaded.terminal,
    )


def _git_value(*args: str, project_root: Path) -> str:
    completed = subprocess.run(
        ["git", *args],
        cwd=project_root,
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Replay the mandatory-veto source-admission manifest gate."
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--worktree-audit-status", choices=("PASS",), required=True)
    args = parser.parse_args(argv)
    branch_name = _git_value("branch", "--show-current", project_root=PROJECT_ROOT)
    if branch_name != "main":
        raise MandatoryVetoManifestReplayGateError(
            "MANIFEST_REPLAY_REPOSITORY_CONTEXT_REJECTED",
            "manifest replay report may only be sealed from main",
        )
    verified_branch_name: Literal["main"] = "main"
    context = RepositoryReplayContext(
        candidate_sha=_git_value("rev-parse", "HEAD", project_root=PROJECT_ROOT),
        local_main_sha=_git_value("rev-parse", "main", project_root=PROJECT_ROOT),
        origin_main_sha=_git_value(
            "rev-parse", "refs/remotes/origin/main", project_root=PROJECT_ROOT
        ),
        branch_name=verified_branch_name,
        worktree_audit_status=args.worktree_audit_status,
    )
    report = run_mandatory_veto_manifest_replay(repository_context=context)
    output = args.output if args.output.is_absolute() else PROJECT_ROOT / args.output
    output.parent.mkdir(parents=True, exist_ok=True)
    write_bytes_atomic(output, report.canonical_bytes)
    print(report.model_dump_json(indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "DEFAULT_MANDATORY_VETO_MANIFEST_REPLAY_GATE_PATH",
    "MandatoryVetoManifestReplayGate",
    "MandatoryVetoManifestReplayGateError",
    "MandatoryVetoManifestReplayGateLoadResult",
    "ManifestReplayReport",
    "RepositoryReplayContext",
    "load_mandatory_veto_manifest_replay_gate",
    "run_mandatory_veto_manifest_replay",
]
