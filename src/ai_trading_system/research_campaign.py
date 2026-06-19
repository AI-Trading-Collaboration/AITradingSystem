from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import tempfile
from datetime import UTC, datetime
from itertools import combinations
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from ai_trading_system.config import PROJECT_ROOT

DEFAULT_CAMPAIGN_ROOT = PROJECT_ROOT / "data" / "research_campaigns"
DEFAULT_CAMPAIGN_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_campaigns"
DEFAULT_MODULE_REGISTRY_PATH = (
    PROJECT_ROOT / "config" / "research" / "module_capability_registry.yaml"
)
DEFAULT_GATE_POLICY_PATH = PROJECT_ROOT / "config" / "research" / "research_gate_policies.yaml"
DEFAULT_WINDOW_POLICY_PATH = PROJECT_ROOT / "config" / "research" / "research_window_policy.yaml"
DEFAULT_MIGRATION_PATH = PROJECT_ROOT / "config" / "research" / "campaign_migrations.yaml"
DEFAULT_COMPATIBILITY_PATH = (
    PROJECT_ROOT / "config" / "research" / "task_specific_runner_compatibility.yaml"
)
DEFAULT_STAGE_ADAPTER_REGISTRY_PATH = (
    PROJECT_ROOT / "config" / "research" / "campaign_stage_adapters.yaml"
)

WORKFLOW_STAGES = [
    "DRAFT",
    "SCOPE_READY",
    "INPUT_PRECHECK",
    "MINI_DIAGNOSTIC",
    "ATTRIBUTION",
    "TARGETED_EVIDENCE",
    "FULL_DIAGNOSTIC",
    "INTERACTION",
    "GATE_READY",
    "OWNER_REVIEW",
    "ARCHIVED",
]

ADAPTER_SUPPORTED_STAGES = [
    "INPUT_PRECHECK",
    "MINI_DIAGNOSTIC",
    "TARGETED_EVIDENCE",
    "FULL_DIAGNOSTIC",
    "SIGNAL_PRECHECK",
    "SIGNAL_DIRECTION_TAXONOMY",
    "REDESIGN_HYPOTHESIS_RANKING",
    "ATTRIBUTION",
    "INTERACTION",
    "GATE",
    "OWNER_PACKET",
]

ADAPTER_OUTPUT_STATUSES = [
    "ADAPTER_READY",
    "ADAPTER_NOT_CONFIGURED",
    "ADAPTER_INPUT_MISSING",
    "ADAPTER_RUN_FAILED",
    "ADAPTER_OUTPUT_INVALID",
    "ADAPTER_SAFETY_BLOCKED",
    "ADAPTER_HOLDOUT_BLOCKED",
    "B2_COMPUTE_ADAPTER_SMOKE_PASS",
    "B2_COMPUTE_ADAPTER_BLOCKED_WITH_REASON",
    "B3_SIGNAL_COMPUTE_ADAPTER_SMOKE_PASS",
    "B3_SIGNAL_COMPUTE_ADAPTER_BLOCKED_WITH_REASON",
]

ADAPTER_RUN_MODES = [
    "AUDITED_ARTIFACT_MODE",
    "COMPUTE_MODE",
    "DRY_RUN_MODE",
    "VALIDATION_ONLY_MODE",
]

DECISION_OUTCOMES = [
    "NOT_EVALUATED",
    "PASS",
    "MIXED",
    "NEEDS_MORE_EVIDENCE",
    "PROMISING",
    "NARROW_ROLE",
    "RETURN_TO_DESIGN",
    "WEAK",
    "REJECTED",
    "BLOCKED",
    "OWNER_OVERRIDE_REQUIRED",
]

EVIDENCE_CATEGORIES = [
    "INPUT_VALIDITY",
    "SIGNAL_COVERAGE",
    "SIGNAL_DIRECTION",
    "TRIGGER_BEHAVIOR",
    "PORTFOLIO_EFFECT",
    "DRAWDOWN_PROTECTION",
    "REENTRY_BEHAVIOR",
    "TURNOVER_COST",
    "BENCHMARK_RELATIVE",
    "WINDOW_STABILITY",
    "INTERACTION_EFFECT",
    "ATTRIBUTION",
    "SCORECARD",
    "SIGNAL_LAG",
    "SIGNAL_NOISE",
    "ASSET_MAPPING",
    "WINDOW_SPECIFIC_WEAKNESS",
    "REDESIGN_HYPOTHESIS",
    "SAFETY",
]

CONTROL_ONLY_DATA_QUALITY_STATUS = "NOT_REQUIRED_CONTROL_PLANE_ONLY"
RESEARCH_ONLY_SAFETY_BOUNDARY = {
    "research_only": True,
    "manual_review_only": True,
    "official_target_weights": False,
    "paper_shadow_activation": False,
    "paper_shadow_allowed": False,
    "broker_effect": "none",
    "order_effect": "none",
    "production_effect": "none",
    "holdout_touched": False,
}
FORBIDDEN_WEIGHT_OUTPUTS = {
    "target_weight",
    "official_target_weight",
    "official_target_weights",
    "executed_weight",
    "hypothetical_weight",
    "broker_order",
    "order_ticket",
}
FORBIDDEN_EVALUATION_MUTATIONS = {
    "modified_feature",
    "modified_signal",
    "modified_target_path",
    "strategy_parameter_update",
    "allocator_rerun",
}
RESTRICTED_MORE_EVIDENCE_OUTCOMES = [
    "NARROW_ROLE",
    "RETURN_TO_DESIGN",
    "WEAK",
    "REJECTED",
    "OWNER_OVERRIDE_REQUIRED",
]

ALWAYS_BLOCKED_RESEARCH_ACTIONS = [
    "B4_RETEST",
    "B5",
    "B6",
    "V3",
    "PAPER_SHADOW",
    "EXTENDED_SHADOW",
    "LIVE_TRADING",
    "OFFICIAL_TARGET_WEIGHTS",
    "BROKER_ORDER",
]


class ResearchCampaignError(ValueError):
    pass


class StopRuleSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    code: str


class HypothesisSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    statement: str
    expected_gain: list[str] = Field(default_factory=list)
    expected_failure_modes: list[str] = Field(default_factory=list)


class ModuleGraphSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    baseline: str
    modules: list[str] = Field(default_factory=list)
    allowed_mechanisms: list[str] = Field(default_factory=list)
    forbidden_mechanisms: list[str] = Field(default_factory=list)
    allowed_interaction_order: int = 2

    @field_validator("allowed_interaction_order")
    @classmethod
    def _interaction_order_positive(cls, value: int) -> int:
        if value < 1:
            raise ValueError("allowed_interaction_order must be >= 1")
        return value


class WindowPolicySpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    development_catalog: str
    diagnostic_catalog: str
    holdout_catalog: str
    holdout_access: str = "FINAL_GATE_ONLY"
    full_catalog: str | None = None

    @field_validator("holdout_access")
    @classmethod
    def _holdout_access_known(cls, value: str) -> str:
        allowed = {"FINAL_GATE_ONLY", "OWNER_AUTHORIZATION_REQUIRED", "FORBIDDEN"}
        if value not in allowed:
            raise ValueError(f"holdout_access must be one of {sorted(allowed)}")
        return value


class EvidenceBudgetSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    max_mini_rounds: int = 1
    max_targeted_rounds: int = 2
    max_window_expansions: int = 1
    max_redesign_rounds: int = 1
    max_needs_more_evidence_occurrences: int = 2
    owner_override_required_after_budget: bool = True
    time_budget: str | None = None
    compute_budget: str | None = None

    @model_validator(mode="after")
    def _non_negative(self) -> EvidenceBudgetSpec:
        for field_name in (
            "max_mini_rounds",
            "max_targeted_rounds",
            "max_window_expansions",
            "max_redesign_rounds",
            "max_needs_more_evidence_occurrences",
        ):
            if getattr(self, field_name) < 0:
                raise ValueError(f"{field_name} must be >= 0")
        return self


class SafetySpec(BaseModel):
    model_config = ConfigDict(extra="allow")

    research_only: bool = True
    manual_review_only: bool = True
    official_target_weights: bool = False
    paper_shadow_allowed: bool = False
    broker_effect: str = "none"
    order_effect: str = "none"
    production_effect: str = "none"


class CampaignSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: str
    campaign_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")
    program_id: str
    title: str
    market_regime: str = "ai_after_chatgpt"
    requested_date_range: str | None = None
    hypothesis: HypothesisSpec
    module_graph: ModuleGraphSpec
    window_policy: WindowPolicySpec
    scorecard_policy: str
    evidence_budget: EvidenceBudgetSpec = Field(default_factory=EvidenceBudgetSpec)
    stop_rules: list[StopRuleSpec] = Field(default_factory=list)
    safety: SafetySpec = Field(default_factory=SafetySpec)
    owner_authorized_holdout: bool = False
    metadata: dict[str, Any] = Field(default_factory=dict)


class ModuleCapability(BaseModel):
    model_config = ConfigDict(extra="forbid")

    module_id: str
    module_type: str
    input_contract: list[str] = Field(default_factory=list)
    output_contract: list[str] = Field(default_factory=list)
    allowed_layers: list[str] = Field(default_factory=list)
    allowed_mechanisms: list[str] = Field(default_factory=list)
    required_features: list[str] = Field(default_factory=list)
    required_signals: list[str] = Field(default_factory=list)
    supported_window_types: list[str] = Field(default_factory=list)
    compatible_modules: list[str] = Field(default_factory=list)
    incompatible_modules: list[str] = Field(default_factory=list)
    forbidden_outputs: list[str] = Field(default_factory=list)
    version: str
    approved_for_campaign_modules: bool = True
    requires_gate_approval_for_interaction: bool = False
    interaction_alias: str | None = None


class StageAdapterInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    artifact_id: str
    path: str
    stages: list[str] = Field(default_factory=list)
    required: bool = True


class StageAdapterEvidenceMapping(BaseModel):
    model_config = ConfigDict(extra="forbid")

    stage: str
    artifact_id: str
    evidence_id_suffix: str
    category: str
    metric_name: str
    value_path: str = "status"
    direction: Literal["positive", "negative", "neutral", "mixed", "unknown"] = "unknown"
    status: Literal["PASS", "FAIL", "MIXED", "BLOCKED", "WARNING", "INFO"] = "INFO"
    confidence: Literal["high", "medium", "low", "unknown"] = "medium"
    reason_codes: list[str] = Field(default_factory=list)
    window_id: str | None = None


class StageAdapterContract(BaseModel):
    model_config = ConfigDict(extra="forbid")

    adapter_id: str
    adapter_version: str
    supported_run_modes: list[str] = Field(default_factory=list)
    default_run_mode: str = "AUDITED_ARTIFACT_MODE"
    supported_stage: list[str]
    supported_module: list[str]
    supported_campaign_type: list[str] = Field(default_factory=list)
    required_inputs: list[StageAdapterInput] = Field(default_factory=list)
    optional_inputs: list[StageAdapterInput] = Field(default_factory=list)
    produced_artifacts: list[str] = Field(default_factory=list)
    produced_evidence_categories: list[str] = Field(default_factory=list)
    supported_windows: list[str] = Field(default_factory=list)
    forbidden_windows: list[str] = Field(default_factory=list)
    safety_boundary: dict[str, Any]
    failure_modes: list[str] = Field(default_factory=list)
    evidence_mappings: list[StageAdapterEvidenceMapping] = Field(default_factory=list)
    stage_outcomes: dict[str, str] = Field(default_factory=dict)
    parity_source: str | None = None

    @model_validator(mode="after")
    def _run_modes_valid(self) -> StageAdapterContract:
        if not self.supported_run_modes:
            self.supported_run_modes = [self.default_run_mode]
        unknown = sorted(set(self.supported_run_modes) - set(ADAPTER_RUN_MODES))
        if unknown:
            raise ValueError(f"supported_run_modes contains unknown modes: {unknown}")
        if self.default_run_mode not in self.supported_run_modes:
            raise ValueError("default_run_mode must be listed in supported_run_modes")
        return self


class StageAdapterRunOutput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str
    campaign_id: str
    stage: str
    adapter_id: str
    run_mode: str = "AUDITED_ARTIFACT_MODE"
    adapter_version: str | None = None
    input_artifacts: list[dict[str, Any]] = Field(default_factory=list)
    output_artifacts: list[dict[str, Any]] = Field(default_factory=list)
    evidence_records: list[dict[str, Any]] = Field(default_factory=list)
    compute_performed: bool = False
    imported_evidence: bool = False
    parity_source: str | None = None
    failure_mode: str | None = None
    status: str
    reason_codes: list[str] = Field(default_factory=list)
    safety_metadata: dict[str, Any]
    adapter_outcome: str = "BLOCKED"
    data_quality_status: str = CONTROL_ONLY_DATA_QUALITY_STATUS


class EvidenceRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    evidence_id: str
    campaign_id: str
    run_id: str
    stage: str
    category: str
    metric_name: str
    value: Any = None
    baseline_value: Any = None
    delta: float | None = None
    direction: Literal["positive", "negative", "neutral", "mixed", "unknown"] = "unknown"
    window_id: str | None = None
    status: Literal["PASS", "FAIL", "MIXED", "BLOCKED", "WARNING", "INFO"] = "INFO"
    confidence: Literal["high", "medium", "low", "unknown"] = "unknown"
    source_artifact_id: str
    reason_codes: list[str] = Field(default_factory=list)
    created_at: str = Field(default_factory=lambda: _now_iso())


class BudgetUsed(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mini_rounds: int = 0
    targeted_rounds: int = 0
    window_expansions: int = 0
    redesign_rounds: int = 0
    needs_more_evidence_occurrences: int = 0
    holdout_accesses: int = 0
    owner_overrides: int = 0


class CampaignState(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: str = "1.0"
    campaign_id: str
    program_id: str
    title: str
    current_stage: str = "DRAFT"
    current_outcome: str = "NOT_EVALUATED"
    reason_codes: list[str] = Field(default_factory=list)
    evidence_budget_used: BudgetUsed = Field(default_factory=BudgetUsed)
    source_artifacts: list[str] = Field(default_factory=list)
    data_quality_status: str = CONTROL_ONLY_DATA_QUALITY_STATUS
    safety_boundary: dict[str, Any] = Field(
        default_factory=lambda: dict(RESEARCH_ONLY_SAFETY_BOUNDARY)
    )
    migrated_from_legacy: bool = False
    legacy_status: str | None = None
    created_at: str = Field(default_factory=lambda: _now_iso())
    updated_at: str = Field(default_factory=lambda: _now_iso())
    stage_history: list[dict[str, Any]] = Field(default_factory=list)


def load_campaign_spec(path: Path) -> CampaignSpec:
    try:
        raw = _read_yaml(path)
        return CampaignSpec.model_validate(raw)
    except ValidationError as exc:
        raise ResearchCampaignError(f"Campaign spec schema invalid: {exc}") from exc


def build_campaign_validation_payload(
    *,
    spec: CampaignSpec,
    module_registry_path: Path = DEFAULT_MODULE_REGISTRY_PATH,
    gate_policy_path: Path = DEFAULT_GATE_POLICY_PATH,
    window_policy_path: Path = DEFAULT_WINDOW_POLICY_PATH,
) -> dict[str, Any]:
    module_registry = load_module_registry(module_registry_path)
    gate_policy = _read_yaml(gate_policy_path)
    window_policy = _read_yaml(window_policy_path)
    issues: list[dict[str, Any]] = []

    issues.extend(_validate_safety(spec))
    issues.extend(_validate_module_graph(spec, module_registry))
    issues.extend(_validate_window_policy(spec, window_policy))
    issues.extend(_validate_gate_policy(spec, gate_policy))
    issues.extend(_validate_budget(spec))

    status = "PASS" if not [issue for issue in issues if issue["severity"] == "error"] else "FAIL"
    warning_count = len([issue for issue in issues if issue["severity"] == "warning"])
    if status == "PASS" and warning_count:
        status = "PASS_WITH_WARNINGS"
    return {
        "schema_version": "1.0",
        "report_type": "research_campaign_validation",
        "campaign_id": spec.campaign_id,
        "program_id": spec.program_id,
        "market_regime": spec.market_regime,
        "requested_date_range": spec.requested_date_range,
        "validation_status": status,
        "status": status,
        "issues": issues,
        "summary": {
            "issue_count": len(issues),
            "error_count": len([issue for issue in issues if issue["severity"] == "error"]),
            "warning_count": warning_count,
            "module_count": len(spec.module_graph.modules),
            "stop_rule_count": len(spec.stop_rules),
        },
        "data_quality_status": CONTROL_ONLY_DATA_QUALITY_STATUS,
        "safety_boundary": dict(RESEARCH_ONLY_SAFETY_BOUNDARY),
    }


def initialize_campaign(
    *,
    spec_path: Path,
    campaign_root: Path = DEFAULT_CAMPAIGN_ROOT,
    module_registry_path: Path = DEFAULT_MODULE_REGISTRY_PATH,
    gate_policy_path: Path = DEFAULT_GATE_POLICY_PATH,
    window_policy_path: Path = DEFAULT_WINDOW_POLICY_PATH,
    migration_path: Path = DEFAULT_MIGRATION_PATH,
    force: bool = False,
) -> dict[str, Any]:
    spec = load_campaign_spec(spec_path)
    validation = build_campaign_validation_payload(
        spec=spec,
        module_registry_path=module_registry_path,
        gate_policy_path=gate_policy_path,
        window_policy_path=window_policy_path,
    )
    if validation["validation_status"] == "FAIL":
        raise ResearchCampaignError(
            f"Campaign validation failed: {validation['summary']['error_count']} errors"
        )

    directory = campaign_directory(spec.campaign_id, campaign_root)
    if directory.exists() and not force:
        raise ResearchCampaignError(f"Campaign already exists: {directory}")
    directory.mkdir(parents=True, exist_ok=True)

    migration = load_campaign_migrations(migration_path).get(spec.campaign_id)
    state = _initial_state_from_spec(spec, migration)
    _append_transition(
        directory,
        {
            "transition_id": f"{spec.campaign_id}-init",
            "campaign_id": spec.campaign_id,
            "from_stage": None,
            "to_stage": state.current_stage,
            "outcome": state.current_outcome,
            "reason_codes": state.reason_codes,
            "created_at": _now_iso(),
            "source": "campaign_init",
        },
    )
    _write_json(directory / "spec.json", spec.model_dump(mode="json"))
    _write_json(directory / "validation.json", validation)
    write_campaign_state(state, directory)

    evidence_records: list[EvidenceRecord] = []
    if migration:
        for raw_record in migration.get("evidence_records", []):
            record = EvidenceRecord.model_validate(
                {
                    **raw_record,
                    "campaign_id": spec.campaign_id,
                    "created_at": raw_record.get("created_at") or _now_iso(),
                }
            )
            evidence_records.append(record)
    write_evidence_records(evidence_records, directory, replace=True)
    manifest = build_reproducibility_manifest(
        spec_path=spec_path,
        spec=spec,
        config_paths=[
            module_registry_path,
            gate_policy_path,
            window_policy_path,
            migration_path,
        ],
        output_paths=[
            directory / "spec.json",
            directory / "validation.json",
            directory / "state.json",
            directory / "evidence.jsonl",
        ],
        transition_source="campaign_init",
    )
    _write_json(directory / "reproducibility_manifest.json", manifest)

    return {
        "campaign_id": spec.campaign_id,
        "campaign_dir": str(directory),
        "validation_status": validation["validation_status"],
        "current_stage": state.current_stage,
        "current_outcome": state.current_outcome,
        "migrated_from_legacy": state.migrated_from_legacy,
        "evidence_record_count": len(evidence_records),
        "reproducibility_manifest": str(directory / "reproducibility_manifest.json"),
        "safety_boundary": state.safety_boundary,
    }


def campaign_plan(
    *,
    campaign_id: str,
    campaign_root: Path = DEFAULT_CAMPAIGN_ROOT,
    gate_policy_path: Path = DEFAULT_GATE_POLICY_PATH,
    adapter_registry_path: Path = DEFAULT_STAGE_ADAPTER_REGISTRY_PATH,
    module_registry_path: Path = DEFAULT_MODULE_REGISTRY_PATH,
) -> dict[str, Any]:
    spec, state, evidence = load_campaign_bundle(campaign_id, campaign_root)
    gate_payload = evaluate_gate(
        spec=spec,
        state=state,
        evidence=evidence,
        gate_policy_path=gate_policy_path,
    )
    budget_payload = evaluate_evidence_budget(spec, state)
    actions = build_next_action_plan(
        spec=spec,
        state=state,
        evidence=evidence,
        gate_payload=gate_payload,
    )
    adapter_summary = _adapter_runtime_summary(
        spec=spec,
        target_stage=actions["next_recommended_stage"],
        adapter_registry_path=adapter_registry_path,
        module_registry_path=module_registry_path,
    )
    return {
        "schema_version": "1.0",
        "report_type": "research_campaign_plan",
        "campaign_id": campaign_id,
        "current_stage": state.current_stage,
        "current_outcome": state.current_outcome,
        "reason_codes": state.reason_codes,
        "evidence_budget_used": state.evidence_budget_used.model_dump(mode="json"),
        "evidence_budget_limits": spec.evidence_budget.model_dump(mode="json"),
        "evidence_budget_remaining": budget_payload["stop_rule_proximity"],
        "budget_status": budget_payload["budget_status"],
        "stop_rule_proximity": budget_payload["stop_rule_proximity"],
        "allowed_next_actions": actions["allowed_next_actions"],
        "blocked_actions": actions["blocked_actions"],
        "required_owner_actions": actions["required_owner_actions"],
        "next_recommended_stage": actions["next_recommended_stage"],
        "adapter_runtime": adapter_summary,
        "adapter_id": adapter_summary["adapter_id"],
        "adapter_run_mode": adapter_summary["run_mode"],
        "source_artifacts": state.source_artifacts,
        "gate_preview": gate_payload,
        "data_quality_status": state.data_quality_status,
        "safety_boundary": state.safety_boundary,
    }


def run_campaign_stage(
    *,
    campaign_id: str,
    requested_stage: str = "next",
    campaign_root: Path = DEFAULT_CAMPAIGN_ROOT,
    module_registry_path: Path = DEFAULT_MODULE_REGISTRY_PATH,
    gate_policy_path: Path = DEFAULT_GATE_POLICY_PATH,
    window_policy_path: Path = DEFAULT_WINDOW_POLICY_PATH,
    adapter_registry_path: Path = DEFAULT_STAGE_ADAPTER_REGISTRY_PATH,
    output_root: Path = DEFAULT_CAMPAIGN_OUTPUT_ROOT,
) -> dict[str, Any]:
    directory = campaign_directory(campaign_id, campaign_root)
    spec, state, evidence = load_campaign_bundle(campaign_id, campaign_root)
    requested_normalized = requested_stage.upper().replace("-", "_")
    plan_payload = (
        campaign_plan(
            campaign_id=campaign_id,
            campaign_root=campaign_root,
            gate_policy_path=gate_policy_path,
            adapter_registry_path=adapter_registry_path,
            module_registry_path=module_registry_path,
        )
        if requested_normalized == "NEXT"
        else None
    )
    target_stage = (
        str(plan_payload["next_recommended_stage"])
        if plan_payload is not None
        else resolve_requested_stage(state.current_stage, requested_stage)
    )
    budget_block = _stage_budget_block_reason(
        spec=spec,
        state=state,
        target_stage=target_stage,
        requested_next=requested_normalized == "NEXT",
    )
    if budget_block is not None:
        return _record_blocked_campaign_run(
            directory=directory,
            spec=spec,
            state=state,
            from_stage=state.current_stage,
            target_stage=state.current_stage,
            reason_codes=[budget_block],
            result_payload={
                "status": "CAMPAIGN_RUN_NEXT_STAGE_BLOCKED_WITH_REASON",
                "blocked_reason": budget_block,
                "budget_status": evaluate_evidence_budget(spec, state)["budget_status"],
                "allowed_recovery_outcomes": RESTRICTED_MORE_EVIDENCE_OUTCOMES,
                "production_effect": "none",
            },
        )
    transition = validate_stage_transition(state.current_stage, target_stage)
    if not transition["allowed"]:
        raise ResearchCampaignError(transition["reason"])

    run_id = f"{campaign_id}-{target_stage.lower()}-{_compact_time()}"
    outcome = "PASS"
    reason_codes: list[str] = []
    generated_evidence: list[EvidenceRecord] = []
    result_payload: dict[str, Any]
    adapter_payload: dict[str, Any] | None = None

    if target_stage == "SCOPE_READY":
        validation = build_campaign_validation_payload(
            spec=spec,
            module_registry_path=module_registry_path,
            gate_policy_path=gate_policy_path,
            window_policy_path=window_policy_path,
        )
        outcome = "PASS" if validation["validation_status"] != "FAIL" else "BLOCKED"
        reason_codes = [
            issue["issue_id"]
            for issue in validation["issues"]
            if issue["severity"] == "error"
        ]
        result_payload = validation
    elif target_stage == "INPUT_PRECHECK":
        adapter_run = run_stage_adapter(
            spec=spec,
            state=state,
            target_stage=target_stage,
            run_id=run_id,
            adapter_registry_path=adapter_registry_path,
            module_registry_path=module_registry_path,
            gate_policy_path=gate_policy_path,
            window_policy_path=window_policy_path,
            output_root=output_root,
        )
        if adapter_run.status != "ADAPTER_NOT_CONFIGURED":
            adapter_payload = _write_stage_adapter_run_artifacts(
                adapter_run.model_dump(mode="json"),
                output_root=output_root,
            )
            outcome = adapter_run.adapter_outcome
            reason_codes = list(adapter_run.reason_codes)
            generated_evidence.extend(
                EvidenceRecord.model_validate(record) for record in adapter_run.evidence_records
            )
            result_payload = adapter_payload
        else:
            validation = build_campaign_validation_payload(
                spec=spec,
                module_registry_path=module_registry_path,
                gate_policy_path=gate_policy_path,
                window_policy_path=window_policy_path,
            )
            outcome = "PASS" if validation["validation_status"] != "FAIL" else "BLOCKED"
            reason_codes = [
                issue["issue_id"]
                for issue in validation["issues"]
                if issue["severity"] == "error"
            ]
            generated_evidence.append(
                _control_evidence(
                    campaign_id=campaign_id,
                    run_id=run_id,
                    stage=target_stage,
                    category="INPUT_VALIDITY",
                    metric_name="campaign_precheck_validation",
                    value=validation["validation_status"],
                    status="PASS" if outcome == "PASS" else "BLOCKED",
                    source_artifact_id="campaign_validation",
                    reason_codes=reason_codes,
                )
            )
            result_payload = validation
    elif target_stage in {"MINI_DIAGNOSTIC", "ATTRIBUTION", "TARGETED_EVIDENCE", "FULL_DIAGNOSTIC"}:
        adapter_run = run_stage_adapter(
            spec=spec,
            state=state,
            target_stage=target_stage,
            run_id=run_id,
            adapter_registry_path=adapter_registry_path,
            module_registry_path=module_registry_path,
            gate_policy_path=gate_policy_path,
            window_policy_path=window_policy_path,
            output_root=output_root,
        )
        if adapter_run.status != "ADAPTER_NOT_CONFIGURED":
            adapter_payload = _write_stage_adapter_run_artifacts(
                adapter_run.model_dump(mode="json"),
                output_root=output_root,
            )
            outcome = adapter_run.adapter_outcome
            reason_codes = list(adapter_run.reason_codes)
            generated_evidence.extend(
                EvidenceRecord.model_validate(record) for record in adapter_run.evidence_records
            )
            result_payload = adapter_payload
        else:
            imported_stage_evidence = [
                record for record in evidence if record.stage == target_stage
            ]
            if imported_stage_evidence:
                outcome = _outcome_from_imported_stage_evidence(imported_stage_evidence, state)
                reason_codes = sorted(
                    {
                        code
                        for record in imported_stage_evidence
                        for code in record.reason_codes
                    }
                    | set(state.reason_codes)
                )
                result_payload = {
                    "status": outcome,
                    "source": "imported_audited_evidence",
                    "evidence_record_count": len(imported_stage_evidence),
                    "source_artifact_ids": sorted(
                        {record.source_artifact_id for record in imported_stage_evidence}
                    ),
                    "production_effect": "none",
                }
            else:
                adapter_payload = _write_stage_adapter_run_artifacts(
                    adapter_run.model_dump(mode="json"),
                    output_root=output_root,
                )
                outcome = "BLOCKED"
                reason_codes = ["STAGE_ADAPTER_NOT_CONFIGURED"]
                generated_evidence.append(
                    _control_evidence(
                        campaign_id=campaign_id,
                        run_id=run_id,
                        stage=target_stage,
                        category="SAFETY",
                        metric_name="stage_execution_adapter",
                        value="not_configured",
                        status="BLOCKED",
                        source_artifact_id="campaign_stage_runner",
                        reason_codes=reason_codes,
                    )
                )
                result_payload = adapter_payload
    elif target_stage == "INTERACTION":
        matrix = plan_experiment_matrix(spec=spec, module_registry_path=module_registry_path)
        generated_evidence.append(
            _control_evidence(
                campaign_id=campaign_id,
                run_id=run_id,
                stage=target_stage,
                category="INTERACTION_EFFECT",
                metric_name="experiment_matrix_count",
                value=len(matrix["experiment_matrix"]),
                status="PASS",
                source_artifact_id="experiment_design_planner",
                reason_codes=[],
            )
        )
        result_payload = matrix
    elif target_stage == "GATE_READY":
        adapter_run = run_stage_adapter(
            spec=spec,
            state=state,
            target_stage=target_stage,
            run_id=run_id,
            adapter_registry_path=adapter_registry_path,
            module_registry_path=module_registry_path,
            gate_policy_path=gate_policy_path,
            window_policy_path=window_policy_path,
            output_root=output_root,
        )
        if adapter_run.status != "ADAPTER_NOT_CONFIGURED":
            adapter_payload = _write_stage_adapter_run_artifacts(
                adapter_run.model_dump(mode="json"),
                output_root=output_root,
            )
            gate_preview = evaluate_gate(
                spec=spec,
                state=state,
                evidence=[
                    *evidence,
                    *(
                        EvidenceRecord.model_validate(r)
                        for r in adapter_run.evidence_records
                    ),
                ],
                gate_policy_path=gate_policy_path,
            )
            outcome = adapter_run.adapter_outcome
            reason_codes = list(adapter_run.reason_codes)
            generated_evidence.extend(
                EvidenceRecord.model_validate(record) for record in adapter_run.evidence_records
            )
            result_payload = {**adapter_payload, "gate_preview": gate_preview}
        else:
            result_payload = evaluate_gate(
                spec=spec,
                state=state,
                evidence=evidence,
                gate_policy_path=gate_policy_path,
            )
            outcome = str(result_payload["decision_outcome"])
            reason_codes = list(result_payload["reason_codes"])
    elif target_stage == "OWNER_REVIEW":
        result_payload = build_owner_packet(
            campaign_id=campaign_id,
            campaign_root=campaign_root,
            output_root=output_root,
            gate_policy_path=gate_policy_path,
        )
        outcome = "PASS"
    else:
        raise ResearchCampaignError(f"Stage cannot be run directly: {target_stage}")

    if target_stage == "MINI_DIAGNOSTIC" and outcome != "BLOCKED":
        state.evidence_budget_used.mini_rounds += 1
    if target_stage == "TARGETED_EVIDENCE" and outcome != "BLOCKED":
        state.evidence_budget_used.targeted_rounds += 1
    if outcome == "NEEDS_MORE_EVIDENCE":
        state.evidence_budget_used.needs_more_evidence_occurrences += 1

    state.current_stage = target_stage
    state.current_outcome = outcome
    state.reason_codes = reason_codes
    state.updated_at = _now_iso()
    state.stage_history.append(
        {
            "run_id": run_id,
            "stage": target_stage,
            "outcome": outcome,
            "reason_codes": reason_codes,
            "created_at": state.updated_at,
        }
    )
    write_campaign_state(state, directory)
    append_evidence_records(generated_evidence, directory)
    _append_transition(
        directory,
        {
            "transition_id": run_id,
            "campaign_id": campaign_id,
            "from_stage": transition["from_stage"],
            "to_stage": target_stage,
            "outcome": outcome,
            "reason_codes": reason_codes,
            "created_at": state.updated_at,
            "source": "campaign_stage_runner",
        },
    )
    _append_jsonl(
        directory / "runs.jsonl",
        {
            "run_id": run_id,
            "campaign_id": campaign_id,
            "stage": target_stage,
            "outcome": outcome,
            "reason_codes": reason_codes,
            "created_at": state.updated_at,
            "result": result_payload,
            "adapter_id": (
                result_payload.get("adapter_id")
                if isinstance(result_payload, dict)
                else None
            ),
            "adapter_status": (
                result_payload.get("status") if isinstance(result_payload, dict) else None
            ),
            "production_effect": "none",
        },
    )
    return {
        "run_id": run_id,
        "campaign_id": campaign_id,
        "stage": target_stage,
        "outcome": outcome,
        "reason_codes": reason_codes,
        "generated_evidence_count": len(generated_evidence),
        "result": result_payload,
        "adapter_id": (
            result_payload.get("adapter_id") if isinstance(result_payload, dict) else None
        ),
        "adapter_status": (
            result_payload.get("status") if isinstance(result_payload, dict) else None
        ),
        "safety_boundary": state.safety_boundary,
    }


def build_status_payload(
    *,
    campaign_id: str,
    detailed: bool = False,
    campaign_root: Path = DEFAULT_CAMPAIGN_ROOT,
    compatibility_path: Path = DEFAULT_COMPATIBILITY_PATH,
) -> dict[str, Any]:
    spec, state, evidence = load_campaign_bundle(campaign_id, campaign_root)
    plan = campaign_plan(campaign_id=campaign_id, campaign_root=campaign_root)
    payload: dict[str, Any] = {
        "schema_version": "1.0",
        "report_type": "research_campaign_status",
        "campaign_id": campaign_id,
        "title": spec.title,
        "market_regime": spec.market_regime,
        "requested_date_range": spec.requested_date_range,
        "current_stage": state.current_stage,
        "current_outcome": state.current_outcome,
        "reason_codes": state.reason_codes,
        "evidence_record_count": len(evidence),
        "evidence_budget_used": state.evidence_budget_used.model_dump(mode="json"),
        "evidence_budget_remaining": plan["evidence_budget_remaining"],
        "budget_status": plan["budget_status"],
        "allowed_next_actions": plan["allowed_next_actions"],
        "blocked_actions": plan["blocked_actions"],
        "required_owner_actions": plan["required_owner_actions"],
        "adapter_runtime": plan["adapter_runtime"],
        "adapter_id": plan["adapter_id"],
        "adapter_run_mode": plan["adapter_run_mode"],
        "source_artifacts": state.source_artifacts,
        "data_quality_status": state.data_quality_status,
        "safety_boundary": state.safety_boundary,
        "production_effect": "none",
    }
    if detailed:
        payload["evidence_budget_limits"] = spec.evidence_budget.model_dump(mode="json")
        payload["stage_history"] = state.stage_history
        payload["task_specific_runner_compatibility"] = load_compatibility_policy(
            compatibility_path
        )
    return payload


def build_b2_campaign_adapter_parity_map(
    *,
    adapter_registry_path: Path = DEFAULT_STAGE_ADAPTER_REGISTRY_PATH,
) -> dict[str, Any]:
    contracts = load_stage_adapter_contracts(adapter_registry_path)
    contract = contracts.get("b2-risk-overlay-audited-artifact-adapter-v1")
    if contract is None:
        entries: list[dict[str, Any]] = []
        status = "FAIL"
        issues = ["B2_ADAPTER_CONTRACT_MISSING"]
    else:
        entries = []
        mapped_inputs = {
            mapping.artifact_id: mapping
            for mapping in contract.evidence_mappings
            if mapping.stage in {"TARGETED_EVIDENCE", "FULL_DIAGNOSTIC", "ATTRIBUTION", "GATE"}
        }
        for input_spec in contract.required_inputs:
            mapping = mapped_inputs.get(input_spec.artifact_id)
            stage = input_spec.stages[0] if input_spec.stages else "UNKNOWN"
            entries.append(
                {
                    "old_artifact_id": input_spec.artifact_id,
                    "new_campaign_stage": mapping.stage if mapping else stage,
                    "expected_adapter_id": contract.adapter_id,
                    "expected_evidence_category": mapping.category if mapping else "SAFETY",
                    "parity_check_method": "source_json_status_and_safety_metadata_match",
                    "parity_required": True,
                }
            )
        expected_artifacts = {
            "b2_fast_risk_no_trigger_audit",
            "b2_slow_drawdown_repeatability_study",
            "b2_reentry_lag_root_cause_review",
            "b2_cost_benchmark_utility_review",
            "b2_no_trigger_correctness_review",
            "b2_needs_more_evidence_root_cause_drilldown",
            "b2_final_research_gate",
        }
        missing = sorted(expected_artifacts - {entry["old_artifact_id"] for entry in entries})
        status = "PASS" if not missing else "FAIL"
        issues = [f"MISSING_PARITY_MAP_{artifact}" for artifact in missing]
    return {
        "schema_version": "1.0",
        "report_type": "b2_campaign_adapter_parity_map",
        "status": status,
        "market_regime": "ai_after_chatgpt",
        "requested_date_range": "2022-12-01..2026-06-18",
        "parity_entries": entries,
        "orphan_b2_evidence": [],
        "issues": issues,
        "data_quality_status": CONTROL_ONLY_DATA_QUALITY_STATUS,
        "safety_boundary": _adapter_safety_metadata(),
        "production_effect": "none",
    }


def build_b2_campaign_parity_validation(
    *,
    campaign_root: Path = DEFAULT_CAMPAIGN_ROOT,
    campaign_id: str = "b2-risk-overlay-current-form",
) -> dict[str, Any]:
    spec, state, evidence = load_campaign_bundle(campaign_id, campaign_root)
    plan = campaign_plan(campaign_id=campaign_id, campaign_root=campaign_root)
    expected_reason_codes = {
        "FAST_RISK_NOT_SUPPORTED",
        "SLOW_DRAWDOWN_SINGLE_WINDOW_ONLY",
        "REENTRY_LAG_SIGNAL_DRIVEN",
        "UTILITY_MIXED",
    }
    expected_blocked = {"B4_RETEST", "B5", "B6", "V3", "PAPER_SHADOW"}
    positive_reason_codes = {
        code
        for record in evidence
        for code in record.reason_codes
        if record.direction == "positive"
    }
    all_evidence_reason_codes = {
        code for record in evidence for code in record.reason_codes
    }
    checks = [
        {
            "check_id": "campaign_stage_outcome_matches_old_outcome",
            "passed": state.current_stage == "TARGETED_EVIDENCE"
            and state.current_outcome == "NEEDS_MORE_EVIDENCE",
        },
        {
            "check_id": "reason_codes_cover_old_conclusion",
            "passed": expected_reason_codes <= set(state.reason_codes)
            or expected_reason_codes <= all_evidence_reason_codes,
        },
        {
            "check_id": "positive_control_evidence_present",
            "passed": "CONTROL_BEHAVIOR_CLEAN" in positive_reason_codes,
        },
        {
            "check_id": "blocked_actions_match_manual_conclusion",
            "passed": expected_blocked <= set(plan["blocked_actions"]),
        },
        {
            "check_id": "no_fabricated_metrics",
            "passed": all(record.source_artifact_id for record in evidence),
        },
    ]
    status = "PASS" if all(check["passed"] for check in checks) else "FAIL"
    return {
        "schema_version": "1.0",
        "report_type": "b2_campaign_parity_validation",
        "campaign_id": campaign_id,
        "status": status,
        "stage": state.current_stage,
        "outcome": state.current_outcome,
        "reason_codes": state.reason_codes,
        "positive_evidence": sorted(positive_reason_codes),
        "blocked_actions": plan["blocked_actions"],
        "checks": checks,
        "source_artifacts": state.source_artifacts,
        "market_regime": spec.market_regime,
        "requested_date_range": spec.requested_date_range,
        "data_quality_status": state.data_quality_status,
        "safety_boundary": state.safety_boundary,
        "production_effect": "none",
    }


def build_evidence_budget_enforcement_report(
    *,
    campaign_root: Path = DEFAULT_CAMPAIGN_ROOT,
    campaign_id: str = "b2-risk-overlay-current-form",
) -> dict[str, Any]:
    spec, state, _ = load_campaign_bundle(campaign_id, campaign_root)
    budget = evaluate_evidence_budget(spec, state)
    targeted_remaining = budget["stop_rule_proximity"]["targeted_rounds_remaining"]
    checks = [
        {
            "check_id": "max_mini_rounds_declared",
            "passed": spec.evidence_budget.max_mini_rounds >= 0,
        },
        {
            "check_id": "max_targeted_rounds_declared",
            "passed": spec.evidence_budget.max_targeted_rounds >= 0,
        },
        {
            "check_id": "needs_more_evidence_has_limit",
            "passed": spec.evidence_budget.max_needs_more_evidence_occurrences >= 0,
        },
        {
            "check_id": "owner_override_required_after_budget",
            "passed": spec.evidence_budget.owner_override_required_after_budget is True,
        },
    ]
    return {
        "schema_version": "1.0",
        "report_type": "evidence_budget_enforcement_report",
        "campaign_id": campaign_id,
        "status": "PASS" if all(check["passed"] for check in checks) else "FAIL",
        "budget_status": budget["budget_status"],
        "evidence_budget_used": state.evidence_budget_used.model_dump(mode="json"),
        "evidence_budget_limits": spec.evidence_budget.model_dump(mode="json"),
        "stop_rule_proximity": budget["stop_rule_proximity"],
        "b2_final_targeted_round_rule": (
            "one_final_targeted_round_available"
            if targeted_remaining == 1
            else "no_additional_targeted_round_without_owner_override"
        ),
        "restricted_outcomes_when_exhausted": budget["restricted_outcomes_when_exhausted"],
        "checks": checks,
        "data_quality_status": state.data_quality_status,
        "safety_boundary": state.safety_boundary,
        "production_effect": "none",
    }


def build_campaign_next_action_parity_review(
    *,
    campaign_root: Path = DEFAULT_CAMPAIGN_ROOT,
) -> dict[str, Any]:
    b2 = campaign_plan(campaign_id="b2-risk-overlay-current-form", campaign_root=campaign_root)
    b3 = campaign_plan(campaign_id="b3-slow-tilt-signal-precheck", campaign_root=campaign_root)
    b2_allowed = {
        "COMPLETE_FINAL_REPEATABILITY_ROUND",
        "NARROW_ROLE",
        "RETURN_TO_DESIGN",
    }
    b2_blocked = {
        "B4_RETEST",
        "B5",
        "B6",
        "V3",
        "PAPER_SHADOW",
        "EXTENDED_SHADOW",
        "LIVE_TRADING",
        "OFFICIAL_TARGET_WEIGHTS",
        "BROKER_ORDER",
    }
    b3_allowed = {
        "CONTINUE_SIGNAL_DIRECTION_REDESIGN",
        "RUN_SIGNAL_ONLY_PRECHECK_IF_HYPOTHESIS_CHANGES",
    }
    b3_blocked = {"B3_MINI_BACKFILL", "B4_RETEST", "B5", "B6", "V3", "PAPER_SHADOW"}
    checks = [
        {
            "check_id": "b2_allowed_actions_match_manual_conclusion",
            "passed": b2_allowed <= set(b2["allowed_next_actions"]),
        },
        {
            "check_id": "b2_blocked_actions_match_manual_conclusion",
            "passed": b2_blocked <= set(b2["blocked_actions"]),
        },
        {
            "check_id": "b3_allowed_actions_match_manual_conclusion",
            "passed": b3_allowed <= set(b3["allowed_next_actions"]),
        },
        {
            "check_id": "b3_blocked_actions_match_manual_conclusion",
            "passed": b3_blocked <= set(b3["blocked_actions"]),
        },
    ]
    return {
        "schema_version": "1.0",
        "report_type": "campaign_next_action_parity_review",
        "status": "PASS" if all(check["passed"] for check in checks) else "FAIL",
        "b2_allowed_actions": b2["allowed_next_actions"],
        "b2_blocked_actions": b2["blocked_actions"],
        "b3_allowed_actions": b3["allowed_next_actions"],
        "b3_blocked_actions": b3["blocked_actions"],
        "checks": checks,
        "safety_boundary": _adapter_safety_metadata(),
        "production_effect": "none",
    }


def build_b2_compute_adapter_smoke_report(
    *,
    campaign_root: Path = DEFAULT_CAMPAIGN_ROOT,
    output_root: Path = DEFAULT_CAMPAIGN_OUTPUT_ROOT,
    adapter_registry_path: Path = DEFAULT_STAGE_ADAPTER_REGISTRY_PATH,
    campaign_id: str = "b2-risk-overlay-current-form",
) -> dict[str, Any]:
    spec, state, _ = load_campaign_bundle(campaign_id, campaign_root)
    adapter_run = run_stage_adapter(
        spec=spec,
        state=state,
        target_stage="TARGETED_EVIDENCE",
        run_id=f"{campaign_id}-b2-compute-smoke-{_compact_time()}",
        adapter_registry_path=adapter_registry_path,
        output_root=output_root,
    )
    run_artifact = _write_stage_adapter_run_artifacts(
        adapter_run.model_dump(mode="json"),
        output_root=output_root,
    )
    checks = [
        {
            "check_id": "compute_mode_selected",
            "passed": adapter_run.run_mode == "COMPUTE_MODE",
        },
        {
            "check_id": "real_compute_performed",
            "passed": adapter_run.compute_performed is True
            and adapter_run.imported_evidence is False,
        },
        {
            "check_id": "b2_smoke_status_pass",
            "passed": adapter_run.status == "B2_COMPUTE_ADAPTER_SMOKE_PASS",
        },
        {
            "check_id": "no_forbidden_production_effects",
            "passed": run_artifact["safety_metadata"]["production_effect"] == "none"
            and run_artifact["safety_metadata"]["holdout_touched"] is False,
        },
    ]
    status = (
        "B2_COMPUTE_ADAPTER_SMOKE_PASS"
        if all(check["passed"] for check in checks)
        else "B2_COMPUTE_ADAPTER_BLOCKED_WITH_REASON"
    )
    return {
        "schema_version": "1.0",
        "report_type": "b2_compute_adapter_smoke",
        "campaign_id": campaign_id,
        "status": status,
        "checks": checks,
        "adapter_run": run_artifact,
        "market_regime": spec.market_regime,
        "requested_date_range": spec.requested_date_range,
        "data_quality_status": adapter_run.data_quality_status,
        "safety_boundary": _adapter_safety_metadata(),
        "production_effect": "none",
    }


def build_b2_compute_parity_validation(
    *,
    campaign_root: Path = DEFAULT_CAMPAIGN_ROOT,
    output_root: Path = DEFAULT_CAMPAIGN_OUTPUT_ROOT,
    adapter_registry_path: Path = DEFAULT_STAGE_ADAPTER_REGISTRY_PATH,
    campaign_id: str = "b2-risk-overlay-current-form",
    smoke_report: dict[str, Any] | None = None,
) -> dict[str, Any]:
    spec, state, _ = load_campaign_bundle(campaign_id, campaign_root)
    smoke = smoke_report or build_b2_compute_adapter_smoke_report(
        campaign_root=campaign_root,
        output_root=output_root,
        adapter_registry_path=adapter_registry_path,
        campaign_id=campaign_id,
    )
    adapter_run = smoke["adapter_run"]
    evidence_by_metric = {
        record["metric_name"]: record.get("value")
        for record in adapter_run.get("evidence_records", [])
    }
    audited_rerun = _read_json(PROJECT_ROOT / "docs" / "research" / "b2_control_window_rerun.json")
    audited_no_trigger = _read_json(
        PROJECT_ROOT / "docs" / "research" / "b2_no_trigger_correctness_review.json"
    )
    plan = campaign_plan(campaign_id=campaign_id, campaign_root=campaign_root)
    checks = [
        {
            "check_id": "status_matches_expected_control_rerun",
            "passed": smoke["status"] == "B2_COMPUTE_ADAPTER_SMOKE_PASS"
            and audited_rerun.get("status") == "B2_CONTROL_RERUN_COMPLETE",
        },
        {
            "check_id": "reason_codes_include_smoke_pass",
            "passed": "B2_COMPUTE_ADAPTER_SMOKE_PASS" in adapter_run["reason_codes"],
        },
        {
            "check_id": "risk_trigger_count_matches_audited",
            "passed": evidence_by_metric.get("risk_trigger_count")
            == audited_rerun.get("aggregate", {}).get("trigger_count"),
        },
        {
            "check_id": "false_risk_off_count_matches_audited",
            "passed": evidence_by_metric.get("false_risk_off_count")
            == audited_rerun.get("aggregate", {}).get("false_risk_off_count"),
        },
        {
            "check_id": "exposure_change_count_matches_audited",
            "passed": evidence_by_metric.get("unnecessary_exposure_reduction_count")
            == audited_rerun.get("aggregate", {}).get(
                "unnecessary_exposure_reduction_count"
            ),
        },
        {
            "check_id": "no_trigger_status_matches_audited",
            "passed": evidence_by_metric.get("no_trigger_correctness_status")
            == audited_no_trigger.get("status"),
        },
        {
            "check_id": "evidence_categories_cover_control_safety_and_portfolio",
            "passed": {"TRIGGER_BEHAVIOR", "SAFETY", "PORTFOLIO_EFFECT"}
            <= {
                record["category"]
                for record in adapter_run.get("evidence_records", [])
            },
        },
        {
            "check_id": "next_actions_remain_manual_review_safe",
            "passed": "COMPLETE_FINAL_REPEATABILITY_ROUND" in plan["allowed_next_actions"]
            and "B4_RETEST" in plan["blocked_actions"],
        },
        {
            "check_id": "safety_metadata_matches_audited_boundary",
            "passed": adapter_run["safety_metadata"]["production_effect"] == "none"
            and adapter_run["safety_metadata"]["holdout_touched"] is False,
        },
    ]
    status = (
        "B2_COMPUTE_PARITY_PASS"
        if all(check["passed"] for check in checks)
        else "B2_COMPUTE_PARITY_FAIL"
    )
    return {
        "schema_version": "1.0",
        "report_type": "b2_compute_parity_validation",
        "campaign_id": campaign_id,
        "status": status,
        "checks": checks,
        "computed_status": smoke["status"],
        "audited_status": audited_rerun.get("status"),
        "state_reason_codes": state.reason_codes,
        "adapter_reason_codes": adapter_run["reason_codes"],
        "computed_metrics": evidence_by_metric,
        "audited_metrics": audited_rerun.get("aggregate", {}),
        "parity_source": str(PROJECT_ROOT / "docs" / "research" / "b2_control_window_rerun.json"),
        "market_regime": spec.market_regime,
        "requested_date_range": spec.requested_date_range,
        "data_quality_status": smoke["data_quality_status"],
        "safety_boundary": _adapter_safety_metadata(),
        "production_effect": "none",
    }


def build_campaign_run_next_stage_smoke_report(
    *,
    campaign_root: Path = DEFAULT_CAMPAIGN_ROOT,
    output_root: Path = DEFAULT_CAMPAIGN_OUTPUT_ROOT,
    adapter_registry_path: Path = DEFAULT_STAGE_ADAPTER_REGISTRY_PATH,
    campaign_id: str = "b2-risk-overlay-current-form",
) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="campaign-run-next-smoke-") as tmp:
        tmp_root = Path(tmp) / "campaigns"
        shutil.copytree(campaign_directory(campaign_id, campaign_root), tmp_root / campaign_id)
        try:
            result = run_campaign_stage(
                campaign_id=campaign_id,
                requested_stage="next",
                campaign_root=tmp_root,
                adapter_registry_path=adapter_registry_path,
                output_root=output_root / "run_next_stage_smoke",
            )
            blocked_reason = None
        except ResearchCampaignError as exc:
            result = {
                "outcome": "BLOCKED",
                "stage": "UNKNOWN",
                "adapter_status": None,
                "reason_codes": [str(exc)],
            }
            blocked_reason = str(exc)
    checks = [
        {"check_id": "run_next_not_blocked", "passed": result["outcome"] != "BLOCKED"},
        {
            "check_id": "run_next_stage_is_safe_b2_stage",
            "passed": result["stage"] == "TARGETED_EVIDENCE",
        },
        {
            "check_id": "run_next_uses_b2_compute_adapter",
            "passed": result.get("adapter_status") == "B2_COMPUTE_ADAPTER_SMOKE_PASS",
        },
        {
            "check_id": "run_next_production_effect_none",
            "passed": result.get("safety_boundary", {}).get("production_effect") == "none",
        },
    ]
    status = (
        "CAMPAIGN_RUN_NEXT_STAGE_SMOKE_PASS"
        if all(check["passed"] for check in checks)
        else "CAMPAIGN_RUN_NEXT_STAGE_BLOCKED_WITH_REASON"
    )
    return {
        "schema_version": "1.0",
        "report_type": "campaign_run_next_stage_smoke",
        "campaign_id": campaign_id,
        "status": status,
        "blocked_reason": blocked_reason,
        "checks": checks,
        "run_result": result,
        "safety_boundary": _adapter_safety_metadata(),
        "production_effect": "none",
    }


def build_evidence_budget_forced_transition_report(
    *,
    campaign_root: Path = DEFAULT_CAMPAIGN_ROOT,
    output_root: Path = DEFAULT_CAMPAIGN_OUTPUT_ROOT,
    adapter_registry_path: Path = DEFAULT_STAGE_ADAPTER_REGISTRY_PATH,
    campaign_id: str = "b2-risk-overlay-current-form",
) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="campaign-budget-forced-") as tmp:
        tmp_root = Path(tmp) / "campaigns"
        shutil.copytree(campaign_directory(campaign_id, campaign_root), tmp_root / campaign_id)
        allowed_result = run_campaign_stage(
            campaign_id=campaign_id,
            requested_stage="next",
            campaign_root=tmp_root,
            adapter_registry_path=adapter_registry_path,
            output_root=output_root / "budget_forced_transition",
        )

    with tempfile.TemporaryDirectory(prefix="campaign-budget-exhausted-") as tmp:
        tmp_root = Path(tmp) / "campaigns"
        tmp_campaign_dir = tmp_root / campaign_id
        shutil.copytree(campaign_directory(campaign_id, campaign_root), tmp_campaign_dir)
        spec, state, evidence = load_campaign_bundle(campaign_id, tmp_root)
        state.evidence_budget_used.targeted_rounds = spec.evidence_budget.max_targeted_rounds
        state.evidence_budget_used.needs_more_evidence_occurrences = (
            spec.evidence_budget.max_needs_more_evidence_occurrences
        )
        write_campaign_state(state, tmp_campaign_dir)
        exhausted_plan = campaign_plan(campaign_id=campaign_id, campaign_root=tmp_root)
        exhausted_gate = evaluate_gate(spec=spec, state=state, evidence=evidence)
        exhausted_result = run_campaign_stage(
            campaign_id=campaign_id,
            requested_stage="next",
            campaign_root=tmp_root,
            adapter_registry_path=adapter_registry_path,
            output_root=output_root / "budget_forced_transition",
        )

    checks = [
        {
            "check_id": "remaining_budget_allows_limited_next_evidence",
            "passed": allowed_result["outcome"] != "BLOCKED",
        },
        {
            "check_id": "exhausted_budget_blocks_generic_needs_more_evidence",
            "passed": exhausted_result["outcome"] == "BLOCKED"
            and "EVIDENCE_BUDGET_EXHAUSTED_OWNER_OVERRIDE_REQUIRED"
            in exhausted_result["reason_codes"],
        },
        {
            "check_id": "exhausted_budget_forces_restricted_outcomes",
            "passed": set(exhausted_gate["allowed_decision_outcomes"])
            == set(RESTRICTED_MORE_EVIDENCE_OUTCOMES),
        },
        {
            "check_id": "owner_override_is_explicit_and_audited",
            "passed": "OWNER_OVERRIDE_REQUIRED_FOR_MORE_EVIDENCE"
            in exhausted_plan["required_owner_actions"],
        },
    ]
    return {
        "schema_version": "1.0",
        "report_type": "evidence_budget_forced_transition_report",
        "campaign_id": campaign_id,
        "status": (
            "EVIDENCE_BUDGET_FORCED_TRANSITION_PASS"
            if all(check["passed"] for check in checks)
            else "EVIDENCE_BUDGET_FORCED_TRANSITION_FAIL"
        ),
        "checks": checks,
        "allowed_budget_run_result": allowed_result,
        "exhausted_budget_run_result": exhausted_result,
        "restricted_outcomes_when_exhausted": RESTRICTED_MORE_EVIDENCE_OUTCOMES,
        "safety_boundary": _adapter_safety_metadata(),
        "production_effect": "none",
    }


def build_b3_signal_compute_adapter_smoke_report(
    *,
    campaign_root: Path = DEFAULT_CAMPAIGN_ROOT,
    output_root: Path = DEFAULT_CAMPAIGN_OUTPUT_ROOT,
    adapter_registry_path: Path = DEFAULT_STAGE_ADAPTER_REGISTRY_PATH,
    campaign_id: str = "b3-slow-tilt-signal-precheck",
) -> dict[str, Any]:
    spec, state, _ = load_campaign_bundle(campaign_id, campaign_root)
    adapter_run = run_stage_adapter(
        spec=spec,
        state=state,
        target_stage="INPUT_PRECHECK",
        run_id=f"{campaign_id}-b3-signal-compute-smoke-{_compact_time()}",
        adapter_registry_path=adapter_registry_path,
        output_root=output_root,
    )
    run_artifact = _write_stage_adapter_run_artifacts(
        adapter_run.model_dump(mode="json"),
        output_root=output_root,
    )
    checks = [
        {
            "check_id": "signal_compute_mode_selected",
            "passed": adapter_run.run_mode == "COMPUTE_MODE",
        },
        {
            "check_id": "b3_signal_smoke_pass",
            "passed": adapter_run.status == "B3_SIGNAL_COMPUTE_ADAPTER_SMOKE_PASS",
        },
        {
            "check_id": "no_weight_or_backfill_outputs",
            "passed": all(
                record["metric_name"] != "target_weight"
                for record in run_artifact.get("evidence_records", [])
            )
            and run_artifact["safety_metadata"]["official_target_weights"] is False,
        },
        {
            "check_id": "b4_b5_b6_v3_blocked",
            "passed": {"B3_PRECHECK_MIXED", "SIGNAL_DIRECTION_MIXED"}
            <= set(adapter_run.reason_codes),
        },
    ]
    return {
        "schema_version": "1.0",
        "report_type": "b3_signal_compute_adapter_smoke",
        "campaign_id": campaign_id,
        "status": (
            "B3_SIGNAL_COMPUTE_ADAPTER_SMOKE_PASS"
            if all(check["passed"] for check in checks)
            else "B3_SIGNAL_COMPUTE_ADAPTER_BLOCKED_WITH_REASON"
        ),
        "checks": checks,
        "adapter_run": run_artifact,
        "market_regime": spec.market_regime,
        "requested_date_range": spec.requested_date_range,
        "data_quality_status": adapter_run.data_quality_status,
        "safety_boundary": _adapter_safety_metadata(),
        "production_effect": "none",
    }


def build_campaign_status_ux_report(
    *,
    campaign_root: Path = DEFAULT_CAMPAIGN_ROOT,
    campaign_id: str = "b2-risk-overlay-current-form",
) -> dict[str, Any]:
    concise = build_status_payload(campaign_id=campaign_id, campaign_root=campaign_root)
    detailed = build_status_payload(
        campaign_id=campaign_id,
        detailed=True,
        campaign_root=campaign_root,
    )
    plan = campaign_plan(campaign_id=campaign_id, campaign_root=campaign_root)
    required_fields = {
        "current_stage",
        "current_outcome",
        "reason_codes",
        "evidence_budget_used",
        "evidence_budget_remaining",
        "allowed_next_actions",
        "blocked_actions",
        "required_owner_actions",
        "safety_boundary",
        "adapter_run_mode",
        "source_artifacts",
    }
    checks = [
        {
            "check_id": "concise_status_contains_workflow_fields",
            "passed": required_fields <= set(concise),
        },
        {
            "check_id": "detailed_status_contains_history_and_compatibility",
            "passed": {"stage_history", "task_specific_runner_compatibility"} <= set(detailed),
        },
        {
            "check_id": "plan_contains_allowed_blocked_budget_and_adapter_mode",
            "passed": {
                "allowed_next_actions",
                "blocked_actions",
                "evidence_budget_remaining",
                "adapter_run_mode",
            }
            <= set(plan),
        },
    ]
    return {
        "schema_version": "1.0",
        "report_type": "campaign_status_plan_ux_report",
        "campaign_id": campaign_id,
        "status": (
            "CAMPAIGN_STATUS_AND_PLAN_UX_PASS"
            if all(check["passed"] for check in checks)
            else "CAMPAIGN_STATUS_AND_PLAN_UX_FAIL"
        ),
        "checks": checks,
        "concise_status": concise,
        "detailed_status": detailed,
        "plan": plan,
        "safety_boundary": _adapter_safety_metadata(),
        "production_effect": "none",
    }


def build_case_specific_runner_deprecation_plan(
    *,
    compatibility_path: Path = DEFAULT_COMPATIBILITY_PATH,
) -> dict[str, Any]:
    policy = load_compatibility_policy(compatibility_path)
    replacements = {
        "b2-control-window-research": (
            "aits research campaign run --id b2-risk-overlay-current-form --stage next"
        ),
        "b2-targeted-evidence-research": (
            "aits research campaign run --id b2-risk-overlay-current-form --stage TARGETED_EVIDENCE"
        ),
        "b2-final-decision-research": (
            "aits research campaign gate/status/packet --id b2-risk-overlay-current-form"
        ),
        "b2-b3-v2-research": (
            "aits research campaign run --id b3-slow-tilt-signal-precheck --stage INPUT_PRECHECK"
        ),
    }
    runners = []
    for runner in policy.get("deprecated_task_specific_runners", []):
        runner_id = str(runner.get("runner_id"))
        runners.append(
            {
                "old_command": f"aits etf weight-research {runner_id}",
                "replacement_campaign_command": replacements.get(
                    runner_id,
                    "not_yet_replaceable",
                ),
                "parity_status": runner.get("parity_status", "PARTIAL"),
                "deprecation_status": runner.get("deprecation_status", "KEEP_COMPATIBILITY"),
                "compatibility_window": runner.get(
                    "compatibility_window",
                    "keep_until_campaign_compute_parity_and_owner_review",
                ),
                "commands_not_yet_replaceable": runner.get("commands_not_yet_replaceable", []),
                "blockers": runner.get("blockers", []),
                "extension_allowed": runner.get("extension_allowed", False),
            }
        )
    checks = [
        {"check_id": "old_commands_listed", "passed": bool(runners)},
        {
            "check_id": "replacement_commands_declared",
            "passed": all(row["replacement_campaign_command"] for row in runners),
        },
        {
            "check_id": "old_commands_not_removed",
            "passed": all(row["deprecation_status"] != "REMOVED" for row in runners),
        },
    ]
    return {
        "schema_version": "1.0",
        "report_type": "case_specific_runner_deprecation_plan",
        "status": (
            "CASE_SPECIFIC_RUNNER_DEPRECATION_PLAN_READY"
            if all(check["passed"] for check in checks)
            else "CASE_SPECIFIC_RUNNER_DEPRECATION_PLAN_INCOMPLETE"
        ),
        "checks": checks,
        "old_runners": runners,
        "commands_not_yet_replaceable": [
            row
            for row in runners
            if row["replacement_campaign_command"] == "not_yet_replaceable"
            or row["commands_not_yet_replaceable"]
        ],
        "safety_boundary": _adapter_safety_metadata(),
        "production_effect": "none",
    }


def build_campaign_control_plane_v1_validation_pack(
    *,
    campaign_root: Path = DEFAULT_CAMPAIGN_ROOT,
    output_root: Path = DEFAULT_CAMPAIGN_OUTPUT_ROOT,
    adapter_registry_path: Path = DEFAULT_STAGE_ADAPTER_REGISTRY_PATH,
) -> dict[str, Any]:
    contract_validation = validate_stage_adapter_contracts(
        adapter_registry_path=adapter_registry_path
    )
    parity_map = build_b2_campaign_adapter_parity_map(
        adapter_registry_path=adapter_registry_path
    )
    b2_parity = build_b2_campaign_parity_validation(campaign_root=campaign_root)
    budget = build_evidence_budget_enforcement_report(campaign_root=campaign_root)
    next_actions = build_campaign_next_action_parity_review(campaign_root=campaign_root)
    b2_compute_smoke = build_b2_compute_adapter_smoke_report(
        campaign_root=campaign_root,
        output_root=output_root,
        adapter_registry_path=adapter_registry_path,
    )
    b2_compute_parity = build_b2_compute_parity_validation(
        campaign_root=campaign_root,
        output_root=output_root,
        adapter_registry_path=adapter_registry_path,
        smoke_report=b2_compute_smoke,
    )
    run_next_smoke = build_campaign_run_next_stage_smoke_report(
        campaign_root=campaign_root,
        output_root=output_root,
        adapter_registry_path=adapter_registry_path,
    )
    forced_budget = build_evidence_budget_forced_transition_report(
        campaign_root=campaign_root,
        output_root=output_root,
        adapter_registry_path=adapter_registry_path,
    )
    b3_compute_smoke = build_b3_signal_compute_adapter_smoke_report(
        campaign_root=campaign_root,
        output_root=output_root,
        adapter_registry_path=adapter_registry_path,
    )
    status_ux = build_campaign_status_ux_report(campaign_root=campaign_root)
    deprecation_plan = build_case_specific_runner_deprecation_plan()
    checks = [
        {
            "check_id": "adapter_contract_validation",
            "passed": contract_validation["validation_status"] != "FAIL",
        },
        {"check_id": "b2_parity_map", "passed": parity_map["status"] == "PASS"},
        {"check_id": "b2_migration_parity", "passed": b2_parity["status"] == "PASS"},
        {"check_id": "evidence_budget_enforcement", "passed": budget["status"] == "PASS"},
        {"check_id": "next_action_planner", "passed": next_actions["status"] == "PASS"},
        {
            "check_id": "b2_compute_adapter_smoke",
            "passed": b2_compute_smoke["status"] == "B2_COMPUTE_ADAPTER_SMOKE_PASS",
        },
        {
            "check_id": "b2_compute_audited_parity",
            "passed": b2_compute_parity["status"]
            in {
                "B2_COMPUTE_PARITY_PASS",
                "B2_COMPUTE_PARITY_PASS_WITH_EXPLAINED_DIFFS",
            },
        },
        {
            "check_id": "campaign_run_next_stage_smoke",
            "passed": run_next_smoke["status"] == "CAMPAIGN_RUN_NEXT_STAGE_SMOKE_PASS",
        },
        {
            "check_id": "evidence_budget_forced_transition",
            "passed": forced_budget["status"] == "EVIDENCE_BUDGET_FORCED_TRANSITION_PASS",
        },
        {
            "check_id": "b3_signal_compute_adapter_smoke",
            "passed": b3_compute_smoke["status"] == "B3_SIGNAL_COMPUTE_ADAPTER_SMOKE_PASS",
        },
        {
            "check_id": "campaign_status_plan_ux",
            "passed": status_ux["status"] == "CAMPAIGN_STATUS_AND_PLAN_UX_PASS",
        },
        {
            "check_id": "case_specific_runner_deprecation_plan",
            "passed": deprecation_plan["status"]
            == "CASE_SPECIFIC_RUNNER_DEPRECATION_PLAN_READY",
        },
        {
            "check_id": "no_forbidden_production_effects",
            "passed": all(
                payload["production_effect"] == "none"
                for payload in (
                    parity_map,
                    b2_parity,
                    budget,
                    next_actions,
                    b2_compute_smoke,
                    b2_compute_parity,
                    run_next_smoke,
                    forced_budget,
                    b3_compute_smoke,
                    status_ux,
                    deprecation_plan,
                )
            ),
        },
    ]
    status = (
        "RESEARCH_CAMPAIGN_CONTROL_PLANE_V1_READY_WITH_LIMITATIONS"
        if all(check["passed"] for check in checks)
        else "RESEARCH_CAMPAIGN_CONTROL_PLANE_V1_BLOCKED"
    )
    return {
        "schema_version": "1.0",
        "report_type": "campaign_control_plane_v1_validation_pack",
        "release_candidate": "rc2",
        "status": status,
        "checks": checks,
        "limitations": [
            "B3 remains signal-precheck only.",
            "Some legacy task-specific CLI surfaces remain read-only compatibility inputs.",
            (
                "B2 compute adapter covers the control-window diagnostic smoke, "
                "not every legacy B2 runner."
            ),
            (
                "Old B2/B3 task-specific runners are not removed until owner review "
                "and compatibility coverage are complete."
            ),
        ]
        if status.endswith("READY_WITH_LIMITATIONS")
        else [],
        "component_statuses": {
            "adapter_contract_validation": contract_validation["validation_status"],
            "b2_parity_map": parity_map["status"],
            "b2_parity_validation": b2_parity["status"],
            "evidence_budget_enforcement": budget["status"],
            "next_action_parity_review": next_actions["status"],
            "b2_compute_adapter_smoke": b2_compute_smoke["status"],
            "b2_compute_parity_validation": b2_compute_parity["status"],
            "campaign_run_next_stage_smoke": run_next_smoke["status"],
            "evidence_budget_forced_transition": forced_budget["status"],
            "b3_signal_compute_adapter_smoke": b3_compute_smoke["status"],
            "campaign_status_plan_ux": status_ux["status"],
            "case_specific_runner_deprecation_plan": deprecation_plan["status"],
        },
        "safety_boundary": _adapter_safety_metadata(),
        "production_effect": "none",
    }


def write_campaign_control_plane_v1_validation_artifacts(
    *,
    campaign_root: Path = DEFAULT_CAMPAIGN_ROOT,
    output_root: Path = DEFAULT_CAMPAIGN_OUTPUT_ROOT,
    adapter_registry_path: Path = DEFAULT_STAGE_ADAPTER_REGISTRY_PATH,
) -> dict[str, Any]:
    output_dir = output_root / "control_plane_v1_rc2_validation"
    output_dir.mkdir(parents=True, exist_ok=True)
    b2_compute_smoke = build_b2_compute_adapter_smoke_report(
        campaign_root=campaign_root,
        output_root=output_root,
        adapter_registry_path=adapter_registry_path,
    )
    payloads = {
        "b2_campaign_adapter_parity_map": build_b2_campaign_adapter_parity_map(
            adapter_registry_path=adapter_registry_path
        ),
        "b2_campaign_parity_validation": build_b2_campaign_parity_validation(
            campaign_root=campaign_root
        ),
        "evidence_budget_enforcement_report": build_evidence_budget_enforcement_report(
            campaign_root=campaign_root
        ),
        "campaign_next_action_parity_review": build_campaign_next_action_parity_review(
            campaign_root=campaign_root
        ),
        "b2_compute_adapter_smoke": b2_compute_smoke,
        "b2_compute_parity_validation": build_b2_compute_parity_validation(
            campaign_root=campaign_root,
            output_root=output_root,
            adapter_registry_path=adapter_registry_path,
            smoke_report=b2_compute_smoke,
        ),
        "campaign_run_next_stage_smoke": build_campaign_run_next_stage_smoke_report(
            campaign_root=campaign_root,
            output_root=output_root,
            adapter_registry_path=adapter_registry_path,
        ),
        "evidence_budget_forced_transition_report": (
            build_evidence_budget_forced_transition_report(
                campaign_root=campaign_root,
                output_root=output_root,
                adapter_registry_path=adapter_registry_path,
            )
        ),
        "b3_signal_compute_adapter_smoke": build_b3_signal_compute_adapter_smoke_report(
            campaign_root=campaign_root,
            output_root=output_root,
            adapter_registry_path=adapter_registry_path,
        ),
        "campaign_status_plan_ux_report": build_campaign_status_ux_report(
            campaign_root=campaign_root
        ),
        "case_specific_runner_deprecation_plan": (
            build_case_specific_runner_deprecation_plan()
        ),
        "campaign_control_plane_v1_validation_pack": (
            build_campaign_control_plane_v1_validation_pack(
                campaign_root=campaign_root,
                output_root=output_root,
                adapter_registry_path=adapter_registry_path,
            )
        ),
    }
    written: dict[str, dict[str, str]] = {}
    for basename, payload in payloads.items():
        json_path = output_dir / f"{basename}.json"
        md_path = output_dir / f"{basename}.md"
        _write_json(json_path, payload)
        md_path.write_text(render_campaign_validation_markdown(payload), encoding="utf-8")
        written[basename] = {"json_path": str(json_path), "markdown_path": str(md_path)}
    return {
        "schema_version": "1.0",
        "report_type": "campaign_control_plane_v1_validation_artifacts",
        "status": payloads["campaign_control_plane_v1_validation_pack"]["status"],
        "artifacts": written,
        "production_effect": "none",
    }


def render_campaign_validation_markdown(payload: dict[str, Any]) -> str:
    lines = [
        f"# {str(payload['report_type']).replace('_', ' ').title()}",
        "",
        "## Reader Brief",
        "",
        f"- Status: {payload.get('status', payload.get('validation_status', 'UNKNOWN'))}",
        f"- Production Effect: {payload.get('production_effect', 'none')}",
    ]
    if payload.get("campaign_id"):
        lines.append(f"- Campaign: {payload['campaign_id']}")
    if payload.get("data_quality_status"):
        lines.append(f"- Data Quality: {payload['data_quality_status']}")
    lines.extend(["", "## Checks", ""])
    checks = payload.get("checks", [])
    if checks:
        for check in checks:
            lines.append(f"- {check['check_id']}: {check['passed']}")
    else:
        lines.append("- none")
    if payload.get("limitations"):
        lines.extend(["", "## Limitations", ""])
        lines.extend(f"- {item}" for item in payload["limitations"])
    if payload.get("issues"):
        lines.extend(["", "## Issues", ""])
        lines.extend(f"- {item}" for item in payload["issues"] or ["none"])
    lines.extend(["", "## Safety Boundary", ""])
    lines.append(
        json.dumps(payload.get("safety_boundary", {}), ensure_ascii=False, sort_keys=True)
    )
    return "\n".join(lines) + "\n"


def diagnose_campaign(
    *,
    campaign_id: str,
    campaign_root: Path = DEFAULT_CAMPAIGN_ROOT,
    gate_policy_path: Path = DEFAULT_GATE_POLICY_PATH,
) -> dict[str, Any]:
    spec, state, evidence = load_campaign_bundle(campaign_id, campaign_root)
    gate_policy = _scorecard_policy(spec.scorecard_policy, _read_yaml(gate_policy_path))
    required_categories = set(gate_policy.get("required_evidence_categories", []))
    by_category: dict[str, list[dict[str, Any]]] = {}
    for record in evidence:
        by_category.setdefault(record.category, []).append(record.model_dump(mode="json"))

    positive = [
        record.model_dump(mode="json")
        for record in evidence
        if record.direction == "positive" and record.status in {"PASS", "INFO", "WARNING"}
    ]
    negative = [
        record.model_dump(mode="json")
        for record in evidence
        if record.direction == "negative" or record.status in {"FAIL", "BLOCKED"}
    ]
    missing = sorted(required_categories - set(by_category))
    return {
        "schema_version": "1.0",
        "report_type": "research_campaign_diagnosis",
        "campaign_id": campaign_id,
        "current_stage": state.current_stage,
        "current_outcome": state.current_outcome,
        "evidence_matrix": by_category,
        "positive_evidence": positive,
        "negative_evidence": negative,
        "missing_required_evidence_categories": missing,
        "best_evidence": positive[:5],
        "worst_evidence": negative[:5],
        "data_quality_status": state.data_quality_status,
        "safety_boundary": state.safety_boundary,
    }


def evaluate_gate(
    *,
    spec: CampaignSpec,
    state: CampaignState,
    evidence: list[EvidenceRecord],
    gate_policy_path: Path = DEFAULT_GATE_POLICY_PATH,
) -> dict[str, Any]:
    gate_policy = _scorecard_policy(spec.scorecard_policy, _read_yaml(gate_policy_path))
    required_categories = set(gate_policy.get("required_evidence_categories", []))
    present_categories = {record.category for record in evidence}
    missing_categories = sorted(required_categories - present_categories)
    blocking_records = [
        record
        for record in evidence
        if record.status in {"FAIL", "BLOCKED"} or record.direction == "negative"
    ]
    reason_codes = sorted(
        {
            code
            for record in evidence
            for code in record.reason_codes
        }
        | set(state.reason_codes)
    )

    if missing_categories:
        decision = str(gate_policy.get("missing_evidence_outcome", "NEEDS_MORE_EVIDENCE"))
        reason_codes.extend(f"MISSING_{category}" for category in missing_categories)
    elif blocking_records:
        decision = _decision_from_reason_codes(
            reason_codes,
            gate_policy.get("blocking_reason_outcome_map", {}),
            default=str(gate_policy.get("blocking_evidence_outcome", "WEAK")),
        )
    elif any(record.direction == "mixed" or record.status == "MIXED" for record in evidence):
        decision = "MIXED"
    else:
        decision = str(gate_policy.get("pass_outcome", "PROMISING"))

    budget = evaluate_evidence_budget(spec, state)
    if decision == "NEEDS_MORE_EVIDENCE" and budget["budget_status"] == "EXHAUSTED":
        decision = str(gate_policy.get("over_budget_outcome", "OWNER_OVERRIDE_REQUIRED"))
        reason_codes.append("EVIDENCE_BUDGET_EXHAUSTED")

    return {
        "schema_version": "1.0",
        "report_type": "research_campaign_gate",
        "campaign_id": spec.campaign_id,
        "scorecard_policy": spec.scorecard_policy,
        "decision_outcome": decision,
        "reason_codes": sorted(set(reason_codes)),
        "required_evidence_categories": sorted(required_categories),
        "missing_required_evidence_categories": missing_categories,
        "blocking_evidence_ids": [record.evidence_id for record in blocking_records],
        "budget_status": budget["budget_status"],
        "allowed_decision_outcomes": (
            RESTRICTED_MORE_EVIDENCE_OUTCOMES
            if budget["budget_status"] == "EXHAUSTED"
            else DECISION_OUTCOMES
        ),
        "data_quality_status": state.data_quality_status,
        "safety_boundary": state.safety_boundary,
        "production_effect": "none",
    }


def evaluate_evidence_budget(spec: CampaignSpec, state: CampaignState) -> dict[str, Any]:
    used = state.evidence_budget_used
    limits = spec.evidence_budget
    exceeded = {
        "mini_rounds": used.mini_rounds >= limits.max_mini_rounds,
        "targeted_rounds": used.targeted_rounds >= limits.max_targeted_rounds,
        "window_expansions": used.window_expansions >= limits.max_window_expansions,
        "redesign_rounds": used.redesign_rounds >= limits.max_redesign_rounds,
        "needs_more_evidence_occurrences": (
            used.needs_more_evidence_occurrences >= limits.max_needs_more_evidence_occurrences
        ),
    }
    hard_exceeded = {
        key: value
        for key, value in exceeded.items()
        if value
        and key
        in {
            "targeted_rounds",
            "redesign_rounds",
            "needs_more_evidence_occurrences",
        }
    }
    proximity = {
        "mini_rounds_remaining": max(0, limits.max_mini_rounds - used.mini_rounds),
        "targeted_rounds_remaining": max(0, limits.max_targeted_rounds - used.targeted_rounds),
        "window_expansions_remaining": max(
            0,
            limits.max_window_expansions - used.window_expansions,
        ),
        "redesign_rounds_remaining": max(0, limits.max_redesign_rounds - used.redesign_rounds),
        "needs_more_evidence_remaining": max(
            0,
            limits.max_needs_more_evidence_occurrences
            - used.needs_more_evidence_occurrences,
        ),
    }
    return {
        "budget_status": "EXHAUSTED" if hard_exceeded else "AVAILABLE",
        "limits_exceeded": exceeded,
        "stop_rule_proximity": proximity,
        "restricted_outcomes_when_exhausted": RESTRICTED_MORE_EVIDENCE_OUTCOMES,
        "owner_override_required_after_budget": limits.owner_override_required_after_budget,
    }


def build_next_action_plan(
    *,
    spec: CampaignSpec,
    state: CampaignState,
    evidence: list[EvidenceRecord],
    gate_payload: dict[str, Any],
) -> dict[str, Any]:
    budget = evaluate_evidence_budget(spec, state)
    allowed: list[str] = []
    blocked: list[str] = []
    owner_required: list[str] = []
    reason_codes = set(state.reason_codes) | set(gate_payload.get("reason_codes", []))

    if (
        state.current_stage == "TARGETED_EVIDENCE"
        and "SLOW_DRAWDOWN_SINGLE_WINDOW_ONLY" in reason_codes
        and budget["stop_rule_proximity"]["targeted_rounds_remaining"] > 0
    ):
        allowed.append("COMPLETE_FINAL_REPEATABILITY_ROUND")
    if "FAST_RISK_NOT_SUPPORTED" in reason_codes:
        allowed.append("NARROW_ROLE")
    if "REENTRY_LAG_SIGNAL_DRIVEN" in reason_codes:
        allowed.append("RETURN_TO_DESIGN")
    if gate_payload["decision_outcome"] in {"PROMISING", "PASS"}:
        allowed.append("PREPARE_OWNER_PACKET")
    if (
        not allowed
        and gate_payload["decision_outcome"] == "NEEDS_MORE_EVIDENCE"
        and budget["budget_status"] != "EXHAUSTED"
    ):
        allowed.append("COLLECT_DEFINED_EVIDENCE_WITHIN_BUDGET")
    if "SIGNAL_DIRECTION_MIXED" in reason_codes or "B3_PRECHECK_MIXED" in reason_codes:
        allowed.append("CONTINUE_SIGNAL_DIRECTION_REDESIGN")
        allowed.append("RUN_SIGNAL_ONLY_PRECHECK_IF_HYPOTHESIS_CHANGES")

    if budget["limits_exceeded"]["mini_rounds"]:
        blocked.append("RUN_ADDITIONAL_MINI_DIAGNOSTIC_WITHOUT_OWNER_OVERRIDE")
    if budget["limits_exceeded"]["targeted_rounds"]:
        blocked.append("RUN_ADDITIONAL_TARGETED_EVIDENCE_WITHOUT_OWNER_OVERRIDE")
    if budget["limits_exceeded"]["window_expansions"]:
        blocked.append("RUN_ADDITIONAL_WINDOW_EXPANSION_WITHOUT_OWNER_OVERRIDE")
    if budget["limits_exceeded"]["redesign_rounds"]:
        blocked.append("RUN_ADDITIONAL_REDESIGN_ROUND_WITHOUT_OWNER_OVERRIDE")
    if budget["budget_status"] == "EXHAUSTED":
        blocked.append("EMIT_OPEN_ENDED_NEEDS_MORE_EVIDENCE")
        if budget["owner_override_required_after_budget"]:
            owner_required.append("OWNER_OVERRIDE_REQUIRED_FOR_MORE_EVIDENCE")
    if "SIGNAL_DIRECTION_MIXED" in reason_codes or "B3_PRECHECK_MIXED" in reason_codes:
        blocked.append("B3_MINI_BACKFILL")
    if not (state.current_stage == "GATE_READY" and spec.owner_authorized_holdout):
        blocked.append("ACCESS_UNTOUCHED_HOLDOUT")
        owner_required.append("FINAL_GATE_OWNER_AUTHORIZATION_REQUIRED_FOR_HOLDOUT")
    if "MISSING_INTERACTION_EFFECT" in reason_codes:
        blocked.append("PROMOTE_WITHOUT_INTERACTION_EVIDENCE")
    blocked.extend(ALWAYS_BLOCKED_RESEARCH_ACTIONS)

    next_stage = _recommended_next_stage(state.current_stage, allowed, blocked)
    return {
        "allowed_next_actions": sorted(set(allowed)),
        "blocked_actions": sorted(set(blocked)),
        "required_owner_actions": sorted(set(owner_required)),
        "next_recommended_stage": next_stage,
    }


def plan_experiment_matrix(
    *,
    spec: CampaignSpec,
    module_registry_path: Path = DEFAULT_MODULE_REGISTRY_PATH,
) -> dict[str, Any]:
    registry = load_module_registry(module_registry_path)
    approved_modules: list[str] = []
    blocked_modules: list[dict[str, str]] = []
    for module_id in spec.module_graph.modules:
        capability = registry[module_id]
        if capability.requires_gate_approval_for_interaction:
            blocked_modules.append(
                {
                    "module_id": module_id,
                    "reason": "REQUIRES_GATE_APPROVAL_FOR_INTERACTION",
                }
            )
            continue
        approved_modules.append(module_id)

    max_order = min(spec.module_graph.allowed_interaction_order, len(approved_modules))
    matrix: list[dict[str, Any]] = [
        {
            "experiment_id": spec.module_graph.baseline,
            "modules": [],
            "role": "baseline",
        }
    ]
    for order in range(1, max_order + 1):
        for combo in combinations(approved_modules, order):
            aliases = [_module_alias(registry[module_id], module_id) for module_id in combo]
            matrix.append(
                {
                    "experiment_id": f"{spec.module_graph.baseline}+{'+'.join(aliases)}",
                    "modules": list(combo),
                    "role": "main_effect" if order == 1 else f"{order}_way_interaction",
                }
            )

    return {
        "schema_version": "1.0",
        "report_type": "research_campaign_experiment_matrix",
        "campaign_id": spec.campaign_id,
        "baseline": spec.module_graph.baseline,
        "allowed_interaction_order": spec.module_graph.allowed_interaction_order,
        "experiment_matrix": matrix,
        "blocked_modules": blocked_modules,
        "effect_formulas": {
            "main_effect": "U(B0 + A) - U(B0)",
            "pair_interaction": "U(B0 + A + B) - U(B0 + A) - U(B0 + B) + U(B0)",
            "utility_definition": "portfolio utility from configured scorecard policy",
        },
        "production_effect": "none",
    }


def classify_interaction_effect(
    *,
    value: float,
    gate_policy_path: Path = DEFAULT_GATE_POLICY_PATH,
) -> str:
    policy = _read_yaml(gate_policy_path).get("interaction_effect_policy", {})
    thresholds = policy.get("classification_thresholds", {})
    positive_min = float(thresholds.get("positive_synergy_min", 0.02))
    negative_max = float(thresholds.get("negative_interference_max", -0.02))
    redundant_abs_max = float(thresholds.get("redundant_abs_max", 0.005))
    additive_abs_max = float(thresholds.get("mostly_additive_abs_max", 0.02))
    if value >= positive_min:
        return "POSITIVE_SYNERGY"
    if value <= negative_max:
        return "NEGATIVE_INTERFERENCE"
    if abs(value) <= redundant_abs_max:
        return "REDUNDANT"
    if abs(value) <= additive_abs_max:
        return "MOSTLY_ADDITIVE"
    return "INCONCLUSIVE"


def build_owner_packet(
    *,
    campaign_id: str,
    campaign_root: Path = DEFAULT_CAMPAIGN_ROOT,
    output_root: Path = DEFAULT_CAMPAIGN_OUTPUT_ROOT,
    gate_policy_path: Path = DEFAULT_GATE_POLICY_PATH,
) -> dict[str, Any]:
    spec, state, evidence = load_campaign_bundle(campaign_id, campaign_root)
    gate_payload = evaluate_gate(
        spec=spec,
        state=state,
        evidence=evidence,
        gate_policy_path=gate_policy_path,
    )
    plan = build_next_action_plan(
        spec=spec,
        state=state,
        evidence=evidence,
        gate_payload=gate_payload,
    )
    positive = [
        record.model_dump(mode="json")
        for record in evidence
        if record.direction == "positive" and record.status in {"PASS", "INFO", "WARNING"}
    ]
    negative = [
        record.model_dump(mode="json")
        for record in evidence
        if record.direction == "negative" or record.status in {"FAIL", "BLOCKED"}
    ]
    output_dir = output_root / campaign_id
    output_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": "1.0",
        "report_type": "research_campaign_owner_packet",
        "campaign_id": campaign_id,
        "decision": gate_payload["decision_outcome"],
        "current_stage": state.current_stage,
        "hypothesis": spec.hypothesis.model_dump(mode="json"),
        "what_was_tested": {
            "modules": spec.module_graph.modules,
            "baseline": spec.module_graph.baseline,
            "window_policy": spec.window_policy.model_dump(mode="json"),
        },
        "positive_evidence": positive,
        "negative_evidence": negative,
        "evidence_budget_used": state.evidence_budget_used.model_dump(mode="json"),
        "evidence_budget_limits": spec.evidence_budget.model_dump(mode="json"),
        "remaining_uncertainty": gate_payload["missing_required_evidence_categories"],
        "allowed_next_actions": plan["allowed_next_actions"],
        "blocked_actions": plan["blocked_actions"],
        "required_owner_actions": plan["required_owner_actions"],
        "stop_rules_approaching": evaluate_evidence_budget(spec, state)["stop_rule_proximity"],
        "safety_boundary": state.safety_boundary,
        "source_artifacts": state.source_artifacts,
        "owner_decision_appended": False,
        "data_quality_status": state.data_quality_status,
        "production_effect": "none",
    }
    json_path = output_dir / f"owner_packet_{campaign_id}.json"
    md_path = output_dir / f"owner_packet_{campaign_id}.md"
    _write_json(json_path, payload)
    md_path.write_text(render_owner_packet_markdown(payload), encoding="utf-8")
    return {
        **payload,
        "json_path": str(json_path),
        "markdown_path": str(md_path),
    }


def render_owner_packet_markdown(payload: dict[str, Any]) -> str:
    lines = [
        f"# Research Campaign Owner Packet: {payload['campaign_id']}",
        "",
        "## Reader Brief",
        "",
        f"- Decision: {payload['decision']}",
        f"- Current Stage: {payload['current_stage']}",
        f"- Data Quality: {payload['data_quality_status']}",
        "- Safety Boundary: "
        f"research_only={payload['safety_boundary']['research_only']}; "
        f"official_target_weights={payload['safety_boundary']['official_target_weights']}; "
        f"production_effect={payload['safety_boundary']['production_effect']}",
        "- Owner Decision Appended: false",
        "",
        "## Hypothesis",
        "",
        payload["hypothesis"]["statement"],
        "",
        "## What Was Tested",
        "",
        f"- Baseline: {payload['what_was_tested']['baseline']}",
        f"- Modules: {', '.join(payload['what_was_tested']['modules'])}",
        "",
        "## Positive Evidence",
        "",
    ]
    lines.extend(_evidence_lines(payload["positive_evidence"]))
    lines.extend(["", "## Negative Evidence", ""])
    lines.extend(_evidence_lines(payload["negative_evidence"]))
    lines.extend(
        [
            "",
            "## Evidence Budget Used",
            "",
            json.dumps(payload["evidence_budget_used"], ensure_ascii=False, sort_keys=True),
            "",
            "## Remaining Uncertainty",
            "",
        ]
    )
    lines.extend(f"- {item}" for item in payload["remaining_uncertainty"] or ["none"])
    lines.extend(["", "## Allowed Next Actions", ""])
    lines.extend(f"- {item}" for item in payload["allowed_next_actions"] or ["none"])
    lines.extend(["", "## Blocked Actions", ""])
    lines.extend(f"- {item}" for item in payload["blocked_actions"] or ["none"])
    lines.extend(["", "## Stop Rules Approaching", ""])
    for key, value in payload["stop_rules_approaching"].items():
        lines.append(f"- {key}: {value}")
    lines.extend(["", "## Source Artifacts", ""])
    lines.extend(f"- {item}" for item in payload["source_artifacts"] or ["none"])
    return "\n".join(lines) + "\n"


def archive_campaign(
    *,
    campaign_id: str,
    campaign_root: Path = DEFAULT_CAMPAIGN_ROOT,
    reason: str = "manual_archive",
) -> dict[str, Any]:
    directory = campaign_directory(campaign_id, campaign_root)
    spec, state, _ = load_campaign_bundle(campaign_id, campaign_root)
    state.current_stage = "ARCHIVED"
    state.current_outcome = (
        state.current_outcome
        if state.current_outcome != "NOT_EVALUATED"
        else "MIXED"
    )
    state.updated_at = _now_iso()
    state.stage_history.append(
        {
            "run_id": f"{campaign_id}-archive-{_compact_time()}",
            "stage": "ARCHIVED",
            "outcome": state.current_outcome,
            "reason_codes": [reason],
            "created_at": state.updated_at,
        }
    )
    write_campaign_state(state, directory)
    _append_transition(
        directory,
        {
            "transition_id": f"{campaign_id}-archive-{_compact_time()}",
            "campaign_id": campaign_id,
            "from_stage": None,
            "to_stage": "ARCHIVED",
            "outcome": state.current_outcome,
            "reason_codes": [reason],
            "created_at": state.updated_at,
            "source": "campaign_archive",
        },
    )
    return {
        "campaign_id": spec.campaign_id,
        "current_stage": state.current_stage,
        "current_outcome": state.current_outcome,
        "archive_reason": reason,
        "production_effect": "none",
    }


def build_reproducibility_manifest(
    *,
    spec_path: Path,
    spec: CampaignSpec,
    config_paths: list[Path],
    output_paths: list[Path],
    transition_source: str,
) -> dict[str, Any]:
    return {
        "schema_version": "1.0",
        "manifest_type": "research_campaign_reproducibility",
        "campaign_id": spec.campaign_id,
        "generated_at": _now_iso(),
        "git_commit": _git_commit(),
        "git_dirty": _git_dirty(),
        "market_regime": spec.market_regime,
        "requested_date_range": spec.requested_date_range,
        "spec": {
            "path": str(spec_path),
            "checksum": _checksum(spec_path),
        },
        "configs": [_path_record(path) for path in config_paths],
        "inputs": [],
        "windows": spec.window_policy.model_dump(mode="json"),
        "seed": None,
        "outputs": [_path_record(path) for path in output_paths],
        "transition_source": transition_source,
        "data_quality_status": CONTROL_ONLY_DATA_QUALITY_STATUS,
        "production_effect": "none",
    }


def validate_stage_transition(current_stage: str, next_stage: str) -> dict[str, Any]:
    if current_stage not in WORKFLOW_STAGES:
        return _transition(False, current_stage, next_stage, "UNKNOWN_CURRENT_STAGE")
    if next_stage not in WORKFLOW_STAGES:
        return _transition(False, current_stage, next_stage, "UNKNOWN_NEXT_STAGE")
    current_index = WORKFLOW_STAGES.index(current_stage)
    next_index = WORKFLOW_STAGES.index(next_stage)
    if next_stage == current_stage:
        return _transition(True, current_stage, next_stage, "same_stage_rerun")
    if next_stage == "ARCHIVED" and current_stage in {"OWNER_REVIEW", "GATE_READY"}:
        return _transition(True, current_stage, next_stage, "archive_after_review_or_gate")
    if next_index - current_index == 1:
        return _transition(True, current_stage, next_stage, "standard_next_stage")
    return _transition(False, current_stage, next_stage, "ILLEGAL_STAGE_SKIP")


def resolve_requested_stage(current_stage: str, requested_stage: str) -> str:
    normalized = requested_stage.upper().replace("-", "_")
    aliases = {
        "NEXT": _next_stage(current_stage),
        "SCOPE": "SCOPE_READY",
        "VALIDATE": "SCOPE_READY",
        "PRECHECK": "INPUT_PRECHECK",
        "INPUT": "INPUT_PRECHECK",
        "MINI": "MINI_DIAGNOSTIC",
        "TARGETED": "TARGETED_EVIDENCE",
        "FULL": "FULL_DIAGNOSTIC",
        "GATE": "GATE_READY",
        "OWNER": "OWNER_REVIEW",
        "PACKET": "OWNER_REVIEW",
    }
    return aliases.get(normalized, normalized)


def load_campaign_bundle(
    campaign_id: str,
    campaign_root: Path = DEFAULT_CAMPAIGN_ROOT,
) -> tuple[CampaignSpec, CampaignState, list[EvidenceRecord]]:
    directory = campaign_directory(campaign_id, campaign_root)
    if not directory.exists():
        raise ResearchCampaignError(f"Campaign not found: {campaign_id}")
    spec = CampaignSpec.model_validate(_read_json(directory / "spec.json"))
    state = CampaignState.model_validate(_read_json(directory / "state.json"))
    evidence = read_evidence_records(directory)
    return spec, state, evidence


def load_module_registry(path: Path = DEFAULT_MODULE_REGISTRY_PATH) -> dict[str, ModuleCapability]:
    raw = _read_yaml(path)
    modules = raw.get("modules", {})
    registry: dict[str, ModuleCapability] = {}
    for module_id, payload in modules.items():
        registry[module_id] = ModuleCapability.model_validate({"module_id": module_id, **payload})
    return registry


def load_campaign_migrations(path: Path = DEFAULT_MIGRATION_PATH) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    return dict(_read_yaml(path).get("migrations", {}))


def load_compatibility_policy(path: Path = DEFAULT_COMPATIBILITY_PATH) -> dict[str, Any]:
    if not path.exists():
        return {
            "status": "COMPATIBILITY_POLICY_MISSING",
            "deprecated_task_specific_runner_count": 0,
        }
    payload = _read_yaml(path)
    runners = payload.get("deprecated_task_specific_runners", [])
    return {
        "status": payload.get("status", "COMPATIBILITY_POLICY_READY"),
        "deprecated_task_specific_runner_count": len(runners),
        "deprecated_task_specific_runners": runners,
    }


def load_stage_adapter_contracts(
    path: Path = DEFAULT_STAGE_ADAPTER_REGISTRY_PATH,
) -> dict[str, StageAdapterContract]:
    if not path.exists():
        return {}
    raw = _read_yaml(path)
    contracts: dict[str, StageAdapterContract] = {}
    for adapter_id, payload in raw.get("adapters", {}).items():
        contract_payload = dict(payload)
        contract_payload.setdefault("adapter_id", adapter_id)
        contracts[adapter_id] = StageAdapterContract.model_validate(contract_payload)
    return contracts


def validate_stage_adapter_contracts(
    *,
    adapter_registry_path: Path = DEFAULT_STAGE_ADAPTER_REGISTRY_PATH,
    module_registry_path: Path = DEFAULT_MODULE_REGISTRY_PATH,
) -> dict[str, Any]:
    issues: list[dict[str, Any]] = []
    try:
        contracts = load_stage_adapter_contracts(adapter_registry_path)
    except ValidationError as exc:
        return {
            "schema_version": "1.0",
            "report_type": "research_campaign_stage_adapter_contract_validation",
            "validation_status": "FAIL",
            "status": "FAIL",
            "issues": [
                _issue(
                    "error",
                    "adapter_contract_schema_invalid",
                    f"Stage adapter registry schema invalid: {exc}",
                )
            ],
            "adapter_count": 0,
            "supported_stages": ADAPTER_SUPPORTED_STAGES,
            "output_statuses": ADAPTER_OUTPUT_STATUSES,
            "run_modes": ADAPTER_RUN_MODES,
            "safety_boundary": _adapter_safety_metadata(),
            "production_effect": "none",
        }
    registry = load_module_registry(module_registry_path)
    if not contracts:
        issues.append(
            _issue(
                "error",
                "stage_adapter_registry_empty",
                f"No stage adapters configured at {adapter_registry_path}",
            )
        )
    for contract in contracts.values():
        unknown_stages = sorted(set(contract.supported_stage) - set(ADAPTER_SUPPORTED_STAGES))
        if unknown_stages:
            issues.append(
                _issue(
                    "error",
                    "adapter_unknown_supported_stage",
                    f"{contract.adapter_id} has unknown stages: {unknown_stages}",
                )
            )
        unknown_modules = sorted(set(contract.supported_module) - set(registry))
        if unknown_modules:
            issues.append(
                _issue(
                    "error",
                    "adapter_unknown_supported_module",
                    f"{contract.adapter_id} references unknown modules: {unknown_modules}",
                )
            )
        unknown_categories = sorted(
            set(contract.produced_evidence_categories) - set(EVIDENCE_CATEGORIES)
        )
        if unknown_categories:
            issues.append(
                _issue(
                    "error",
                    "adapter_unknown_evidence_category",
                    f"{contract.adapter_id} emits unknown evidence categories: "
                    f"{unknown_categories}",
                )
            )
        safety_issues = _adapter_safety_issues(contract.safety_boundary, contract.adapter_id)
        issues.extend(safety_issues)
        for artifact_name in contract.produced_artifacts:
            normalized = artifact_name.lower().replace("-", "_")
            if any(token in normalized for token in FORBIDDEN_WEIGHT_OUTPUTS):
                issues.append(
                    _issue(
                        "error",
                        "adapter_forbidden_output_name",
                        f"{contract.adapter_id} produced artifact has forbidden semantics: "
                        f"{artifact_name}",
                    )
                )
        for input_spec in contract.required_inputs:
            path = _resolve_project_path(input_spec.path)
            if input_spec.required and not path.exists():
                issues.append(
                    _issue(
                        "error",
                        "adapter_required_input_missing",
                        f"{contract.adapter_id} required input missing: {input_spec.path}",
                    )
                )
    status = "PASS" if not [issue for issue in issues if issue["severity"] == "error"] else "FAIL"
    warning_count = len([issue for issue in issues if issue["severity"] == "warning"])
    if status == "PASS" and warning_count:
        status = "PASS_WITH_WARNINGS"
    return {
        "schema_version": "1.0",
        "report_type": "research_campaign_stage_adapter_contract_validation",
        "validation_status": status,
        "status": status,
        "adapter_count": len(contracts),
        "adapter_ids": sorted(contracts),
        "supported_stages": ADAPTER_SUPPORTED_STAGES,
        "output_statuses": ADAPTER_OUTPUT_STATUSES,
        "run_modes": ADAPTER_RUN_MODES,
        "adapter_run_modes": {
            adapter_id: contract.default_run_mode
            for adapter_id, contract in sorted(contracts.items())
        },
        "issues": issues,
        "safety_boundary": _adapter_safety_metadata(),
        "production_effect": "none",
    }


def run_stage_adapter(
    *,
    spec: CampaignSpec,
    state: CampaignState,
    target_stage: str,
    run_id: str,
    adapter_registry_path: Path = DEFAULT_STAGE_ADAPTER_REGISTRY_PATH,
    module_registry_path: Path = DEFAULT_MODULE_REGISTRY_PATH,
    gate_policy_path: Path = DEFAULT_GATE_POLICY_PATH,
    window_policy_path: Path = DEFAULT_WINDOW_POLICY_PATH,
    output_root: Path = DEFAULT_CAMPAIGN_OUTPUT_ROOT,
) -> StageAdapterRunOutput:
    adapter_stage = _adapter_stage_for_workflow_stage(
        spec=spec,
        workflow_stage=target_stage,
        module_registry_path=module_registry_path,
    )
    contracts = load_stage_adapter_contracts(adapter_registry_path)
    contract = _find_stage_adapter_contract(spec, adapter_stage, contracts)
    if contract is None:
        return _adapter_blocked_output(
            run_id=run_id,
            campaign_id=spec.campaign_id,
            stage=adapter_stage,
            status="ADAPTER_NOT_CONFIGURED",
            reason_codes=["STAGE_ADAPTER_NOT_CONFIGURED"],
            run_mode="VALIDATION_ONLY_MODE",
        )

    safety_issues = _adapter_safety_issues(contract.safety_boundary, contract.adapter_id)
    if safety_issues:
        return _adapter_blocked_output(
            run_id=run_id,
            campaign_id=spec.campaign_id,
            stage=adapter_stage,
            adapter_id=contract.adapter_id,
            adapter_version=contract.adapter_version,
            status="ADAPTER_SAFETY_BLOCKED",
            reason_codes=[issue["issue_id"].upper() for issue in safety_issues],
            run_mode=contract.default_run_mode,
        )

    inputs = [
        input_spec
        for input_spec in contract.required_inputs
        if _adapter_input_applies(input_spec, adapter_stage)
    ]
    source_payloads: dict[str, dict[str, Any]] = {}
    input_artifacts: list[dict[str, Any]] = []
    missing_inputs: list[str] = []
    invalid_inputs: list[str] = []
    for input_spec in inputs:
        path = _resolve_project_path(input_spec.path)
        artifact_record = {
            "artifact_id": input_spec.artifact_id,
            "path": str(path),
            "required": input_spec.required,
            "exists": path.exists(),
            "checksum": _checksum(path),
        }
        input_artifacts.append(artifact_record)
        if not path.exists():
            if input_spec.required:
                missing_inputs.append(input_spec.artifact_id)
            continue
        try:
            source_payloads[input_spec.artifact_id] = _read_json(path)
        except ResearchCampaignError:
            invalid_inputs.append(input_spec.artifact_id)

    if missing_inputs:
        return _adapter_blocked_output(
            run_id=run_id,
            campaign_id=spec.campaign_id,
            stage=adapter_stage,
            adapter_id=contract.adapter_id,
            adapter_version=contract.adapter_version,
            input_artifacts=input_artifacts,
            status="ADAPTER_INPUT_MISSING",
            reason_codes=["STAGE_ADAPTER_INPUT_MISSING", *missing_inputs],
            run_mode=contract.default_run_mode,
        )
    if invalid_inputs:
        return _adapter_blocked_output(
            run_id=run_id,
            campaign_id=spec.campaign_id,
            stage=adapter_stage,
            adapter_id=contract.adapter_id,
            adapter_version=contract.adapter_version,
            input_artifacts=input_artifacts,
            status="ADAPTER_OUTPUT_INVALID",
            reason_codes=["STAGE_ADAPTER_OUTPUT_INVALID", *invalid_inputs],
            run_mode=contract.default_run_mode,
        )

    source_issues = _adapter_source_artifact_issues(source_payloads)
    if any(issue["issue_id"] == "adapter_source_holdout_accessed" for issue in source_issues):
        return _adapter_blocked_output(
            run_id=run_id,
            campaign_id=spec.campaign_id,
            stage=adapter_stage,
            adapter_id=contract.adapter_id,
            adapter_version=contract.adapter_version,
            input_artifacts=input_artifacts,
            status="ADAPTER_HOLDOUT_BLOCKED",
            reason_codes=[issue["issue_id"].upper() for issue in source_issues],
            run_mode=contract.default_run_mode,
        )
    if source_issues:
        return _adapter_blocked_output(
            run_id=run_id,
            campaign_id=spec.campaign_id,
            stage=adapter_stage,
            adapter_id=contract.adapter_id,
            adapter_version=contract.adapter_version,
            input_artifacts=input_artifacts,
            status="ADAPTER_OUTPUT_INVALID",
            reason_codes=[issue["issue_id"].upper() for issue in source_issues],
            run_mode=contract.default_run_mode,
        )

    if contract.default_run_mode == "COMPUTE_MODE":
        return _run_compute_stage_adapter(
            spec=spec,
            state=state,
            target_stage=target_stage,
            adapter_stage=adapter_stage,
            run_id=run_id,
            contract=contract,
            input_artifacts=input_artifacts,
            source_payloads=source_payloads,
            module_registry_path=module_registry_path,
            gate_policy_path=gate_policy_path,
            window_policy_path=window_policy_path,
            output_root=output_root,
        )

    evidence_records: list[dict[str, Any]] = []
    reason_codes: set[str] = set()
    for mapping in contract.evidence_mappings:
        if mapping.stage != adapter_stage:
            continue
        payload = source_payloads.get(mapping.artifact_id)
        if payload is None:
            return _adapter_blocked_output(
                run_id=run_id,
                campaign_id=spec.campaign_id,
                stage=adapter_stage,
                adapter_id=contract.adapter_id,
                adapter_version=contract.adapter_version,
                input_artifacts=input_artifacts,
                status="ADAPTER_OUTPUT_INVALID",
                reason_codes=[
                    "STAGE_ADAPTER_OUTPUT_INVALID",
                    f"MISSING_MAPPING_INPUT_{mapping.artifact_id}",
                ],
                run_mode=contract.default_run_mode,
            )
        value = _extract_value(payload, mapping.value_path)
        if value is None:
            return _adapter_blocked_output(
                run_id=run_id,
                campaign_id=spec.campaign_id,
                stage=adapter_stage,
                adapter_id=contract.adapter_id,
                adapter_version=contract.adapter_version,
                input_artifacts=input_artifacts,
                status="ADAPTER_OUTPUT_INVALID",
                reason_codes=[
                    "STAGE_ADAPTER_OUTPUT_INVALID",
                    f"MISSING_VALUE_{mapping.artifact_id}_{mapping.value_path}",
                ],
                run_mode=contract.default_run_mode,
            )
        record = EvidenceRecord(
            evidence_id=f"{run_id}-{mapping.evidence_id_suffix}",
            campaign_id=spec.campaign_id,
            run_id=run_id,
            stage=adapter_stage,
            category=mapping.category,
            metric_name=mapping.metric_name,
            value=value,
            direction=mapping.direction,
            window_id=mapping.window_id,
            status=mapping.status,
            confidence=mapping.confidence,
            source_artifact_id=mapping.artifact_id,
            reason_codes=mapping.reason_codes,
        )
        reason_codes.update(mapping.reason_codes)
        evidence_records.append(record.model_dump(mode="json"))

    adapter_outcome = contract.stage_outcomes.get(
        adapter_stage,
        _outcome_from_adapter_evidence(evidence_records, state),
    )
    data_quality_status = _adapter_data_quality_status(source_payloads)
    return StageAdapterRunOutput(
        run_id=run_id,
        campaign_id=spec.campaign_id,
        stage=adapter_stage,
        adapter_id=contract.adapter_id,
        run_mode=contract.default_run_mode,
        adapter_version=contract.adapter_version,
        input_artifacts=input_artifacts,
        output_artifacts=[
            {
                "artifact_id": artifact_name,
                "status": "planned",
            }
            for artifact_name in contract.produced_artifacts
        ],
        compute_performed=False,
        imported_evidence=True,
        parity_source=contract.parity_source,
        failure_mode=None,
        evidence_records=evidence_records,
        status="ADAPTER_READY",
        reason_codes=sorted(reason_codes | set(state.reason_codes)),
        safety_metadata=_adapter_safety_metadata(),
        adapter_outcome=adapter_outcome,
        data_quality_status=data_quality_status,
    )


def write_campaign_state(state: CampaignState, directory: Path) -> None:
    _write_json(directory / "state.json", state.model_dump(mode="json"))


def read_evidence_records(directory: Path) -> list[EvidenceRecord]:
    path = directory / "evidence.jsonl"
    if not path.exists():
        return []
    records = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            records.append(EvidenceRecord.model_validate(json.loads(line)))
    return records


def write_evidence_records(
    records: list[EvidenceRecord],
    directory: Path,
    *,
    replace: bool = False,
) -> None:
    path = directory / "evidence.jsonl"
    if replace:
        path.write_text("", encoding="utf-8")
    append_evidence_records(records, directory)


def append_evidence_records(records: list[EvidenceRecord], directory: Path) -> None:
    for record in records:
        _append_jsonl(directory / "evidence.jsonl", record.model_dump(mode="json"))


def campaign_directory(campaign_id: str, campaign_root: Path = DEFAULT_CAMPAIGN_ROOT) -> Path:
    if "/" in campaign_id or "\\" in campaign_id or ".." in campaign_id:
        raise ResearchCampaignError(f"Unsafe campaign_id: {campaign_id}")
    return campaign_root / campaign_id


def _initial_state_from_spec(spec: CampaignSpec, migration: dict[str, Any] | None) -> CampaignState:
    if not migration:
        return CampaignState(
            campaign_id=spec.campaign_id,
            program_id=spec.program_id,
            title=spec.title,
            source_artifacts=[],
        )
    budget = BudgetUsed.model_validate(migration.get("evidence_budget_used", {}))
    return CampaignState(
        campaign_id=spec.campaign_id,
        program_id=spec.program_id,
        title=spec.title,
        current_stage=str(migration.get("stage", "DRAFT")),
        current_outcome=str(migration.get("outcome", "NOT_EVALUATED")),
        reason_codes=list(migration.get("reason_codes", [])),
        evidence_budget_used=budget,
        source_artifacts=list(migration.get("source_artifacts", [])),
        migrated_from_legacy=True,
        legacy_status=migration.get("legacy_status"),
        stage_history=list(migration.get("stage_history", [])),
    )


def _validate_safety(spec: CampaignSpec) -> list[dict[str, Any]]:
    safety = spec.safety
    issues: list[dict[str, Any]] = []
    checks = {
        "research_only": safety.research_only is True,
        "manual_review_only": safety.manual_review_only is True,
        "official_target_weights": safety.official_target_weights is False,
        "paper_shadow_allowed": safety.paper_shadow_allowed is False,
        "broker_effect": safety.broker_effect == "none",
        "order_effect": safety.order_effect == "none",
        "production_effect": safety.production_effect == "none",
    }
    for field_name, passed in checks.items():
        if not passed:
            issues.append(
                _issue(
                    "error",
                    f"safety_{field_name}_invalid",
                    f"safety.{field_name} violates research-only boundary",
                )
            )
    return issues


def _validate_module_graph(
    spec: CampaignSpec,
    registry: dict[str, ModuleCapability],
) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    all_module_ids = [spec.module_graph.baseline, *spec.module_graph.modules]
    for module_id in all_module_ids:
        if module_id not in registry:
            issues.append(_issue("error", "unknown_module", f"Unknown module: {module_id}"))
    if issues:
        return issues

    selected = [registry[module_id] for module_id in spec.module_graph.modules]
    for capability in selected:
        if not capability.approved_for_campaign_modules:
            issues.append(
                _issue(
                    "error",
                    "module_not_approved_for_campaign",
                    f"{capability.module_id} cannot be used as a campaign module",
                )
            )
        if capability.module_type in {"P0_MIXED_ALLOCATOR", "MIXED_ALLOCATOR"}:
            issues.append(
                _issue(
                    "error",
                    "p0_mixed_allocator_not_single_module",
                    f"{capability.module_id} cannot masquerade as a single module",
                )
            )
        output_set = set(capability.output_contract)
        if capability.module_type == "SIGNAL" and output_set & FORBIDDEN_WEIGHT_OUTPUTS:
            issues.append(
                _issue(
                    "error",
                    "signal_module_outputs_weight",
                    f"{capability.module_id} output_contract contains weight output",
                )
            )
        if capability.module_type == "EVALUATOR" and output_set & FORBIDDEN_EVALUATION_MUTATIONS:
            issues.append(
                _issue(
                    "error",
                    "evaluation_module_mutates_strategy",
                    f"{capability.module_id} output_contract contains strategy mutation output",
                )
            )
        mechanism_overlap = set(capability.allowed_mechanisms) - set(
            spec.module_graph.allowed_mechanisms
        )
        if mechanism_overlap:
            issues.append(
                _issue(
                    "error",
                    "module_uses_unallowed_mechanism",
                    f"{capability.module_id} mechanisms not allowed by campaign: "
                    f"{sorted(mechanism_overlap)}",
                )
            )
        forbidden_overlap = set(capability.allowed_mechanisms) & set(
            spec.module_graph.forbidden_mechanisms
        )
        if forbidden_overlap:
            issues.append(
                _issue(
                    "error",
                    "module_uses_forbidden_mechanism",
                    f"{capability.module_id} mechanisms forbidden by campaign: "
                    f"{sorted(forbidden_overlap)}",
                )
            )
    selected_ids = set(spec.module_graph.modules)
    for capability in selected:
        incompatible = selected_ids & set(capability.incompatible_modules)
        if incompatible:
            issues.append(
                _issue(
                    "error",
                    "incompatible_module_combination",
                    f"{capability.module_id} incompatible with {sorted(incompatible)}",
                )
            )
    return issues


def _validate_window_policy(spec: CampaignSpec, policy: dict[str, Any]) -> list[dict[str, Any]]:
    catalogs = policy.get("catalogs", {})
    issues: list[dict[str, Any]] = []
    for field_name in ("development_catalog", "diagnostic_catalog", "holdout_catalog"):
        catalog_id = getattr(spec.window_policy, field_name)
        if catalog_id not in catalogs:
            issues.append(
                _issue(
                    "error",
                    "unknown_window_catalog",
                    f"Unknown {field_name}: {catalog_id}",
                )
            )
    if issues:
        return issues
    holdout = catalogs[spec.window_policy.holdout_catalog]
    if not holdout.get("holdout", False):
        issues.append(
            _issue(
                "error",
                "holdout_catalog_not_marked_holdout",
                f"{spec.window_policy.holdout_catalog} must be holdout=true",
            )
        )
    if (
        spec.window_policy.holdout_access != "FINAL_GATE_ONLY"
        and not spec.owner_authorized_holdout
    ):
        issues.append(
            _issue(
                "error",
                "holdout_access_requires_owner_authorization",
                "Holdout access beyond FINAL_GATE_ONLY requires owner_authorized_holdout=true",
            )
        )
    if spec.owner_authorized_holdout and spec.window_policy.holdout_access == "FORBIDDEN":
        issues.append(
            _issue("error", "holdout_forbidden", "Campaign forbids holdout access")
        )
    return issues


def _validate_gate_policy(spec: CampaignSpec, gate_policy: dict[str, Any]) -> list[dict[str, Any]]:
    scorecards = gate_policy.get("scorecards", {})
    if spec.scorecard_policy not in scorecards:
        return [
            _issue(
                "error",
                "unknown_scorecard_policy",
                f"Unknown scorecard policy: {spec.scorecard_policy}",
            )
        ]
    scorecard = scorecards[spec.scorecard_policy]
    metadata = scorecard.get("policy_metadata", {})
    missing = [
        key
        for key in (
            "owner",
            "status",
            "rationale",
            "intended_effect",
            "validation_evidence",
            "review_condition",
        )
        if not metadata.get(key)
    ]
    if missing:
        return [
            _issue(
                "error",
                "scorecard_policy_metadata_incomplete",
                f"Scorecard metadata missing: {missing}",
            )
        ]
    return []


def _validate_budget(spec: CampaignSpec) -> list[dict[str, Any]]:
    budget = spec.evidence_budget
    if budget.max_needs_more_evidence_occurrences == 0:
        return [
            _issue(
                "warning",
                "needs_more_evidence_budget_zero",
                "Campaign cannot emit NEEDS_MORE_EVIDENCE without immediate restricted decision",
            )
        ]
    return []


def _scorecard_policy(scorecard_policy: str, raw_policy: dict[str, Any]) -> dict[str, Any]:
    scorecards = raw_policy.get("scorecards", {})
    if scorecard_policy not in scorecards:
        raise ResearchCampaignError(f"Unknown scorecard policy: {scorecard_policy}")
    return dict(scorecards[scorecard_policy])


def _decision_from_reason_codes(
    reason_codes: list[str],
    mapping: dict[str, str],
    *,
    default: str,
) -> str:
    for reason_code in reason_codes:
        if reason_code in mapping:
            return str(mapping[reason_code])
    return default


def _adapter_safety_metadata() -> dict[str, Any]:
    return {
        "research_only": True,
        "manual_review_only": True,
        "official_target_weights": False,
        "paper_shadow_activation": False,
        "paper_shadow_allowed": False,
        "broker_effect": "none",
        "order_effect": "none",
        "production_effect": "none",
        "holdout_touched": False,
    }


def _adapter_safety_issues(payload: dict[str, Any], adapter_id: str) -> list[dict[str, Any]]:
    required = _adapter_safety_metadata()
    issues: list[dict[str, Any]] = []
    for key, expected in required.items():
        if payload.get(key) != expected:
            issues.append(
                _issue(
                    "error",
                    f"adapter_safety_{key}_invalid",
                    f"{adapter_id} safety boundary {key}={payload.get(key)!r}, "
                    f"expected {expected!r}",
                )
            )
    return issues


def _run_compute_stage_adapter(
    *,
    spec: CampaignSpec,
    state: CampaignState,
    target_stage: str,
    adapter_stage: str,
    run_id: str,
    contract: StageAdapterContract,
    input_artifacts: list[dict[str, Any]],
    source_payloads: dict[str, dict[str, Any]],
    module_registry_path: Path,
    gate_policy_path: Path,
    window_policy_path: Path,
    output_root: Path,
) -> StageAdapterRunOutput:
    validation = build_campaign_validation_payload(
        spec=spec,
        module_registry_path=module_registry_path,
        gate_policy_path=gate_policy_path,
        window_policy_path=window_policy_path,
    )
    if validation["validation_status"] == "FAIL":
        reason_codes = [
            str(issue["issue_id"]).upper()
            for issue in validation["issues"]
            if issue["severity"] == "error"
        ]
        return _adapter_blocked_output(
            run_id=run_id,
            campaign_id=spec.campaign_id,
            stage=adapter_stage,
            adapter_id=contract.adapter_id,
            adapter_version=contract.adapter_version,
            input_artifacts=input_artifacts,
            status="ADAPTER_SAFETY_BLOCKED",
            reason_codes=["CAMPAIGN_VALIDATION_FAILED", *reason_codes],
            run_mode=contract.default_run_mode,
        )
    if contract.adapter_id == "b2-risk-overlay-control-window-compute-adapter-v1":
        return _run_b2_control_window_compute_adapter(
            spec=spec,
            state=state,
            target_stage=target_stage,
            adapter_stage=adapter_stage,
            run_id=run_id,
            contract=contract,
            input_artifacts=input_artifacts,
            output_root=output_root,
        )
    if contract.adapter_id == "b3-signal-precheck-compute-adapter-v1":
        return _run_b3_signal_compute_adapter(
            spec=spec,
            state=state,
            adapter_stage=adapter_stage,
            run_id=run_id,
            contract=contract,
            input_artifacts=input_artifacts,
            source_payloads=source_payloads,
            output_root=output_root,
        )
    return _adapter_blocked_output(
        run_id=run_id,
        campaign_id=spec.campaign_id,
        stage=adapter_stage,
        adapter_id=contract.adapter_id,
        adapter_version=contract.adapter_version,
        input_artifacts=input_artifacts,
        status="ADAPTER_NOT_CONFIGURED",
        reason_codes=["COMPUTE_ADAPTER_DISPATCH_NOT_IMPLEMENTED"],
        run_mode=contract.default_run_mode,
    )


def _run_b2_control_window_compute_adapter(
    *,
    spec: CampaignSpec,
    state: CampaignState,
    target_stage: str,
    adapter_stage: str,
    run_id: str,
    contract: StageAdapterContract,
    input_artifacts: list[dict[str, Any]],
    output_root: Path,
) -> StageAdapterRunOutput:
    from ai_trading_system.etf_portfolio.weight_research_b2_control_windows import (
        run_b2_control_window_research,
    )

    compute_dir = output_root / spec.campaign_id / "b2_compute"
    try:
        payloads, paths = run_b2_control_window_research(
            output_dir=compute_dir,
            alias_dir=None,
            generated_at=datetime.now(UTC),
        )
    except Exception as exc:  # pragma: no cover - exercised through blocked status in CLI use.
        return _adapter_blocked_output(
            run_id=run_id,
            campaign_id=spec.campaign_id,
            stage=adapter_stage,
            adapter_id=contract.adapter_id,
            adapter_version=contract.adapter_version,
            input_artifacts=input_artifacts,
            status="B2_COMPUTE_ADAPTER_BLOCKED_WITH_REASON",
            reason_codes=["B2_COMPUTE_ADAPTER_EXCEPTION", type(exc).__name__],
            run_mode=contract.default_run_mode,
        )

    source_issues = _adapter_source_artifact_issues(payloads)
    if source_issues:
        status = (
            "ADAPTER_HOLDOUT_BLOCKED"
            if any(
                issue["issue_id"] == "adapter_source_holdout_accessed"
                for issue in source_issues
            )
            else "B2_COMPUTE_ADAPTER_BLOCKED_WITH_REASON"
        )
        return _adapter_blocked_output(
            run_id=run_id,
            campaign_id=spec.campaign_id,
            stage=adapter_stage,
            adapter_id=contract.adapter_id,
            adapter_version=contract.adapter_version,
            input_artifacts=input_artifacts,
            status=status,
            reason_codes=[issue["issue_id"].upper() for issue in source_issues],
            run_mode=contract.default_run_mode,
        )

    rerun = payloads["b2_control_window_rerun"]
    no_trigger = payloads["b2_no_trigger_correctness_review"]
    safe_compute = (
        rerun.get("b2_logic_only") is True
        and rerun.get("B3_used") is False
        and rerun.get("B4_B5_B6_v3_used") is False
        and rerun.get("untouched_holdout_used") is False
    )
    smoke_pass = (
        safe_compute
        and rerun.get("status") == "B2_CONTROL_RERUN_COMPLETE"
        and no_trigger.get("status") == "B2_NO_TRIGGER_CORRECTNESS_PASS"
    )
    status = (
        "B2_COMPUTE_ADAPTER_SMOKE_PASS"
        if smoke_pass
        else "B2_COMPUTE_ADAPTER_BLOCKED_WITH_REASON"
    )
    reason_codes = {
        status,
        *state.reason_codes,
        "CONTROL_BEHAVIOR_CLEAN" if smoke_pass else "B2_CONTROL_COMPUTE_REVIEW_REQUIRED",
    }
    output_artifacts = _compute_output_artifacts(paths, payloads)
    evidence_records = _b2_compute_evidence_records(
        run_id=run_id,
        campaign_id=spec.campaign_id,
        stage=adapter_stage,
        rerun=rerun,
        no_trigger=no_trigger,
        smoke_pass=smoke_pass,
    )
    return StageAdapterRunOutput(
        run_id=run_id,
        campaign_id=spec.campaign_id,
        stage=adapter_stage,
        adapter_id=contract.adapter_id,
        run_mode=contract.default_run_mode,
        adapter_version=contract.adapter_version,
        input_artifacts=input_artifacts,
        output_artifacts=output_artifacts,
        evidence_records=[record.model_dump(mode="json") for record in evidence_records],
        compute_performed=True,
        imported_evidence=False,
        parity_source=contract.parity_source,
        failure_mode=None if smoke_pass else status,
        status=status,
        reason_codes=sorted(reason_codes),
        safety_metadata=_adapter_safety_metadata(),
        adapter_outcome=contract.stage_outcomes.get(
            adapter_stage,
            "NEEDS_MORE_EVIDENCE" if smoke_pass else "BLOCKED",
        ),
        data_quality_status=_adapter_data_quality_status(payloads),
    )


def _run_b3_signal_compute_adapter(
    *,
    spec: CampaignSpec,
    state: CampaignState,
    adapter_stage: str,
    run_id: str,
    contract: StageAdapterContract,
    input_artifacts: list[dict[str, Any]],
    source_payloads: dict[str, dict[str, Any]],
    output_root: Path,
) -> StageAdapterRunOutput:
    from ai_trading_system.etf_portfolio.weight_research_b2_b3_v2 import (
        build_b3_redesign_candidate_precheck_v2,
        build_b3_signal_direction_failure_taxonomy,
        render_b2_b3_v2_payload,
    )

    sources = {
        "b3_precheck": source_payloads["b3_signal_direction_precheck"],
        "b3_audit": source_payloads["b3_slow_tilt_signal_direction_audit"],
        "b3_ranking": source_payloads["b3_redesign_hypothesis_ranking"],
        "final_branch": source_payloads["final_branch_decision_snapshot"],
    }
    generated = datetime.now(UTC)
    data_quality = _control_only_quality_gate()
    requested_range = _requested_range_from_source(
        sources["final_branch"],
        default_source="final_branch_decision_snapshot",
    )
    taxonomy = build_b3_signal_direction_failure_taxonomy(
        sources=sources,
        generated_at=generated,
        data_quality_gate=data_quality,
        requested_date_range=requested_range,
    )
    precheck = build_b3_redesign_candidate_precheck_v2(
        sources=sources,
        taxonomy=taxonomy,
        generated_at=generated,
        data_quality_gate=data_quality,
        requested_date_range=requested_range,
    )
    payloads = {
        "b3_signal_direction_failure_taxonomy_compute": taxonomy,
        "b3_signal_precheck_compute": precheck,
    }
    paths = _write_named_compute_payloads(
        payloads=payloads,
        output_dir=output_root / spec.campaign_id / "b3_signal_compute",
        renderer=render_b2_b3_v2_payload,
    )
    safety_pass = (
        precheck.get("weight_generation") is False
        and precheck.get("backfill_executed") is False
        and precheck.get("B4_executed") is False
        and precheck.get("B5_executed") is False
        and precheck.get("v3_executed") is False
    )
    smoke_pass = taxonomy["status"] == "B3_SIGNAL_DIRECTION_FAILURE_TAXONOMY_READY" and safety_pass
    status = (
        "B3_SIGNAL_COMPUTE_ADAPTER_SMOKE_PASS"
        if smoke_pass
        else "B3_SIGNAL_COMPUTE_ADAPTER_BLOCKED_WITH_REASON"
    )
    evidence_records = _b3_compute_evidence_records(
        run_id=run_id,
        campaign_id=spec.campaign_id,
        stage=adapter_stage,
        taxonomy=taxonomy,
        precheck=precheck,
        smoke_pass=smoke_pass,
    )
    return StageAdapterRunOutput(
        run_id=run_id,
        campaign_id=spec.campaign_id,
        stage=adapter_stage,
        adapter_id=contract.adapter_id,
        run_mode=contract.default_run_mode,
        adapter_version=contract.adapter_version,
        input_artifacts=input_artifacts,
        output_artifacts=_compute_output_artifacts(paths, payloads),
        evidence_records=[record.model_dump(mode="json") for record in evidence_records],
        compute_performed=True,
        imported_evidence=False,
        parity_source=contract.parity_source,
        failure_mode=None if smoke_pass else status,
        status=status,
        reason_codes=sorted({status, *state.reason_codes, "SIGNAL_DIRECTION_MIXED"}),
        safety_metadata=_adapter_safety_metadata(),
        adapter_outcome=contract.stage_outcomes.get(adapter_stage, "MIXED"),
        data_quality_status=CONTROL_ONLY_DATA_QUALITY_STATUS,
    )


def _b2_compute_evidence_records(
    *,
    run_id: str,
    campaign_id: str,
    stage: str,
    rerun: dict[str, Any],
    no_trigger: dict[str, Any],
    smoke_pass: bool,
) -> list[EvidenceRecord]:
    aggregate = rerun.get("aggregate", {})
    status: Literal["PASS", "FAIL", "MIXED", "BLOCKED", "WARNING", "INFO"] = (
        "PASS" if smoke_pass else "BLOCKED"
    )
    reason_codes = [
        "B2_COMPUTE_ADAPTER_SMOKE_PASS" if smoke_pass else "B2_COMPUTE_REVIEW_REQUIRED",
        "CONTROL_BEHAVIOR_CLEAN" if smoke_pass else "CONTROL_BEHAVIOR_REVIEW_REQUIRED",
    ]
    return [
        EvidenceRecord(
            evidence_id=f"{run_id}-b2-compute-control-rerun-status",
            campaign_id=campaign_id,
            run_id=run_id,
            stage=stage,
            category="TRIGGER_BEHAVIOR",
            metric_name="b2_control_window_rerun_status",
            value=rerun.get("status"),
            direction="positive" if smoke_pass else "negative",
            status=status,
            confidence="high" if smoke_pass else "medium",
            source_artifact_id="b2_control_window_rerun_compute",
            reason_codes=reason_codes,
        ),
        EvidenceRecord(
            evidence_id=f"{run_id}-b2-compute-trigger-count",
            campaign_id=campaign_id,
            run_id=run_id,
            stage=stage,
            category="TRIGGER_BEHAVIOR",
            metric_name="risk_trigger_count",
            value=aggregate.get("trigger_count"),
            direction="positive" if aggregate.get("trigger_count") == 0 else "negative",
            status=status,
            confidence="high" if smoke_pass else "medium",
            source_artifact_id="b2_control_window_rerun_compute",
            reason_codes=reason_codes,
        ),
        EvidenceRecord(
            evidence_id=f"{run_id}-b2-compute-false-risk-off-count",
            campaign_id=campaign_id,
            run_id=run_id,
            stage=stage,
            category="SAFETY",
            metric_name="false_risk_off_count",
            value=aggregate.get("false_risk_off_count"),
            direction="positive" if aggregate.get("false_risk_off_count") == 0 else "negative",
            status=status,
            confidence="high" if smoke_pass else "medium",
            source_artifact_id="b2_no_trigger_correctness_compute",
            reason_codes=reason_codes,
        ),
        EvidenceRecord(
            evidence_id=f"{run_id}-b2-compute-exposure-change-count",
            campaign_id=campaign_id,
            run_id=run_id,
            stage=stage,
            category="PORTFOLIO_EFFECT",
            metric_name="unnecessary_exposure_reduction_count",
            value=aggregate.get("unnecessary_exposure_reduction_count"),
            direction=(
                "positive"
                if aggregate.get("unnecessary_exposure_reduction_count") == 0
                else "negative"
            ),
            status=status,
            confidence="high" if smoke_pass else "medium",
            source_artifact_id="b2_control_window_rerun_compute",
            reason_codes=reason_codes,
        ),
        EvidenceRecord(
            evidence_id=f"{run_id}-b2-compute-no-trigger-status",
            campaign_id=campaign_id,
            run_id=run_id,
            stage=stage,
            category="SAFETY",
            metric_name="no_trigger_correctness_status",
            value=no_trigger.get("status"),
            direction="positive" if smoke_pass else "negative",
            status=status,
            confidence="high" if smoke_pass else "medium",
            source_artifact_id="b2_no_trigger_correctness_compute",
            reason_codes=reason_codes,
        ),
    ]


def _b3_compute_evidence_records(
    *,
    run_id: str,
    campaign_id: str,
    stage: str,
    taxonomy: dict[str, Any],
    precheck: dict[str, Any],
    smoke_pass: bool,
) -> list[EvidenceRecord]:
    status: Literal["PASS", "FAIL", "MIXED", "BLOCKED", "WARNING", "INFO"] = (
        "MIXED" if smoke_pass else "BLOCKED"
    )
    return [
        EvidenceRecord(
            evidence_id=f"{run_id}-b3-signal-direction-taxonomy",
            campaign_id=campaign_id,
            run_id=run_id,
            stage=stage,
            category="SIGNAL_DIRECTION",
            metric_name="b3_signal_direction_taxonomy_status",
            value=taxonomy.get("status"),
            direction="mixed",
            status=status,
            confidence="medium",
            source_artifact_id="b3_signal_direction_failure_taxonomy_compute",
            reason_codes=["SIGNAL_DIRECTION_MIXED", "B3_PRECHECK_MIXED"],
        ),
        EvidenceRecord(
            evidence_id=f"{run_id}-b3-signal-only-boundary",
            campaign_id=campaign_id,
            run_id=run_id,
            stage=stage,
            category="SAFETY",
            metric_name="b3_signal_only_compute_boundary",
            value={
                "weight_generation": precheck.get("weight_generation"),
                "backfill_executed": precheck.get("backfill_executed"),
                "B4_executed": precheck.get("B4_executed"),
                "B5_executed": precheck.get("B5_executed"),
                "v3_executed": precheck.get("v3_executed"),
            },
            direction="positive" if smoke_pass else "negative",
            status="PASS" if smoke_pass else "BLOCKED",
            confidence="high",
            source_artifact_id="b3_signal_precheck_compute",
            reason_codes=["B3_SIGNAL_COMPUTE_ADAPTER_SMOKE_PASS"],
        ),
        EvidenceRecord(
            evidence_id=f"{run_id}-b3-redesign-precheck-status",
            campaign_id=campaign_id,
            run_id=run_id,
            stage=stage,
            category="REDESIGN_HYPOTHESIS",
            metric_name="b3_redesign_candidate_precheck_status",
            value=precheck.get("status"),
            direction="mixed",
            status=status,
            confidence="medium",
            source_artifact_id="b3_signal_precheck_compute",
            reason_codes=["B3_PRECHECK_MIXED"],
        ),
    ]


def _compute_output_artifacts(
    paths: dict[str, tuple[Path, Path]],
    payloads: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    artifacts: list[dict[str, Any]] = []
    for artifact_id, (json_path, markdown_path) in sorted(paths.items()):
        artifacts.append(
            {
                "artifact_id": artifact_id,
                "json_path": str(json_path),
                "markdown_path": str(markdown_path),
                "status": payloads.get(artifact_id, {}).get("status", "UNKNOWN"),
            }
        )
    return artifacts


def _write_named_compute_payloads(
    *,
    payloads: dict[str, dict[str, Any]],
    output_dir: Path,
    renderer: Any,
) -> dict[str, tuple[Path, Path]]:
    output_dir.mkdir(parents=True, exist_ok=True)
    paths: dict[str, tuple[Path, Path]] = {}
    for stem, payload in payloads.items():
        json_path = output_dir / f"{stem}.json"
        markdown_path = output_dir / f"{stem}.md"
        _write_json(json_path, payload)
        markdown_path.write_text(renderer(payload), encoding="utf-8")
        paths[stem] = (json_path, markdown_path)
    return paths


def _control_only_quality_gate() -> dict[str, Any]:
    return {
        "required_command": "not_required_signal_artifact_precheck",
        "status": CONTROL_ONLY_DATA_QUALITY_STATUS,
        "passed": True,
        "error_count": 0,
        "warning_count": 0,
        "info_count": 0,
        "report_path": None,
    }


def _requested_range_from_source(payload: dict[str, Any], *, default_source: str) -> dict[str, Any]:
    value = payload.get("requested_date_range")
    if isinstance(value, dict):
        return dict(value)
    return {
        "start_date": "2022-12-01",
        "end_date": None,
        "source": default_source,
    }


def _adapter_stage_for_workflow_stage(
    *,
    spec: CampaignSpec,
    workflow_stage: str,
    module_registry_path: Path,
) -> str:
    if workflow_stage == "GATE_READY":
        return "GATE"
    if workflow_stage == "OWNER_REVIEW":
        return "OWNER_PACKET"
    if workflow_stage == "INPUT_PRECHECK":
        registry = load_module_registry(module_registry_path)
        module_types = {
            registry[module_id].module_type
            for module_id in spec.module_graph.modules
            if module_id in registry
        }
        if "SIGNAL" in module_types:
            return "SIGNAL_PRECHECK"
    return workflow_stage


def _find_stage_adapter_contract(
    spec: CampaignSpec,
    adapter_stage: str,
    contracts: dict[str, StageAdapterContract],
) -> StageAdapterContract | None:
    module_ids = set(spec.module_graph.modules)
    campaign_type_candidates = {
        spec.campaign_id,
        spec.program_id,
        str(spec.metadata.get("campaign_type", "")),
    }
    mode_priority = {
        "COMPUTE_MODE": 0,
        "AUDITED_ARTIFACT_MODE": 1,
        "DRY_RUN_MODE": 2,
        "VALIDATION_ONLY_MODE": 3,
    }
    ordered_contracts = sorted(
        contracts.values(),
        key=lambda item: (mode_priority.get(item.default_run_mode, 99), item.adapter_id),
    )
    for contract in ordered_contracts:
        if adapter_stage not in contract.supported_stage:
            continue
        if not (module_ids & set(contract.supported_module)):
            continue
        if contract.supported_campaign_type and not (
            campaign_type_candidates & set(contract.supported_campaign_type)
        ):
            continue
        return contract
    return None


def _adapter_input_applies(input_spec: StageAdapterInput, adapter_stage: str) -> bool:
    return not input_spec.stages or adapter_stage in input_spec.stages


def _adapter_source_artifact_issues(
    source_payloads: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    for artifact_id, payload in source_payloads.items():
        if payload.get("holdout_accessed") is True:
            issues.append(
                _issue(
                    "error",
                    "adapter_source_holdout_accessed",
                    f"{artifact_id} accessed holdout and cannot feed Campaign adapter",
                )
            )
        if payload.get("forbidden_outputs_absent") is False:
            issues.append(
                _issue(
                    "error",
                    "adapter_source_forbidden_outputs_present",
                    f"{artifact_id} reports forbidden outputs",
                )
            )
        safety = payload.get("safety_boundary")
        if isinstance(safety, dict):
            if safety.get("official_target_weights") is not False:
                issues.append(
                    _issue(
                        "error",
                        "adapter_source_official_target_weights",
                        f"{artifact_id} violates official target weight boundary",
                    )
                )
            if safety.get("production_effect") not in {None, "none"}:
                issues.append(
                    _issue(
                        "error",
                        "adapter_source_production_effect",
                        f"{artifact_id} has production effect",
                    )
                )
            if safety.get("paper_shadow_activation") is True:
                issues.append(
                    _issue(
                        "error",
                        "adapter_source_paper_shadow_activation",
                        f"{artifact_id} activates paper shadow",
                    )
                )
    return issues


def _adapter_blocked_output(
    *,
    run_id: str,
    campaign_id: str,
    stage: str,
    status: str,
    reason_codes: list[str],
    adapter_id: str = "none",
    adapter_version: str | None = None,
    input_artifacts: list[dict[str, Any]] | None = None,
    run_mode: str = "VALIDATION_ONLY_MODE",
) -> StageAdapterRunOutput:
    return StageAdapterRunOutput(
        run_id=run_id,
        campaign_id=campaign_id,
        stage=stage,
        adapter_id=adapter_id,
        run_mode=run_mode,
        adapter_version=adapter_version,
        input_artifacts=input_artifacts or [],
        output_artifacts=[],
        evidence_records=[],
        compute_performed=False,
        imported_evidence=False,
        parity_source=None,
        failure_mode=status,
        status=status,
        reason_codes=reason_codes,
        safety_metadata=_adapter_safety_metadata(),
        adapter_outcome="BLOCKED",
        data_quality_status=CONTROL_ONLY_DATA_QUALITY_STATUS,
    )


def _extract_value(payload: dict[str, Any], dotted_path: str) -> Any:
    current: Any = payload
    for part in dotted_path.split("."):
        if isinstance(current, dict) and part in current:
            current = current[part]
        else:
            return None
    return current


def _outcome_from_adapter_evidence(
    records: list[dict[str, Any]],
    state: CampaignState,
) -> str:
    if state.current_outcome != "NOT_EVALUATED":
        return state.current_outcome
    if any(record["status"] == "BLOCKED" for record in records):
        return "BLOCKED"
    if any(record["status"] == "FAIL" or record["direction"] == "negative" for record in records):
        return "MIXED"
    if any(record["status"] == "MIXED" or record["direction"] == "mixed" for record in records):
        return "MIXED"
    return "PASS"


def _adapter_data_quality_status(source_payloads: dict[str, dict[str, Any]]) -> str:
    statuses = [
        str(payload.get("data_quality_gate", {}).get("status"))
        for payload in source_payloads.values()
        if isinstance(payload.get("data_quality_gate"), dict)
        and payload.get("data_quality_gate", {}).get("status")
    ]
    if not statuses:
        return CONTROL_ONLY_DATA_QUALITY_STATUS
    if "FAIL" in statuses:
        return "FAIL"
    if any(status == "PASS_WITH_WARNINGS" for status in statuses):
        return "PASS_WITH_WARNINGS"
    if all(status == "PASS" for status in statuses):
        return "PASS"
    return sorted(set(statuses))[0]


def _control_evidence(
    *,
    campaign_id: str,
    run_id: str,
    stage: str,
    category: str,
    metric_name: str,
    value: Any,
    status: Literal["PASS", "FAIL", "MIXED", "BLOCKED", "WARNING", "INFO"],
    source_artifact_id: str,
    reason_codes: list[str],
) -> EvidenceRecord:
    return EvidenceRecord(
        evidence_id=f"{run_id}-{metric_name}",
        campaign_id=campaign_id,
        run_id=run_id,
        stage=stage,
        category=category,
        metric_name=metric_name,
        value=value,
        status=status,
        confidence="high" if status == "PASS" else "medium",
        source_artifact_id=source_artifact_id,
        reason_codes=reason_codes,
        direction="neutral" if status == "PASS" else "negative",
    )


def _adapter_runtime_summary(
    *,
    spec: CampaignSpec,
    target_stage: str,
    adapter_registry_path: Path,
    module_registry_path: Path,
) -> dict[str, Any]:
    try:
        adapter_stage = _adapter_stage_for_workflow_stage(
            spec=spec,
            workflow_stage=target_stage,
            module_registry_path=module_registry_path,
        )
        contract = _find_stage_adapter_contract(
            spec,
            adapter_stage,
            load_stage_adapter_contracts(adapter_registry_path),
        )
    except ResearchCampaignError:
        contract = None
        adapter_stage = target_stage
    if contract is None:
        return {
            "adapter_stage": adapter_stage,
            "adapter_id": None,
            "run_mode": None,
            "compute_performed": False,
            "imported_evidence": False,
            "failure_mode": "STAGE_ADAPTER_NOT_CONFIGURED",
        }
    return {
        "adapter_stage": adapter_stage,
        "adapter_id": contract.adapter_id,
        "run_mode": contract.default_run_mode,
        "compute_performed": contract.default_run_mode == "COMPUTE_MODE",
        "imported_evidence": contract.default_run_mode == "AUDITED_ARTIFACT_MODE",
        "failure_mode": None,
    }


def _stage_budget_block_reason(
    *,
    spec: CampaignSpec,
    state: CampaignState,
    target_stage: str,
    requested_next: bool,
) -> str | None:
    budget = evaluate_evidence_budget(spec, state)
    if (
        requested_next
        and state.current_outcome == "NEEDS_MORE_EVIDENCE"
        and budget["budget_status"] == "EXHAUSTED"
    ):
        return "EVIDENCE_BUDGET_EXHAUSTED_OWNER_OVERRIDE_REQUIRED"
    remaining = budget["stop_rule_proximity"]
    if target_stage == "MINI_DIAGNOSTIC" and remaining["mini_rounds_remaining"] <= 0:
        return "MINI_DIAGNOSTIC_BUDGET_EXHAUSTED"
    if target_stage == "TARGETED_EVIDENCE" and remaining["targeted_rounds_remaining"] <= 0:
        return "TARGETED_EVIDENCE_BUDGET_EXHAUSTED"
    return None


def _record_blocked_campaign_run(
    *,
    directory: Path,
    spec: CampaignSpec,
    state: CampaignState,
    from_stage: str,
    target_stage: str,
    reason_codes: list[str],
    result_payload: dict[str, Any],
) -> dict[str, Any]:
    run_id = f"{spec.campaign_id}-{target_stage.lower()}-{_compact_time()}"
    evidence = _control_evidence(
        campaign_id=spec.campaign_id,
        run_id=run_id,
        stage=target_stage,
        category="SAFETY",
        metric_name="campaign_run_next_stage",
        value="blocked",
        status="BLOCKED",
        source_artifact_id="campaign_stage_runner",
        reason_codes=reason_codes,
    )
    state.current_stage = target_stage
    state.current_outcome = "BLOCKED"
    state.reason_codes = reason_codes
    state.updated_at = _now_iso()
    state.stage_history.append(
        {
            "run_id": run_id,
            "stage": target_stage,
            "outcome": "BLOCKED",
            "reason_codes": reason_codes,
            "created_at": state.updated_at,
        }
    )
    write_campaign_state(state, directory)
    append_evidence_records([evidence], directory)
    _append_transition(
        directory,
        {
            "transition_id": run_id,
            "campaign_id": spec.campaign_id,
            "from_stage": from_stage,
            "to_stage": target_stage,
            "outcome": "BLOCKED",
            "reason_codes": reason_codes,
            "created_at": state.updated_at,
            "source": "campaign_stage_runner",
        },
    )
    _append_jsonl(
        directory / "runs.jsonl",
        {
            "run_id": run_id,
            "campaign_id": spec.campaign_id,
            "stage": target_stage,
            "outcome": "BLOCKED",
            "reason_codes": reason_codes,
            "created_at": state.updated_at,
            "result": result_payload,
            "adapter_id": None,
            "adapter_status": result_payload.get("status"),
            "production_effect": "none",
        },
    )
    return {
        "run_id": run_id,
        "campaign_id": spec.campaign_id,
        "stage": target_stage,
        "outcome": "BLOCKED",
        "reason_codes": reason_codes,
        "generated_evidence_count": 1,
        "result": result_payload,
        "adapter_id": None,
        "adapter_status": result_payload.get("status"),
        "safety_boundary": state.safety_boundary,
    }


def _recommended_next_stage(current_stage: str, allowed: list[str], blocked: list[str]) -> str:
    if "PREPARE_OWNER_PACKET" in allowed:
        return "OWNER_REVIEW"
    if "COMPLETE_FINAL_REPEATABILITY_ROUND" in allowed:
        return "TARGETED_EVIDENCE"
    if "RETURN_TO_DESIGN" in allowed or "NARROW_ROLE" in allowed:
        return "OWNER_REVIEW"
    if "EMIT_OPEN_ENDED_NEEDS_MORE_EVIDENCE" in blocked:
        return "OWNER_REVIEW"
    return _next_stage(current_stage)


def _outcome_from_imported_stage_evidence(
    records: list[EvidenceRecord],
    state: CampaignState,
) -> str:
    if state.current_stage == records[0].stage and state.current_outcome != "NOT_EVALUATED":
        return state.current_outcome
    if any(record.status == "BLOCKED" for record in records):
        return "BLOCKED"
    if any(record.status == "FAIL" or record.direction == "negative" for record in records):
        return "MIXED"
    if any(record.status == "MIXED" or record.direction == "mixed" for record in records):
        return "MIXED"
    return "PASS"


def _next_stage(current_stage: str) -> str:
    if current_stage not in WORKFLOW_STAGES:
        raise ResearchCampaignError(f"Unknown current stage: {current_stage}")
    index = WORKFLOW_STAGES.index(current_stage)
    if index >= len(WORKFLOW_STAGES) - 1:
        return current_stage
    return WORKFLOW_STAGES[index + 1]


def _transition(allowed: bool, current_stage: str, next_stage: str, reason: str) -> dict[str, Any]:
    return {
        "allowed": allowed,
        "from_stage": current_stage,
        "to_stage": next_stage,
        "reason": reason,
    }


def _module_alias(capability: ModuleCapability, module_id: str) -> str:
    return capability.interaction_alias or module_id


def _evidence_lines(records: list[dict[str, Any]]) -> list[str]:
    if not records:
        return ["- none"]
    return [
        f"- {record['evidence_id']}: {record['category']} / {record['metric_name']} "
        f"= {record.get('value')} ({record['status']})"
        for record in records
    ]


def _write_stage_adapter_run_artifacts(
    payload: dict[str, Any],
    *,
    output_root: Path,
) -> dict[str, Any]:
    output_dir = output_root / payload["campaign_id"]
    output_dir.mkdir(parents=True, exist_ok=True)
    date_token = _now_iso()[:10]
    if payload["campaign_id"].startswith("b2-"):
        basename = f"b2_campaign_adapter_run_{date_token}"
    elif payload["campaign_id"].startswith("b3-"):
        basename = "b3_campaign_signal_precheck_run"
    else:
        basename = f"campaign_stage_adapter_run_{payload['stage'].lower()}_{date_token}"
    json_path = output_dir / f"{basename}.json"
    md_path = output_dir / f"{basename}.md"
    enriched = dict(payload)
    existing_outputs = list(enriched.get("output_artifacts") or [])
    enriched["output_artifacts"] = [
        *existing_outputs,
        {"artifact_id": f"{basename}.json", "path": str(json_path)},
        {"artifact_id": f"{basename}.md", "path": str(md_path)},
    ]
    _write_json(json_path, enriched)
    md_path.write_text(render_stage_adapter_run_markdown(enriched), encoding="utf-8")
    enriched["json_path"] = str(json_path)
    enriched["markdown_path"] = str(md_path)
    return enriched


def render_stage_adapter_run_markdown(payload: dict[str, Any]) -> str:
    lines = [
        f"# Campaign Stage Adapter Run: {payload['campaign_id']}",
        "",
        "## Reader Brief",
        "",
        f"- Adapter Status: {payload['status']}",
        f"- Adapter: {payload['adapter_id']}",
        f"- Run Mode: {payload['run_mode']}",
        f"- Compute Performed: {payload['compute_performed']}",
        f"- Imported Evidence: {payload['imported_evidence']}",
        f"- Parity Source: {payload.get('parity_source') or 'none'}",
        f"- Failure Mode: {payload.get('failure_mode') or 'none'}",
        f"- Stage: {payload['stage']}",
        f"- Outcome: {payload['adapter_outcome']}",
        f"- Data Quality: {payload['data_quality_status']}",
        "- Safety Boundary: "
        f"research_only={payload['safety_metadata']['research_only']}; "
        f"manual_review_only={payload['safety_metadata']['manual_review_only']}; "
        f"official_target_weights={payload['safety_metadata']['official_target_weights']}; "
        f"paper_shadow_activation={payload['safety_metadata']['paper_shadow_activation']}; "
        f"broker_effect={payload['safety_metadata']['broker_effect']}; "
        f"order_effect={payload['safety_metadata']['order_effect']}; "
        f"production_effect={payload['safety_metadata']['production_effect']}; "
        f"holdout_touched={payload['safety_metadata']['holdout_touched']}",
        "",
        "## Reason Codes",
        "",
    ]
    lines.extend(f"- {code}" for code in payload["reason_codes"] or ["none"])
    lines.extend(["", "## Input Artifacts", ""])
    for artifact in payload["input_artifacts"] or []:
        lines.append(
            f"- {artifact.get('artifact_id')}: exists={artifact.get('exists')} "
            f"checksum={artifact.get('checksum')}"
        )
    if not payload["input_artifacts"]:
        lines.append("- none")
    lines.extend(["", "## Evidence Records", ""])
    lines.extend(_evidence_lines(payload["evidence_records"]))
    lines.extend(["", "## Output Artifacts", ""])
    for artifact in payload["output_artifacts"] or []:
        lines.append(f"- {artifact.get('artifact_id')}: {artifact.get('path')}")
    return "\n".join(lines) + "\n"


def _append_transition(directory: Path, payload: dict[str, Any]) -> None:
    _append_jsonl(directory / "transitions.jsonl", payload)


def _append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _read_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ResearchCampaignError(f"Cannot read JSON: {path}") from exc
    except json.JSONDecodeError as exc:
        raise ResearchCampaignError(f"Invalid JSON: {path}") from exc


def _read_yaml(path: Path) -> dict[str, Any]:
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ResearchCampaignError(f"Cannot read YAML: {path}") from exc
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ResearchCampaignError(f"YAML root must be a mapping: {path}")
    return raw


def _resolve_project_path(path: str) -> Path:
    candidate = Path(path)
    if candidate.is_absolute():
        return candidate
    return PROJECT_ROOT / candidate


def _issue(severity: str, issue_id: str, message: str) -> dict[str, Any]:
    return {"severity": severity, "issue_id": issue_id, "message": message}


def _path_record(path: Path) -> dict[str, Any]:
    return {
        "path": str(path),
        "exists": path.exists(),
        "checksum": _checksum(path) if path.exists() and path.is_file() else None,
    }


def _checksum(path: Path) -> str | None:
    if not path.exists() or not path.is_file():
        return None
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _git_commit() -> str | None:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=PROJECT_ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    return result.stdout.strip()


def _git_dirty() -> bool | None:
    try:
        result = subprocess.run(
            ["git", "status", "--short"],
            cwd=PROJECT_ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    return bool(result.stdout.strip())


def _now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _compact_time() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
